//! Scoped timing wheel wrapper that hides generativity branding from callers.

use core::num::NonZeroUsize;

use crate::runtime::timing::time::{Duration, MonoInstant, NonZeroDuration, Now, TimeUnit};
use crate::runtime::timing::wheel::{TimerHandle, Wheel, WheelError};

/// Runs `f` with a scoped wheel; branded handles cannot escape the closure.
pub fn with_wheel<T, U, const SLOTS: usize, R>(
    tick_duration: NonZeroDuration<U>,
    capacity: NonZeroUsize,
    f: impl for<'id> FnOnce(&mut WheelScope<'id, T, U, SLOTS>) -> R,
) -> R
where
    U: TimeUnit + Now,
{
    generativity::make_guard!(guard);
    let mut scope = WheelScope {
        inner: Wheel::new(guard, tick_duration, capacity),
    };
    f(&mut scope)
}

/// Wrapper around a branded wheel with a user-friendly API.
pub struct WheelScope<'id, T, U: TimeUnit + Now, const SLOTS: usize> {
    inner: Wheel<'id, T, U, SLOTS>,
}

impl<'id, T, U, const SLOTS: usize> WheelScope<'id, T, U, SLOTS>
where
    U: TimeUnit + Now,
{
    /// Schedules `payload` to fire after `delay` from the current time.
    pub fn schedule_after(
        &mut self,
        delay: Duration<U>,
        payload: T,
    ) -> Result<TimerHandle<'id, T, U>, WheelError> {
        let now = self.inner.now();
        self.inner.schedule_after(&now, delay, payload)
    }

    /// Schedules `payload` to fire after `delay` from an explicit instant.
    pub fn schedule_after_at(
        &mut self,
        now: MonoInstant<U>,
        delay: Duration<U>,
        payload: T,
    ) -> Result<TimerHandle<'id, T, U>, WheelError> {
        let now = self.inner.tick_from_instant(now);
        self.inner.schedule_after(&now, delay, payload)
    }

    /// Cancels a timer by handle.
    pub fn cancel(&mut self, handle: &TimerHandle<'id, T, U>) -> bool {
        self.inner.cancel(handle)
    }

    /// Advances the wheel to `now` and fires any due timers.
    pub fn tick_at(&mut self, now: MonoInstant<U>, on_fire: impl FnMut(TimerHandle<'id, T, U>, T)) {
        let now = self.inner.tick_from_instant(now);
        self.inner.tick_at(&now, on_fire);
    }

    /// Advances the wheel to the current time and fires any due timers.
    pub fn tick_now(&mut self, on_fire: impl FnMut(TimerHandle<'id, T, U>, T)) {
        let now = self.inner.now();
        self.inner.tick_at(&now, on_fire);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::timing::{
        Duration, MonoInstant, NonZeroDuration, Now, TimeUnit, WheelError,
    };
    use core::cell::Cell;
    use core::num::{NonZeroU64, NonZeroUsize};

    /// Deterministic unit for tests.
    #[derive(Debug, PartialEq, Eq)]
    enum TestUnit {}

    impl TimeUnit for TestUnit {
        const NAME: &'static str = "test";
    }

    thread_local! {
        static NOW: Cell<u64> = const { Cell::new(0) };
    }

    impl Now for TestUnit {
        fn now() -> MonoInstant<Self> {
            MonoInstant::new(NOW.with(Cell::get))
        }
    }

    fn set_now(v: u64) {
        NOW.with(|t| t.set(v));
    }

    fn tick_duration() -> NonZeroDuration<TestUnit> {
        NonZeroDuration::new(NonZeroU64::new(1).unwrap())
    }

    fn with_test_wheel<T, const SLOTS: usize, R>(
        capacity: usize,
        f: impl for<'id> FnOnce(&mut WheelScope<'id, T, TestUnit, SLOTS>) -> R,
    ) -> R {
        with_wheel::<T, TestUnit, SLOTS, R>(
            tick_duration(),
            NonZeroUsize::new(capacity).unwrap(),
            f,
        )
    }

    #[test]
    fn schedule_delay_zero_fires_on_next_tick() {
        set_now(0);
        with_test_wheel::<u32, 4, _>(4, |wheel| {
            let handle = wheel.schedule_after(Duration::new(0), 7).unwrap();
            let mut fired = false;

            wheel.tick_now(|_, _| fired = true);
            assert!(!fired, "delay=0 should not fire without a tick");

            set_now(1);
            wheel.tick_now(|h, payload| {
                assert!(!fired, "timer fired more than once");
                fired = true;
                assert_eq!(h, handle);
                assert_eq!(payload, 7);
            });
            assert!(fired, "timer should fire on next tick");
            assert!(!wheel.cancel(&handle), "handle is stale after firing");
        });
    }

    #[test]
    fn cancel_prevents_fire() {
        set_now(0);
        with_test_wheel::<u32, 4, _>(2, |wheel| {
            let handle = wheel.schedule_after(Duration::new(1), 42).unwrap();
            assert!(wheel.cancel(&handle));
            assert!(!wheel.cancel(&handle), "cancel is not idempotent");

            set_now(2);
            let mut fired = Vec::new();
            wheel.tick_now(|_, payload| fired.push(payload));
            assert!(fired.is_empty());
        });
    }

    #[test]
    fn capacity_exhaustion_returns_error() {
        set_now(0);
        with_test_wheel::<u32, 4, _>(1, |wheel| {
            let _ = wheel.schedule_after(Duration::new(1), 1).unwrap();
            let err = wheel.schedule_after(Duration::new(1), 2).unwrap_err();
            assert!(matches!(err, WheelError::Capacity));
        });
    }

    #[test]
    fn delay_too_long_returns_error() {
        set_now(0);
        with_test_wheel::<u32, 4, _>(2, |wheel| {
            let max = (4u64).saturating_sub(1);
            let err = wheel.schedule_after(Duration::new(max + 1), 1).unwrap_err();
            assert!(
                matches!(err, WheelError::DelayTooLong { delay, max: got_max } if delay == max + 1 && got_max == max),
                "should reject delays beyond one rotation"
            );
        });
    }

    #[test]
    fn schedule_after_at_and_tick_at_use_explicit_instants() {
        set_now(0);
        with_test_wheel::<u32, 8, _>(4, |wheel| {
            let now = MonoInstant::new(3);
            let handle = wheel.schedule_after_at(now, Duration::new(2), 5).unwrap();
            let mut fired = Vec::new();

            wheel.tick_at(MonoInstant::new(4), |h, payload| fired.push((h, payload)));
            assert!(fired.is_empty());

            wheel.tick_at(MonoInstant::new(5), |h, payload| fired.push((h, payload)));
            assert_eq!(fired, vec![(handle, 5)]);
        });
    }

    #[test]
    fn schedule_after_at_uses_cursor_when_now_behind() {
        set_now(0);
        with_test_wheel::<u32, 8, _>(4, |wheel| {
            set_now(5);
            wheel.tick_now(|_, _| {});

            let handle = wheel
                .schedule_after_at(MonoInstant::new(3), Duration::new(1), 9)
                .unwrap();
            let mut fired = Vec::new();

            set_now(5);
            wheel.tick_now(|h, payload| fired.push((h, payload)));
            assert!(fired.is_empty());

            set_now(6);
            wheel.tick_now(|h, payload| fired.push((h, payload)));
            assert_eq!(fired, vec![(handle, 9)]);
        });
    }
}

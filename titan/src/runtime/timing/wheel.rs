//! Hashed timing wheel with O(1) schedule/cancel and bounded per-tick work.
//!
//! Dimensional model:
//! - Physical time: `MonoInstant<U>` is a point; `Duration<U>` is a span.
//! - Tick space: `TickInstant` is a point on a discrete lattice derived from
//!   `floor((instant - start) / tick_duration)`, and `TickSpan` is a span in
//!   that lattice.

use core::marker::PhantomData;
use core::num::NonZeroUsize;

use generativity::{Guard, Id};
use thiserror::Error;

use crate::runtime::timing::slab::{Slab, SlabIndex};
use crate::runtime::timing::tick::{TickInstant, TickSpan};
use crate::runtime::timing::time::{Duration, MonoInstant, NonZeroDuration, Now, TimeUnit};

/// Handle returned to callers; includes index and generation to detect stale use.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TimerHandle<'id, T, U: TimeUnit> {
    /// Index into the slab.
    idx: SlabIndex<T>,
    /// Generation for ABA protection.
    generation: u32,
    _brand: Id<'id>,
    _unit: PhantomData<U>,
}

/// Errors returned by wheel operations.
#[derive(Debug, Error)]
pub enum WheelError {
    /// The requested delay exceeds the wheel's addressable range.
    #[error("delay {delay} ticks exceeds maximum {max}")]
    DelayTooLong {
        /// Requested delay in ticks.
        delay: u64,
        /// Maximum addressable delay.
        max: u64,
    },
    /// No free slots remain in the slab.
    #[error("timer capacity exhausted")]
    Capacity,
}

/// Hashed timing wheel.
pub struct Wheel<'id, T, U: TimeUnit, const SLOTS: usize> {
    slab: Slab<T>,                        // slot pool
    slots: [Option<SlabIndex<T>>; SLOTS], // head of timer list per slot
    start: MonoInstant<U>,
    cursor: TickInstant, // current tick instant
    tick_duration: NonZeroDuration<U>,
    brand: Id<'id>,
}

impl<'id, T, U, const SLOTS: usize> Wheel<'id, T, U, SLOTS>
where
    U: TimeUnit,
{
    const _ASSERT_SLOTS: () = assert!(SLOTS >= 2);

    /// Converts a monotonic instant into a wheel-local tick.
    #[inline]
    fn tick_from_instant(&self, instant: MonoInstant<U>) -> TickInstant {
        let elapsed = instant - self.start;
        let now_tick = elapsed / Duration::from(self.tick_duration);
        TickInstant::new(now_tick)
    }

    /// Maximum delay (in ticks) that can be scheduled.
    #[inline]
    #[must_use]
    pub const fn max_tick_span(&self) -> TickSpan {
        // Single-level wheel covers one full rotation.
        TickSpan::new((SLOTS as u64).saturating_sub(1))
    }

    /// Maps a tick to its slot index.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    const fn slot_index(tick: TickInstant) -> usize {
        // Modulo bounds the value to < SLOTS, which fits in usize.
        (tick.get() % SLOTS as u64) as usize
    }

    /// Schedules a timer `delay` from `now` with payload `payload`.
    ///
    /// # Errors
    ///
    /// Returns an error if the delay exceeds the wheel's range or the slab is at capacity.
    pub fn schedule_after(
        &mut self,
        now: MonoInstant<U>,
        delay: Duration<U>,
        payload: T,
    ) -> Result<TimerHandle<'id, T, U>, WheelError> {
        let now_tick = self.tick_from_instant(now);
        let ticks_needed = delay.div_ceil(Duration::from(self.tick_duration));
        let ticks_ahead = TickSpan::new(ticks_needed.max(1));
        let max_delay = self.max_tick_span();
        if ticks_ahead > max_delay {
            return Err(WheelError::DelayTooLong {
                delay: ticks_ahead.get(),
                max: max_delay.get(),
            });
        }
        let base_tick = core::cmp::max(self.cursor, now_tick);
        // Adding ticks is assumed not to overflow; see TickInstant::add_span.
        let deadline = base_tick + ticks_ahead;

        let slot = Self::slot_index(deadline);
        let head = self.slots[slot];

        let (idx, generation) = {
            let (idx, node) = self
                .slab
                .alloc(payload, deadline)
                .ok_or(WheelError::Capacity)?;
            node.next = head;
            node.prev = None;
            let generation = node.generation;
            (idx, generation)
        };

        // Insert at head of slot list
        if let Some(head_idx) = head
            && let Some(head_node) = self.slab.get_mut(head_idx)
        {
            head_node.prev = Some(idx);
        }
        self.slots[slot] = Some(idx);
        Ok(TimerHandle {
            idx,
            generation,
            _brand: self.brand,
            _unit: PhantomData,
        })
    }

    /// Cancels a timer by handle. Returns `true` if the timer was found and cancelled.
    pub fn cancel(&mut self, handle: &TimerHandle<'id, T, U>) -> bool {
        let Some(node) = self.slab.get(handle.idx) else {
            return false;
        };
        if node.generation != handle.generation {
            return false;
        }

        let idx = handle.idx;

        // Unlink from list
        if let Some(node) = self.slab.get_mut(idx) {
            let next = node.next;
            let prev = node.prev;
            let deadline = node.deadline;
            if let Some(p) = prev {
                if let Some(pnode) = self.slab.get_mut(p) {
                    pnode.next = next;
                }
            } else {
                // head of slot list; find slot
                let slot = Self::slot_index(deadline);
                self.slots[slot] = next;
            }
            if let Some(n) = next
                && let Some(nnode) = self.slab.get_mut(n)
            {
                nnode.prev = prev;
            }
        }

        self.slab.free(idx).is_some()
    }

    /// Advances the wheel to `now` and invokes `on_fire` for each due timer.
    ///
    /// `on_fire` receives the handle (for callers that stash it) and the payload by value.
    pub fn tick_at(
        &mut self,
        now: MonoInstant<U>,
        mut on_fire: impl FnMut(TimerHandle<'id, T, U>, T),
    ) {
        let now_tick = self.tick_from_instant(now);
        if now_tick <= self.cursor {
            return;
        }
        let mut tick = TickInstant::new(self.cursor.get() + 1);
        while tick <= now_tick {
            let slot = Self::slot_index(tick);
            let mut head = self.slots[slot].take();
            while let Some(idx) = head {
                // Save next before we free.
                let next = self.slab.get(idx).and_then(|n| n.next);
                if let Some(node) = self.slab.get_mut(idx)
                    && let Some(payload) = node.payload.take()
                {
                    on_fire(
                        TimerHandle {
                            idx,
                            generation: node.generation,
                            _brand: self.brand,
                            _unit: PhantomData,
                        },
                        payload,
                    );
                }
                let _ = self.slab.free(idx);
                head = next;
            }
            tick = TickInstant::new(tick.get() + 1);
        }
        self.cursor = now_tick;
    }
}

impl<'id, T, U, const SLOTS: usize> Wheel<'id, T, U, SLOTS>
where
    U: TimeUnit + Now,
{
    /// Creates a new wheel starting at the current monotonic timestamp.
    /// The guard brands this wheel instance and must be unique per wheel.
    #[must_use]
    pub fn new(
        guard: Guard<'id>,
        tick_duration: NonZeroDuration<U>,
        capacity: NonZeroUsize,
    ) -> Self {
        let () = Self::_ASSERT_SLOTS;
        let brand: Id<'id> = guard.into();
        Self {
            slots: [None; SLOTS],
            slab: Slab::with_capacity(capacity),
            tick_duration,
            start: U::now(),
            cursor: TickInstant::new(0),
            brand,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use generativity::Guard;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::cell::Cell;
    use std::collections::{HashMap, HashSet};
    use std::num::{NonZeroU64, NonZeroUsize};

    /// Deterministic unit for testing.
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
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

    fn now() -> MonoInstant<TestUnit> {
        TestUnit::now()
    }

    type TestWheel<'id> = Wheel<'id, u32, TestUnit, 8>;
    type StressWheel<'id> = Wheel<'id, u32, TestUnit, 16>;

    fn wheel_u32<'id>(guard: Guard<'id>, capacity: usize) -> TestWheel<'id> {
        set_now(0);
        Wheel::new(
            guard,
            NonZeroDuration::<TestUnit>::new(NonZeroU64::new(1).unwrap()),
            NonZeroUsize::new(capacity).unwrap(),
        )
    }

    #[test]
    fn fires_due_timers_no_alloc() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 4);
        let h1 = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 10)
            .unwrap();
        let h2 = w
            .schedule_after(now(), Duration::<TestUnit>::new(1), 20)
            .unwrap();
        let mut fired = Vec::new();
        set_now(1);
        w.tick_at(now(), |h, v| fired.push((h, v)));
        fired.sort_by_key(|(_, v)| *v);
        assert_eq!(fired, vec![(h1, 10), (h2, 20)]);

        fired.clear();
        set_now(3);
        w.tick_at(now(), |h, v| fired.push((h, v)));
        assert!(fired.is_empty());
    }

    #[test]
    fn cancel_prevents_fire() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 2);
        let h = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 42)
            .unwrap();
        assert!(w.cancel(&h));
        let mut fired = Vec::new();
        set_now(1);
        w.tick_at(now(), |_, v| fired.push(v));
        assert!(fired.is_empty());
    }

    #[test]
    fn stale_handle_rejected() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 1);
        let h1 = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 1)
            .unwrap();
        set_now(1);
        w.tick_at(now(), |_, _| {});
        let h2 = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 2)
            .unwrap();
        assert_ne!(h1.generation, h2.generation);
        assert!(!w.cancel(&h1));
        assert!(w.cancel(&h2));
    }

    #[test]
    fn capacity_exhaustion() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 1);
        let _ = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 1)
            .unwrap();
        assert!(
            matches!(
                w.schedule_after(now(), Duration::<TestUnit>::new(0), 2),
                Err(WheelError::Capacity)
            ),
            "should fail when slab is full"
        );
    }

    #[test]
    fn delay_too_long_rejected() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 1);
        let max = w.max_tick_span();
        let too_long = Duration::<TestUnit>::new(max.get() + 1);
        let err = w.schedule_after(now(), too_long, 1);
        assert!(
            matches!(err, Err(WheelError::DelayTooLong { delay, max: got_max }) if delay == max.get() + 1 && got_max == max.get()),
            "should reject delays beyond one rotation"
        );
    }

    #[test]
    fn schedule_uses_now_when_cursor_behind() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 2);
        set_now(2);
        let h = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 11)
            .unwrap();
        let mut fired = Vec::new();
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert!(fired.is_empty(), "should not fire before deadline");
        set_now(3);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h, 11)]);
    }

    #[test]
    fn stale_now_uses_cursor() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 2);
        let stale_now = now();
        set_now(2);
        w.tick_at(now(), |_, _| {});
        let h = w
            .schedule_after(stale_now, Duration::<TestUnit>::new(0), 5)
            .unwrap();
        let mut fired = Vec::new();
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert!(
            fired.is_empty(),
            "stale now should not schedule in the past"
        );
        set_now(3);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h, 5)]);
    }

    #[test]
    fn cancel_non_head_removes_timer() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 2);
        let h1 = w
            .schedule_after(now(), Duration::<TestUnit>::new(1), 1)
            .unwrap();
        let h2 = w
            .schedule_after(now(), Duration::<TestUnit>::new(1), 2)
            .unwrap();
        assert!(w.cancel(&h1));
        let mut fired = Vec::new();
        set_now(1);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h2, 2)]);
    }

    #[test]
    fn pending_kept_until_due() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 2);
        let h = w
            .schedule_after(now(), Duration::<TestUnit>::new(2), 99)
            .unwrap();
        let mut fired = Vec::new();
        set_now(1);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert!(fired.is_empty(), "not due yet");
        set_now(2);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h, 99)]);
    }

    #[test]
    fn jump_ahead_fires_intermediate() {
        generativity::make_guard!(guard);
        let mut w = wheel_u32(guard, 3);
        let h1 = w
            .schedule_after(now(), Duration::<TestUnit>::new(0), 1)
            .unwrap();
        let h2 = w
            .schedule_after(now(), Duration::<TestUnit>::new(2), 3)
            .unwrap();
        let mut seen = Vec::new();
        set_now(3);
        w.tick_at(now(), |h, v| seen.push((h, v)));
        seen.sort_by_key(|(_, v)| *v);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (h1, 1));
        assert_eq!(seen[1], (h2, 3));
    }

    #[test]
    fn rounds_delay_to_ticks() {
        const TICK_DURATION: u64 = 5;
        generativity::make_guard!(guard);
        set_now(0);
        let mut w: TestWheel<'_> = Wheel::new(
            guard,
            NonZeroDuration::<TestUnit>::new(NonZeroU64::new(TICK_DURATION).unwrap()),
            NonZeroUsize::new(8).unwrap(),
        );
        let h1 = w
            .schedule_after(now(), Duration::<TestUnit>::new(1), 1)
            .unwrap();
        let h2 = w
            .schedule_after(now(), Duration::<TestUnit>::new(5), 2)
            .unwrap();
        let h3 = w
            .schedule_after(now(), Duration::<TestUnit>::new(6), 3)
            .unwrap();

        let mut fired = Vec::new();
        set_now(4);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert!(fired.is_empty(), "no timers should fire before tick 1");

        fired.clear();
        set_now(5);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        fired.sort_by_key(|(_, v)| *v);
        assert_eq!(fired, vec![(h1, 1), (h2, 2)]);

        fired.clear();
        set_now(10);
        w.tick_at(now(), |handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h3, 3)]);
    }

    #[test]
    fn randomized_schedule_cancel_tick_matches_model() {
        const TICK_DURATION: u64 = 5;
        const STEPS: usize = 2000;
        generativity::make_guard!(guard);
        set_now(0);
        let mut w: StressWheel<'_> = Wheel::new(
            guard,
            NonZeroDuration::<TestUnit>::new(NonZeroU64::new(TICK_DURATION).unwrap()),
            NonZeroUsize::new(128).unwrap(),
        );
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let mut model_cursor = TickInstant::new(0);
        let mut model: HashMap<TimerHandle<'_, u32, TestUnit>, TickInstant> = HashMap::new();
        let mut handles: Vec<TimerHandle<'_, u32, TestUnit>> = Vec::new();
        let mut now_units: u64 = 0;
        let max_delay_units = (StressWheel::max_tick_span(&w).get()) * TICK_DURATION;

        // Helper to convert time units to tick instant (mirrors wheel logic).
        let to_tick = |units: u64| TickInstant::new(units / TICK_DURATION);

        for step in 0..STEPS {
            let action = rng.random_range(0..100);
            if action < 50 {
                let delay = rng.random_range(0..=max_delay_units);
                let now_tick = to_tick(now_units);
                match w.schedule_after(now(), Duration::<TestUnit>::new(delay), step as u32) {
                    Ok(handle) => {
                        let ticks_needed = (delay + TICK_DURATION - 1) / TICK_DURATION;
                        let ticks_ahead = ticks_needed.max(1);
                        let base_tick = core::cmp::max(model_cursor, now_tick);
                        let deadline = base_tick + TickSpan::new(ticks_ahead);
                        model.insert(handle.clone(), deadline);
                        handles.push(handle);
                    }
                    Err(WheelError::Capacity) => {}
                    Err(err) => panic!("unexpected schedule error: {err:?}"),
                }
            } else if action < 70 {
                if !handles.is_empty() {
                    let idx = rng.random_range(0..handles.len());
                    let handle = handles.swap_remove(idx);
                    let ok = w.cancel(&handle);
                    let expected = model.remove(&handle).is_some();
                    assert_eq!(ok, expected);
                }
            } else {
                let advance = rng.random_range(0..=20);
                now_units = now_units.saturating_add(advance);
                set_now(now_units);
                let now_tick = to_tick(now_units);

                let mut fired = Vec::new();
                w.tick_at(now(), |handle, _| fired.push(handle));

                if now_tick <= model_cursor {
                    assert!(
                        fired.is_empty(),
                        "no timers should fire without advancement"
                    );
                    continue;
                }

                let mut expected = Vec::new();
                for (handle, deadline) in model.iter() {
                    if *deadline > model_cursor && *deadline <= now_tick {
                        expected.push((*handle).clone());
                    }
                }
                for handle in &expected {
                    model.remove(handle);
                }

                let fired_set: HashSet<_> = fired.into_iter().collect();
                let expected_set: HashSet<_> = expected.into_iter().collect();
                assert_eq!(fired_set, expected_set);

                model_cursor = now_tick;
            }
        }
    }
}

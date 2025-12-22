//! Hashed timing wheel with O(1) schedule/cancel and bounded per-tick work.

use core::num::NonZeroUsize;

use thiserror::Error;

use crate::runtime::timing::slab::{Slab, SlabIndex};
use crate::runtime::timing::time::{Duration, MonoInstant, NonZeroDuration, Now, TimeUnit};

/// Handle returned to callers; includes index and generation to detect stale use.
// Manual Copy/Clone: derive would require T: Copy, but we only hold SlabIndex<T> (PhantomData).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimerHandle<T> {
    /// Index into the slab.
    pub idx: SlabIndex<T>,
    /// Generation for ABA protection.
    pub generation: u32,
}

impl<T> Copy for TimerHandle<T> {}

impl<T> Clone for TimerHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
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
    /// Arithmetic overflow computing deadline.
    #[error("deadline overflow")]
    Overflow,
    /// No free slots remain in the slab.
    #[error("timer capacity exhausted")]
    Capacity,
}

/// Hashed timing wheel.
pub struct Wheel<T, U: TimeUnit> {
    slab: Slab<T>,                    // slot pool
    slots: Vec<Option<SlabIndex<T>>>, // head of timer list per slot
    start: MonoInstant<U>,
    cursor: u64, // current tick
    tick: NonZeroDuration<U>,
}

impl<T, U> Wheel<T, U>
where
    U: TimeUnit + Now,
{
    /// Creates a new wheel starting at the current monotonic timestamp.
    #[must_use]
    pub fn new(slots: NonZeroUsize, tick: NonZeroDuration<U>, capacity: NonZeroUsize) -> Self {
        Self {
            slots: vec![None; slots.get()],
            slab: Slab::with_capacity(capacity),
            tick,
            start: U::now(),
            cursor: 0,
        }
    }

    /// Maximum delay (in ticks) that can be scheduled
    #[inline]
    #[must_use]
    pub fn max_delay_ticks(&self) -> u64 {
        // Single-level wheel covers one full rotation.
        u64::try_from(self.slots.len())
            .unwrap_or(u64::MAX)
            .saturating_sub(1)
    }

    /// Maps a tick to its slot index.
    #[inline]
    fn slot_index(&self, tick: u64) -> usize {
        usize::try_from(tick % u64::try_from(self.slots.len()).unwrap_or(u64::MAX))
            .expect("modulo result should not exceed usize")
    }

    /// Schedules a timer `delay` from now with payload `payload`.
    ///
    /// # Errors
    ///
    /// Returns an error if the delay exceeds the wheel's range, arithmetic overflows,
    /// or the slab is at capacity.
    pub fn schedule(
        &mut self,
        delay: Duration<U>,
        payload: T,
    ) -> Result<TimerHandle<T>, WheelError> {
        let ticks_needed = delay.div_ceil(Duration::from(self.tick));
        let ticks_ahead = ticks_needed.max(1);
        let max_delay = self.max_delay_ticks();
        if ticks_ahead > max_delay {
            return Err(WheelError::DelayTooLong {
                delay: ticks_ahead,
                max: max_delay,
            });
        }
        let deadline = self
            .cursor
            .checked_add(ticks_ahead)
            .ok_or(WheelError::Overflow)?;

        let slot = self.slot_index(deadline);
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
        Ok(TimerHandle { idx, generation })
    }

    /// Cancels a timer by handle. Returns `true` if the timer was found and cancelled.
    pub fn cancel(&mut self, handle: TimerHandle<T>) -> bool {
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
                let slot = self.slot_index(deadline);
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

    /// Advances the wheel using the current monotonic timestamp and invokes `on_fire` for each due timer.
    ///
    /// `on_fire` receives the handle (for callers that stash it) and the payload by value.
    pub fn tick(&mut self, mut on_fire: impl FnMut(TimerHandle<T>, T)) {
        let now = U::now();
        let elapsed = now - self.start;
        let now_tick = elapsed / Duration::from(self.tick); // dimensionless tick count
        if now_tick <= self.cursor {
            return;
        }
        let mut tick = self.cursor + 1;
        while tick <= now_tick {
            let slot = self.slot_index(tick);
            let mut head = self.slots[slot];
            let mut pending_head: Option<SlabIndex<T>> = None;
            while let Some(idx) = head {
                // Save next before we potentially move/free.
                let next = self.slab.get(idx).and_then(|n| n.next);
                let due = self.slab.get(idx).is_some_and(|n| n.deadline <= now_tick);

                if due {
                    if let Some(node) = self.slab.get_mut(idx)
                        && let Some(payload) = node.payload.take()
                    {
                        on_fire(
                            TimerHandle {
                                idx,
                                generation: node.generation,
                            },
                            payload,
                        );
                    }
                    let _ = self.slab.free(idx);
                } else {
                    // Keep pending by pushing to pending_head
                    if let Some(node) = self.slab.get_mut(idx) {
                        node.next = pending_head;
                        node.prev = None;
                    }
                    if let Some(ph) = pending_head
                        && let Some(pnode) = self.slab.get_mut(ph)
                    {
                        pnode.prev = Some(idx);
                    }
                    pending_head = Some(idx);
                }
                head = next;
            }
            self.slots[slot] = pending_head;
            tick += 1;
        }
        self.cursor = now_tick;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::num::{NonZeroU64, NonZeroUsize};

    /// Deterministic unit for testing.
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

    fn wheel_u32(capacity: usize) -> Wheel<u32, TestUnit> {
        set_now(0);
        Wheel::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroDuration::<TestUnit>::new(NonZeroU64::new(1).unwrap()),
            NonZeroUsize::new(capacity).unwrap(),
        )
    }

    #[test]
    fn fires_due_timers_no_alloc() {
        let mut w = wheel_u32(4);
        let h1 = w.schedule(Duration::<TestUnit>::new(0), 10).unwrap();
        let h2 = w.schedule(Duration::<TestUnit>::new(1), 20).unwrap();
        let mut fired = Vec::new();
        set_now(1);
        w.tick(|h, v| fired.push((h, v)));
        fired.sort_by_key(|(_, v)| *v);
        assert_eq!(fired, vec![(h1, 10), (h2, 20)]);

        fired.clear();
        set_now(3);
        w.tick(|h, v| fired.push((h, v)));
        assert!(fired.is_empty());
    }

    #[test]
    fn cancel_prevents_fire() {
        let mut w = wheel_u32(2);
        let h = w.schedule(Duration::<TestUnit>::new(0), 42).unwrap();
        assert!(w.cancel(h));
        let mut fired = Vec::new();
        set_now(1);
        w.tick(|_, v| fired.push(v));
        assert!(fired.is_empty());
    }

    #[test]
    fn stale_handle_rejected() {
        let mut w = wheel_u32(1);
        let h1 = w.schedule(Duration::<TestUnit>::new(0), 1).unwrap();
        set_now(1);
        w.tick(|_, _| {});
        let h2 = w.schedule(Duration::<TestUnit>::new(0), 2).unwrap();
        assert_ne!(h1.generation, h2.generation);
        assert!(!w.cancel(h1));
        assert!(w.cancel(h2));
    }

    #[test]
    fn capacity_exhaustion() {
        let mut w = wheel_u32(1);
        let _ = w.schedule(Duration::<TestUnit>::new(0), 1).unwrap();
        assert!(
            matches!(
                w.schedule(Duration::<TestUnit>::new(0), 2),
                Err(WheelError::Capacity)
            ),
            "should fail when slab is full"
        );
    }

    #[test]
    fn pending_kept_until_due() {
        let mut w = wheel_u32(2);
        let h = w.schedule(Duration::<TestUnit>::new(2), 99).unwrap();
        let mut fired: Vec<(TimerHandle<u32>, u32)> = Vec::new();
        set_now(1);
        w.tick(|handle, v| fired.push((handle, v)));
        assert!(fired.is_empty(), "not due yet");
        set_now(2);
        w.tick(|handle, v| fired.push((handle, v)));
        assert_eq!(fired, vec![(h, 99)]);
    }

    #[test]
    fn jump_ahead_fires_intermediate() {
        let mut w = wheel_u32(3);
        let h1 = w.schedule(Duration::<TestUnit>::new(0), 1).unwrap();
        let h2 = w.schedule(Duration::<TestUnit>::new(2), 3).unwrap();
        let mut seen = Vec::new();
        set_now(3);
        w.tick(|h, v| seen.push((h, v)));
        seen.sort_by_key(|(_, v)| *v);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (h1, 1));
        assert_eq!(seen[1], (h2, 3));
    }
}

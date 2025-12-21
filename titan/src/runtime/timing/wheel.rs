//! Hashed timing wheel with O(1) schedule/cancel and bounded per-tick work.
//!
//! Single-level, power-of-two slots; per-thread, shared-nothing. Tick streams fired timers via
//! callback to avoid per-tick allocation.

use crate::runtime::timing::slab::{Entry, Slab, SlabIndex};
use crate::runtime::timing::time::{Duration, TimeUnit, Timestamp};

/// Handle returned to callers; includes index and generation to detect stale use.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimerHandle<Role, T> {
    pub idx: SlabIndex<T>,
    pub generation: u32,
    pub _role: core::marker::PhantomData<Role>,
}

/// Wheel configuration (immutable after creation).
pub struct WheelConfig {
    pub slots: PowerOfTwo,
    pub tick_ns: u64,
    pub capacity: core::num::NonZeroUsize,
}

/// Hashed timing wheel.
pub struct Wheel<T, Role, U: TimeUnit> {
    slots: Vec<Option<SlabIndex<T>>>, // head of list per slot
    slab: Slab<T>,
    mask: usize,
    pub tick_ns: u64,
    cursor: u64, // current tick
    pub max_fired_per_tick: usize,
    pub max_slot_depth: usize,
    pub slab_high_water: usize,
    _role: core::marker::PhantomData<Role>,
    _unit: core::marker::PhantomData<U>,
}

/// Witness type for power-of-two values.
/// do we need this...?? its not a breaking thing but something we enforce... maybe get ridofit
#[derive(Clone, Copy, Debug)]
pub struct PowerOfTwo(usize);

impl PowerOfTwo {
    pub fn new(val: usize) -> Option<Self> {
        if val.is_power_of_two() && val > 0 {
            Some(Self(val))
        } else {
            None
        }
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl<T, Role, U: TimeUnit> Wheel<T, Role, U> {
    /// Convert a monotonic timestamp (nanoseconds) into wheel ticks using `tick_ns`.
    #[inline]
    pub fn instant_to_tick(&self, now_ns: u64) -> Timestamp<U> {
        Timestamp::new(now_ns / self.tick_ns)
    }

    /// Create a new wheel. `slots` must be power of two.
    pub fn new(cfg: WheelConfig) -> Self {
        let slots_val = cfg.slots.get();
        Self {
            slots: vec![None; slots_val],
            slab: Slab::with_capacity(cfg.capacity),
            mask: slots_val - 1,
            tick_ns: cfg.tick_ns,
            cursor: 0,
            max_fired_per_tick: 0,
            max_slot_depth: 0,
            slab_high_water: 0,
            _role: core::marker::PhantomData,
            _unit: core::marker::PhantomData,
        }
    }

    /// Schedule a timer `delay_ticks` from now with payload `payload`.
    pub fn schedule(&mut self, delay: Duration<U>, payload: T) -> Option<TimerHandle<Role, T>> {
        let delay_ticks = delay.as_u64();
        // Schedule relative to "next tick" to ensure delay=0 fires on next tick.
        let deadline = self.cursor + 1 + delay_ticks;
        let slot = (deadline as usize) & self.mask;
        let head = self.slots[slot];
        let (idx, generation) = {
            let (idx, node) = self.slab.alloc(payload, deadline)?;
            node.next = head;
            node.prev = None;
            let generation = node.generation;
            (idx, generation)
        };
        // Insert at head of slot list
        if let Some(head_idx) = head {
            if let Some(head) = self.slab.get_mut(head_idx) {
                head.prev = Some(idx);
            }
        }
        self.slots[slot] = Some(idx);
        self.slab_high_water = self
            .slab_high_water
            .max(self.slab.capacity() - self.free_slots());
        Some(TimerHandle {
            idx,
            generation,
            _role: core::marker::PhantomData,
        })
    }

    /// Cancel a timer by handle.
    pub fn cancel(&mut self, handle: TimerHandle<Role, T>) -> bool {
        if let Some(node) = self.slab.get(handle.idx) {
            if node.generation != handle.generation {
                return false;
            }
        } else {
            return false;
        }

        let idx = handle.idx;
        // Unlink from list
        if let Some(node) = self.slab.get_mut(idx) {
            let next = node.next;
            let prev = node.prev;
            if let Some(p) = prev {
                if let Some(pnode) = self.slab.get_mut(p) {
                    pnode.next = next;
                }
            } else {
                // head of slot list; find slot
                let slot = (node.deadline as usize) & self.mask;
                self.slots[slot] = next;
            }
            if let Some(n) = next {
                if let Some(nnode) = self.slab.get_mut(n) {
                    nnode.prev = prev;
                }
            }
        }

        self.slab.free(idx).is_some()
    }

    /// Advance the wheel to `now_tick` and invoke `on_fire` for each due timer.
    ///
    /// `on_fire` receives the handle (for callers that stash it) and the payload by value.
    /// No allocations occur during tick.
    pub fn tick(&mut self, now: Timestamp<U>, mut on_fire: impl FnMut(TimerHandle<Role, T>, T)) {
        let now_tick = now.as_u64();
        if now_tick <= self.cursor {
            return;
        }
        let mut tick = self.cursor + 1;
        while tick <= now_tick {
            let slot = (tick as usize) & self.mask;
            let mut head = self.slots[slot];
            let mut pending_head: Option<SlabIndex<T>> = None;
            let mut slot_depth = 0;
            let mut fired_count = 0;
            while let Some(idx) = head {
                slot_depth += 1;
                // Save next before we potentially move/free.
                let next = self.slab.get(idx).and_then(|n| n.next);
                let due = self
                    .slab
                    .get(idx)
                    .map(|n| n.deadline <= now_tick)
                    .unwrap_or(false);

                if due {
                    if let Some(node) = self.slab.get_mut(idx) {
                        if let Some(payload) = node.payload.take() {
                            on_fire(
                                TimerHandle {
                                    idx,
                                    generation: node.generation,
                                    _role: core::marker::PhantomData,
                                },
                                payload,
                            );
                            fired_count += 1;
                        }
                    }
                    self.slab.free(idx);
                } else {
                    // Keep pending by pushing to pending_head
                    if let Some(node) = self.slab.get_mut(idx) {
                        node.next = pending_head;
                        node.prev = None;
                    }
                    if let Some(ph) = pending_head {
                        if let Some(pnode) = self.slab.get_mut(ph) {
                            pnode.prev = Some(idx);
                        }
                    }
                    pending_head = Some(idx);
                }
                head = next;
            }
            self.slots[slot] = pending_head;
            self.max_slot_depth = self.max_slot_depth.max(slot_depth);
            self.max_fired_per_tick = self.max_fired_per_tick.max(fired_count);
            tick += 1;
        }
        self.cursor = now_tick;
    }

    /// Free slots remaining.
    fn free_slots(&self) -> usize {
        let mut count = 0;
        let mut head = self.slab_free_head();
        while let Some(idx) = head {
            count += 1;
            head = match &self.slab_entry(idx) {
                Some(Entry::Free { next, .. }) => *next,
                _ => None,
            };
        }
        count
    }

    fn slab_free_head(&self) -> Option<SlabIndex<T>> {
        self.slab.free_head
    }

    fn slab_entry(&self, idx: SlabIndex<T>) -> Option<&Entry<T>> {
        self.slab.entries.get(idx.0 as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::timing::time::Millis;
    use std::num::NonZeroUsize;

    fn wheel_u32(capacity: usize) -> Wheel<u32, (), Millis> {
        Wheel::new(WheelConfig {
            slots: PowerOfTwo::new(8).unwrap(),
            tick_ns: 1,
            capacity: NonZeroUsize::new(capacity).unwrap(),
        })
    }

    #[test]
    fn fires_due_timers_no_alloc() {
        let mut w = wheel_u32(4);
        let h1 = w.schedule(Duration::<Millis>::new(0), 10).unwrap();
        let h2 = w.schedule(Duration::<Millis>::new(1), 20).unwrap();
        let mut fired = Vec::new();
        w.tick(Timestamp::<Millis>::new(1), |h, v| fired.push((h, v)));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].0, h1);
        assert_eq!(fired[0].1, 10);

        fired.clear();
        w.tick(Timestamp::<Millis>::new(3), |h, v| fired.push((h, v)));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].0, h2);
        assert_eq!(fired[0].1, 20);
    }

    #[test]
    fn cancel_prevents_fire() {
        let mut w = wheel_u32(2);
        let h = w.schedule(Duration::<Millis>::new(0), 42).unwrap();
        assert!(w.cancel(h));
        let mut fired = Vec::new();
        w.tick(Timestamp::<Millis>::new(1), |_, v| fired.push(v));
        assert!(fired.is_empty());
    }

    #[test]
    fn stale_handle_rejected() {
        let mut w = wheel_u32(1);
        let h1 = w.schedule(Duration::<Millis>::new(0), 1).unwrap();
        w.tick(Timestamp::<Millis>::new(1), |_, _| {});
        let h2 = w.schedule(Duration::<Millis>::new(0), 2).unwrap();
        assert_ne!(h1.generation, h2.generation);
        assert!(!w.cancel(h1));
        assert!(w.cancel(h2));
    }

    #[test]
    fn capacity_exhaustion() {
        let mut w = wheel_u32(1);
        let _ = w.schedule(Duration::<Millis>::new(0), 1).unwrap();
        assert!(
            w.schedule(Duration::<Millis>::new(0), 2).is_none(),
            "should fail when slab is full"
        );
    }

    #[test]
    fn pending_kept_until_due() {
        let mut w = wheel_u32(2);
        let h = w.schedule(Duration::<Millis>::new(2), 99).unwrap();
        let mut fired: Vec<(TimerHandle<(), u32>, u32)> = Vec::new();
        w.tick(Timestamp::<Millis>::new(2), |handle, v| {
            fired.push((handle, v))
        });
        assert!(fired.is_empty(), "not due yet");
        w.tick(Timestamp::<Millis>::new(3), |handle, v| {
            fired.push((handle, v))
        });
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].0, h);
        assert_eq!(fired[0].1, 99);
    }

    #[test]
    fn jump_ahead_fires_intermediate() {
        let mut w = wheel_u32(3);
        let h1 = w.schedule(Duration::<Millis>::new(0), 1).unwrap();
        let h2 = w.schedule(Duration::<Millis>::new(2), 3).unwrap();
        let mut seen = Vec::new();
        w.tick(Timestamp::<Millis>::new(3), |h, v| seen.push((h, v)));
        seen.sort_by_key(|(_, v)| *v);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (h1, 1));
        assert_eq!(seen[1], (h2, 3));
    }
}

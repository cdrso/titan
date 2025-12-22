//! Generational slab allocator for intrusive timer nodes.

use core::marker::PhantomData;
use core::num::NonZeroUsize;

/// Newtype for slab indices to prevent cross-slab misuse.
// Manual Copy/Clone: derive would require T: Copy, but PhantomData is just a marker.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SlabIndex<T>(u32, PhantomData<T>);

impl<T> Copy for SlabIndex<T> {}

impl<T> Clone for SlabIndex<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> From<u32> for SlabIndex<T> {
    #[inline]
    fn from(idx: u32) -> Self {
        Self(idx, PhantomData)
    }
}

impl<T> From<SlabIndex<T>> for u32 {
    #[inline]
    fn from(idx: SlabIndex<T>) -> Self {
        idx.0
    }
}

impl<T> From<SlabIndex<T>> for usize {
    #[inline]
    fn from(idx: SlabIndex<T>) -> Self {
        idx.0 as Self
    }
}

/// Timer node stored in the slab.
pub struct Node<T> {
    /// Timer payload; always `Some` while occupied, taken when fired.
    pub payload: Option<T>,
    /// Generation counter for ABA protection.
    pub generation: u32,
    /// Next pointer in the per-slot timer list.
    pub next: Option<SlabIndex<T>>,
    /// Prev pointer in the per-slot timer list (None for head).
    pub prev: Option<SlabIndex<T>>,
    /// Absolute deadline in wheel ticks.
    // TODO check if we need typed time units here like we use elsewhere
    pub deadline: u64,
}

/// Metadata for a free slab slot.
pub struct FreeSlot<T> {
    /// Next free slot in the free list.
    pub next: Option<SlabIndex<T>>,
    /// Generation counter for ABA protection.
    pub generation: u32,
}

/// Slot in the slab.
pub enum Entry<T> {
    /// Occupied timer node.
    Occupied(Node<T>),
    /// Free slot with link to next free.
    Free(FreeSlot<T>),
}

/// Fixed-capacity slab with intrusive free list.
pub struct Slab<T> {
    entries: Vec<Entry<T>>,
    free_head: Option<SlabIndex<T>>,
}

impl<T> Slab<T> {
    /// Creates a slab with given capacity, all slots free.
    ///
    /// # Panics
    ///
    /// Panics if `cap` exceeds `u32::MAX`.
    #[must_use]
    pub fn with_capacity(cap: NonZeroUsize) -> Self {
        let capacity = cap.get();
        let entries: Vec<_> = (0..capacity)
            .map(|i| {
                let next = if i + 1 < capacity {
                    // Safe: panics documented above if capacity > u32::MAX
                    Some(SlabIndex::from(
                        // TODO parse dont validate?
                        u32::try_from(i + 1).expect("capacity should not exceed u32::MAX"),
                    ))
                } else {
                    None
                };

                Entry::Free(FreeSlot {
                    next,
                    generation: 0,
                })
            })
            .collect();
        Self {
            entries,
            free_head: Some(SlabIndex::from(0u32)),
        }
    }

    /// Allocates a new node, returning its index and mutable ref.
    pub fn alloc(&mut self, payload: T, deadline: u64) -> Option<(SlabIndex<T>, &mut Node<T>)> {
        let head = self.free_head?;
        let (next_free, generation) = match &self.entries[usize::from(head)] {
            Entry::Free(slot) => (slot.next, slot.generation),
            Entry::Occupied(_) => unreachable!("free_head must point to free slot"),
        };
        self.free_head = next_free;

        self.entries[usize::from(head)] = Entry::Occupied(Node {
            payload: Some(payload),
            generation,
            next: None,
            prev: None,
            deadline,
        });

        match &mut self.entries[usize::from(head)] {
            Entry::Occupied(node) => Some((head, node)),
            Entry::Free(_) => None,
        }
    }

    /// Frees a node by index; returns its generation for handle invalidation.
    pub fn free(&mut self, idx: SlabIndex<T>) -> Option<u32> {
        let generation = match &self.entries[usize::from(idx)] {
            Entry::Occupied(n) => n.generation,
            Entry::Free(_) => return None,
        };
        self.entries[usize::from(idx)] = Entry::Free(FreeSlot {
            next: self.free_head,
            generation: generation.wrapping_add(1),
        });
        self.free_head = Some(idx);
        Some(generation)
    }

    /// Gets immutable ref to node by index if occupied.
    #[must_use]
    pub fn get(&self, idx: SlabIndex<T>) -> Option<&Node<T>> {
        match &self.entries[usize::from(idx)] {
            Entry::Occupied(n) => Some(n),
            Entry::Free(_) => None,
        }
    }

    /// Gets mutable ref to node by index if occupied.
    pub fn get_mut(&mut self, idx: SlabIndex<T>) -> Option<&mut Node<T>> {
        match &mut self.entries[usize::from(idx)] {
            Entry::Occupied(n) => Some(n),
            Entry::Free(_) => None,
        }
    }
}

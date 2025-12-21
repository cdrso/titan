//! Generational slab allocator for intrusive timer nodes.

/// Slot in the slab.
pub enum Entry<T> {
    /// Occupied timer node.
    Occupied(Node<T>),
    /// Free slot with link to next free.
    Free {
        next: Option<SlabIndex<T>>,
        generation: u32,
    },
}

/// Timer node stored in the slab.
pub struct Node<T> {
    pub payload: Option<T>,
    pub generation: u32,
    pub next: Option<SlabIndex<T>>,
    pub prev: Option<SlabIndex<T>>,
    pub deadline: u64, // raw ticks; wheel interprets
}

/// Newtype for slab indices to prevent cross-slab misuse.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SlabIndex<T>(pub(crate) u32, pub(crate) core::marker::PhantomData<T>);

impl<T> Copy for SlabIndex<T> {}
impl<T> Clone for SlabIndex<T> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Fixed-capacity slab with intrusive free list.
pub struct Slab<T> {
    pub(crate) entries: Vec<Entry<T>>,
    pub(crate) free_head: Option<SlabIndex<T>>,
}

impl<T> Slab<T> {
    /// Create a slab with given capacity, all slots free. Capacity must be > 0.
    pub fn with_capacity(cap: core::num::NonZeroUsize) -> Self {
        let cap_usize = cap.get();
        let mut entries = Vec::with_capacity(cap_usize);
        // Initialize free list in reverse for O(1) push/pop.
        for i in 0..cap_usize {
            let next = if i + 1 < cap_usize {
                Some(SlabIndex((i + 1) as u32, core::marker::PhantomData))
            } else {
                None
            };
            entries.push(Entry::Free {
                next,
                generation: 0,
            });
        }
        Self {
            entries,
            free_head: Some(SlabIndex(0, core::marker::PhantomData)),
        }
    }

    /// Allocate a new node, returning its index and mutable ref.
    pub fn alloc(&mut self, payload: T, deadline: u64) -> Option<(SlabIndex<T>, &mut Node<T>)> {
        let head = self.free_head?;
        let (next_free, generation) = match &self.entries[head.0 as usize] {
            Entry::Free { next, generation } => (*next, *generation),
            _ => unreachable!("free_head must point to free slot"),
        };
        self.free_head = next_free;

        self.entries[head.0 as usize] = Entry::Occupied(Node {
            payload: Some(payload),
            generation,
            next: None,
            prev: None,
            deadline,
        });
        match &mut self.entries[head.0 as usize] {
            Entry::Occupied(node) => Some((head, node)),
            _ => None,
        }
    }

    /// Free a node by index; returns its generation for handle invalidation.
    pub fn free(&mut self, idx: SlabIndex<T>) -> Option<u32> {
        let generation = match &self.entries[idx.0 as usize] {
            Entry::Occupied(n) => n.generation,
            Entry::Free { .. } => return None,
        };
        self.entries[idx.0 as usize] = Entry::Free {
            next: self.free_head,
            generation: generation.wrapping_add(1),
        };
        self.free_head = Some(idx);
        Some(generation)
    }

    /// Get immutable ref to node by index if occupied.
    pub fn get(&self, idx: SlabIndex<T>) -> Option<&Node<T>> {
        match &self.entries[idx.0 as usize] {
            Entry::Occupied(n) => Some(n),
            _ => None,
        }
    }

    /// Get mutable ref to node by index if occupied.
    pub fn get_mut(&mut self, idx: SlabIndex<T>) -> Option<&mut Node<T>> {
        match &mut self.entries[idx.0 as usize] {
            Entry::Occupied(n) => Some(n),
            _ => None,
        }
    }

    /// Capacity of the slab.
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }
}

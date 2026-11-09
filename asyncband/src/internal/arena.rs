// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;
use std::mem;
use std::num::NonZeroUsize;

/// Identifies a slot in an arena.
///
/// The non-zero representation lets wrappers such as `WaiterId` retain a niche when stored in an
/// `Option`. An ID issued to a consumer remains valid only while its slot is occupied. Slot IDs
/// deliberately carry no generation: each consumer supplies the cheaper lifecycle rule that
/// matches its waiter storage.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SlotId(NonZeroUsize);

impl SlotId {
    fn from_index(index: usize) -> Self {
        // `Slot<T>` is non-zero-sized, so a Vec of slots cannot reach `usize::MAX` elements.
        let encoded = index
            .checked_add(1)
            .expect("arena index must fit in a non-zero usize");
        Self(NonZeroUsize::new(encoded).expect("encoded arena index must be non-zero"))
    }

    fn index(self) -> usize {
        self.0.get() - 1
    }
}

impl fmt::Debug for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SlotId").field(&self.index()).finish()
    }
}

/// Minimal reusable storage for internal waiter state.
///
/// The occupied length equals the number of `Occupied` slots. Every `Vacant` slot appears exactly
/// once in the singly linked vacant list starting at `vacant_head`. Removing a value makes its slot
/// ID available for immediate reuse.
///
/// The first occupant lives in the inline `first` slot, so registering a lone waiter never
/// allocates. The second registration spills `first` into the slot vector, which then follows the
/// rules above for the arena's remaining lifetime. Every arena therefore carries one inline value
/// of `T` in exchange for an allocation-free first registration.
#[derive(Debug)]
pub struct Arena<T> {
    /// The occupant of slot 0 until the vector first becomes non-empty; `None` afterwards.
    first: Option<T>,
    slots: Vec<Slot<T>>,
    /// The head of the vacant list, or `None` when every slot is occupied.
    vacant_head: Option<SlotId>,
    len: usize,
}

#[derive(Debug)]
enum Slot<T> {
    Occupied(T),
    Vacant { next: Option<SlotId> },
}

impl<T> Arena<T> {
    pub const fn new() -> Self {
        Self {
            first: None,
            slots: vec![],
            vacant_head: None,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            first: None,
            slots: Vec::with_capacity(capacity),
            vacant_head: None,
            len: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> SlotId {
        let id = if let Some(id) = self.vacant_head {
            let slot = self
                .slots
                .get_mut(id.index())
                .expect("arena free list must point to a slot");
            self.vacant_head = match slot {
                Slot::Vacant { next } => *next,
                Slot::Occupied(_) => {
                    unreachable!("arena free list must point to a vacant slot")
                }
            };
            *slot = Slot::Occupied(value);
            id
        } else if self.slots.is_empty() {
            match self.first.take() {
                None => {
                    self.first = Some(value);
                    SlotId::from_index(0)
                }
                Some(first) => {
                    // Spill the inline slot into the vector so slot 0 keeps its ID, then append
                    // the new value as slot 1.
                    self.slots.push(Slot::Occupied(first));
                    let id = SlotId::from_index(self.slots.len());
                    self.slots.push(Slot::Occupied(value));
                    id
                }
            }
        } else {
            let id = SlotId::from_index(self.slots.len());
            self.slots.push(Slot::Occupied(value));
            id
        };

        self.len += 1;
        id
    }

    /// Returns the value at `index`, looking through the inline slot while the vector is empty.
    fn slot_ref(&self, index: usize) -> Option<&T> {
        if index == 0 && self.slots.is_empty() {
            return self.first.as_ref();
        }
        match self.slots.get(index) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    /// Returns the value at `index` mutably, looking through the inline slot while the vector is
    /// empty.
    fn slot_mut(&mut self, index: usize) -> Option<&mut T> {
        if index == 0 && self.slots.is_empty() {
            return self.first.as_mut();
        }
        match self.slots.get_mut(index) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    pub fn get(&self, id: SlotId) -> Option<&T> {
        self.slot_ref(id.index())
    }

    pub fn get_mut(&mut self, id: SlotId) -> Option<&mut T> {
        self.slot_mut(id.index())
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the number of heap-allocated slots; used by tests to pin the inline fast path.
    #[cfg(test)]
    pub fn vector_len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes the value stored at `id`.
    ///
    /// # Panics
    ///
    /// Panics if the slot ID is out of bounds or its slot is already vacant. Either case is an
    /// internal waiter-lifecycle violation rather than a recoverable lookup failure.
    #[track_caller]
    pub fn remove(&mut self, id: SlotId) -> T {
        let index = id.index();
        if index == 0 && self.slots.is_empty() {
            // The inline slot never joins the vacant list: while the vector is empty, slot 0 is
            // the only ID that ever existed, so reuse goes through `first` directly.
            let value = self.first.take().expect("arena slot ID must be occupied");
            self.len -= 1;
            return value;
        }
        let slot = self
            .slots
            .get_mut(index)
            .expect("arena slot ID must be in bounds");
        let value = match mem::replace(
            slot,
            Slot::Vacant {
                next: self.vacant_head,
            },
        ) {
            Slot::Occupied(value) => value,
            vacant @ Slot::Vacant { .. } => {
                *slot = vacant;
                panic!("arena slot ID must be occupied");
            }
        };
        self.len -= 1;
        self.vacant_head = Some(id);
        value
    }

    /// Collects occupied values in slot order, retaining the allocation for reuse.
    ///
    /// All previous slot IDs become invalid and may be reused after collection.
    #[inline]
    pub fn take_all<C: FromIterator<T>>(&mut self) -> C {
        self.vacant_head = None;
        self.len = 0;
        self.first
            .take()
            .into_iter()
            .chain(self.slots.drain(..).filter_map(|slot| match slot {
                Slot::Occupied(value) => Some(value),
                Slot::Vacant { .. } => None,
            }))
            .collect()
    }

    /// Consumes the arena, yielding occupied values in slot order.
    ///
    /// The returned iterator owns the backing allocation and releases it when dropped.
    #[inline]
    pub fn into_iter(self) -> impl Iterator<Item = T> {
        let slots = self.slots.into_iter().filter_map(|slot| match slot {
            Slot::Occupied(value) => Some(value),
            Slot::Vacant { .. } => None,
        });
        self.first.into_iter().chain(slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_id_preserves_the_option_niche() {
        assert_eq!(size_of::<SlotId>(), size_of::<Option<SlotId>>());
    }

    #[test]
    fn removed_slots_are_reused() {
        let mut arena = Arena::new();
        let first = arena.insert("first");
        let second = arena.insert("second");

        assert_eq!(arena.remove(first), "first");
        let replacement = arena.insert("replacement");

        assert_eq!(replacement, first);
        assert_eq!(arena.get(replacement), Some(&"replacement"));
        assert_eq!(arena.get(second), Some(&"second"));
    }

    #[test]
    fn lone_waiter_uses_the_inline_slot() {
        let mut arena = Arena::new();
        let id = arena.insert("only");

        // The vector-only implementation this test pins against would allocate here already.
        assert_eq!(arena.vector_len(), 0);

        assert_eq!(id, SlotId::from_index(0));
        assert_eq!(arena.get(id), Some(&"only"));
        assert_eq!(arena.len(), 1);

        assert_eq!(arena.remove(id), "only");
        assert!(arena.is_empty());
        assert_eq!(arena.get(id), None);

        // Slot 0 is handed out again while the vector is still empty.
        let again = arena.insert("again");
        assert_eq!(again, id);
        assert_eq!(arena.vector_len(), 0);
    }

    #[test]
    fn spill_preserves_ids_and_values() {
        let mut arena = Arena::new();
        let first = arena.insert(1);
        let second = arena.insert(2);
        let third = arena.insert(3);

        assert_eq!(first, SlotId::from_index(0));
        assert_eq!(
            (arena.get(first), arena.get(second), arena.get(third)),
            (Some(&1), Some(&2), Some(&3))
        );

        // Removing the spilled slot 0 returns it to the vacant list, not to the inline slot.
        assert_eq!(arena.remove(first), 1);
        let replacement = arena.insert(10);
        assert_eq!(replacement, first);
        assert_eq!(arena.get(replacement), Some(&10));
        assert_eq!(arena.get(third), Some(&3));
    }

    #[test]
    fn take_all_starts_from_the_inline_slot_again() {
        let mut arena = Arena::new();
        arena.insert(1);
        arena.insert(2);

        assert_eq!(arena.take_all::<Vec<_>>(), vec![1, 2]);
        assert!(arena.is_empty());

        // The arena is back to pre-spill mode: the next insert goes to the inline slot.
        let id = arena.insert(3);
        assert_eq!(id, SlotId::from_index(0));
        assert_eq!(arena.get(id), Some(&3));
    }

    #[test]
    fn take_all_retains_capacity_and_restarts_slot_ids() {
        let mut arena = Arena::with_capacity(3);
        let first = arena.insert(1);
        let second = arena.insert(2);
        let third = arena.insert(3);
        let capacity = arena.slots.capacity();
        arena.remove(second);

        assert_eq!(arena.take_all::<Vec<_>>(), vec![1, 3]);
        assert_eq!(arena.len(), 0);
        assert_eq!(arena.slots.capacity(), capacity);
        assert_eq!(
            [arena.insert(4), arena.insert(5), arena.insert(6)],
            [first, second, third]
        );
    }

    #[test]
    fn into_iter_skips_vacant_slots() {
        let mut arena = Arena::new();
        arena.insert(1);
        let removed = arena.insert(2);
        arena.insert(3);
        arena.remove(removed);

        assert_eq!(arena.into_iter().collect::<Vec<_>>(), vec![1, 3]);
    }
}

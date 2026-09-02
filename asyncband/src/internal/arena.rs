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

use std::mem;
use std::num::NonZeroUsize;

const NO_VACANT_SLOT: usize = usize::MAX;

/// Identifies a reusable slot while that slot is occupied.
///
/// The non-zero representation lets wrappers such as `WaiterId` retain a niche when stored in an
/// `Option`. Slot IDs deliberately carry no generation: each consumer supplies the cheaper
/// lifecycle rule that matches its waiter storage.
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

impl std::fmt::Debug for SlotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SlotId").field(&self.index()).finish()
    }
}

/// Minimal reusable storage for internal waiter state.
///
/// The occupied length equals the number of `Occupied` slots. Every `Vacant` slot appears exactly
/// once in the singly linked vacant list, which starts at `next_vacant` and terminates at
/// [`NO_VACANT_SLOT`]. Removing a value makes its slot ID available for immediate reuse. Vacant
/// slots at the logical tail are removed when they are reachable from the head of the vacant list;
/// this shortens later scans without releasing the vector's allocation.
#[derive(Debug)]
pub struct Arena<T> {
    slots: Vec<Slot<T>>,
    /// The next reusable slot, or [`NO_VACANT_SLOT`] when every slot is occupied.
    next_vacant: usize,
    len: usize,
}

#[derive(Debug)]
enum Slot<T> {
    Occupied(T),
    Vacant { next: usize },
}

impl<T> Arena<T> {
    pub const fn new() -> Self {
        Self {
            slots: vec![],
            next_vacant: NO_VACANT_SLOT,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            next_vacant: NO_VACANT_SLOT,
            len: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> SlotId {
        self.len += 1;

        let index = if self.next_vacant == NO_VACANT_SLOT {
            let index = self.slots.len();
            self.slots.push(Slot::Occupied(value));
            index
        } else {
            let index = self.next_vacant;
            self.next_vacant = match self.slots.get(index) {
                Some(Slot::Vacant { next }) => *next,
                Some(Slot::Occupied(_)) | None => {
                    unreachable!("arena free list must point to a vacant slot")
                }
            };
            self.slots[index] = Slot::Occupied(value);
            index
        };

        SlotId::from_index(index)
    }

    pub fn get(&self, id: SlotId) -> Option<&T> {
        match self.slots.get(id.index()) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    pub fn get_mut(&mut self, id: SlotId) -> Option<&mut T> {
        match self.slots.get_mut(id.index()) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|slot| match slot {
            Slot::Occupied(value) => Some(value),
            Slot::Vacant { .. } => None,
        })
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
        if index + 1 == self.slots.len() {
            let value = match self.slots.pop().expect("arena slot ID must be in bounds") {
                Slot::Occupied(value) => value,
                vacant @ Slot::Vacant { .. } => {
                    self.slots.push(vacant);
                    panic!("arena slot ID must be occupied");
                }
            };
            self.len -= 1;
            self.trim_vacant_tail();
            return value;
        }

        let slot = self
            .slots
            .get_mut(index)
            .expect("arena slot ID must be in bounds");
        let value = match mem::replace(
            slot,
            Slot::Vacant {
                next: self.next_vacant,
            },
        ) {
            Slot::Occupied(value) => value,
            vacant @ Slot::Vacant { .. } => {
                *slot = vacant;
                panic!("arena slot ID must be occupied");
            }
        };
        self.len -= 1;
        self.next_vacant = index;
        value
    }

    fn trim_vacant_tail(&mut self) {
        while self.next_vacant != NO_VACANT_SLOT && self.next_vacant + 1 == self.slots.len() {
            let Slot::Vacant { next } = self
                .slots
                .pop()
                .expect("vacant-list head must refer to a slot")
            else {
                unreachable!("arena free list must point to a vacant slot")
            };
            self.next_vacant = next;
        }
    }

    /// Drains every occupied value in slot order while retaining the allocation for reuse.
    ///
    /// After a non-empty drain, every previously issued slot ID becomes invalid, including IDs for
    /// slots that were already vacant. Consumers that retain IDs across this operation must supply
    /// their own epoch check.
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.next_vacant = NO_VACANT_SLOT;
        self.len = 0;
        self.slots.drain(..).filter_map(|slot| match slot {
            Slot::Occupied(value) => Some(value),
            Slot::Vacant { .. } => None,
        })
    }

    /// Takes every occupied value and the backing allocation in slot order.
    #[inline]
    pub fn take_all(&mut self) -> impl Iterator<Item = T> + use<T> {
        self.next_vacant = NO_VACANT_SLOT;
        self.len = 0;
        mem::take(&mut self.slots)
            .into_iter()
            .filter_map(|slot| match slot {
                Slot::Occupied(value) => Some(value),
                Slot::Vacant { .. } => None,
            })
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
    fn removing_tail_slots_shortens_the_logical_storage() {
        let mut arena = Arena::with_capacity(4);
        let ids = [
            arena.insert(0),
            arena.insert(1),
            arena.insert(2),
            arena.insert(3),
        ];
        let capacity = arena.slots.capacity();

        arena.remove(ids[2]);
        assert_eq!(arena.slots.len(), 4);
        arena.remove(ids[3]);

        assert_eq!(arena.slots.len(), 2);
        assert_eq!(arena.slots.capacity(), capacity);
        assert_eq!(arena.values().copied().collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn tail_trimming_preserves_the_remaining_vacant_list() {
        let mut arena = Arena::new();
        let ids = [
            arena.insert(0),
            arena.insert(1),
            arena.insert(2),
            arena.insert(3),
            arena.insert(4),
            arena.insert(5),
        ];

        arena.remove(ids[4]);
        arena.remove(ids[2]);
        arena.remove(ids[5]);

        assert_eq!(arena.insert(20), ids[2]);
        assert_eq!(arena.insert(40), ids[4]);
        assert_eq!(arena.insert(50), ids[5]);
    }

    #[test]
    fn drain_restarts_slot_id_allocation() {
        let mut arena = Arena::with_capacity(3);
        let first = arena.insert(1);
        let second = arena.insert(2);
        let third = arena.insert(3);
        let capacity = arena.slots.capacity();
        arena.remove(second);

        assert_eq!(arena.drain().collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(arena.len(), 0);
        assert_eq!(arena.slots.capacity(), capacity);

        let slot_ids = [arena.insert(4), arena.insert(5), arena.insert(6)];
        assert_eq!(slot_ids, [first, second, third]);
    }

    #[test]
    fn take_all_releases_the_backing_allocation() {
        let mut arena = Arena::new();
        arena.insert(1);
        let removed = arena.insert(2);
        arena.insert(3);
        arena.remove(removed);

        let values = arena.take_all();
        assert_eq!(arena.slots.capacity(), 0);
        assert_eq!(values.collect::<Vec<_>>(), vec![1, 3]);
    }
}

// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem;

/// A stable index into an [`Arena`] for as long as its slot remains occupied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArenaKey(usize);

/// Minimal reusable storage for internal waiter state.
#[derive(Debug)]
pub(crate) struct Arena<T> {
    slots: Vec<Slot<T>>,
    next_vacant: Option<usize>,
    len: usize,
}

#[derive(Debug)]
enum Slot<T> {
    Occupied(T),
    Vacant { next: Option<usize> },
}

impl<T> Arena<T> {
    pub(crate) const fn new() -> Self {
        Self {
            slots: Vec::new(),
            next_vacant: None,
            len: 0,
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            next_vacant: None,
            len: 0,
        }
    }

    pub(crate) fn insert(&mut self, value: T) -> ArenaKey {
        let key = if let Some(index) = self.next_vacant {
            let Slot::Vacant { next } = self.slots[index] else {
                unreachable!("arena free list must point to a vacant slot");
            };
            self.next_vacant = next;
            self.slots[index] = Slot::Occupied(value);
            ArenaKey(index)
        } else {
            let key = ArenaKey(self.slots.len());
            self.slots.push(Slot::Occupied(value));
            key
        };
        self.len += 1;
        key
    }

    pub(crate) fn get(&self, key: ArenaKey) -> Option<&T> {
        match self.slots.get(key.0) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    pub(crate) fn get_mut(&mut self, key: ArenaKey) -> Option<&mut T> {
        match self.slots.get_mut(key.0) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant { .. }) | None => None,
        }
    }

    pub(crate) fn remove(&mut self, key: ArenaKey) -> T {
        let slot = self
            .slots
            .get_mut(key.0)
            .expect("arena key must be in bounds");
        assert!(
            matches!(slot, Slot::Occupied(_)),
            "arena key must refer to an occupied slot"
        );
        let value = match mem::replace(
            slot,
            Slot::Vacant {
                next: self.next_vacant,
            },
        ) {
            Slot::Occupied(value) => value,
            Slot::Vacant { .. } => unreachable!("occupied slot was checked before replacement"),
        };
        self.next_vacant = Some(key.0);
        self.len -= 1;
        value
    }

    /// Takes every occupied value while retaining the allocated slots for reuse.
    pub(crate) fn take_all(&mut self) -> Vec<T> {
        let mut values = Vec::with_capacity(self.len);
        let mut next_vacant = None;

        for (index, slot) in self.slots.iter_mut().enumerate() {
            let previous = mem::replace(slot, Slot::Vacant { next: next_vacant });
            if let Slot::Occupied(value) = previous {
                values.push(value);
            }
            next_vacant = Some(index);
        }

        self.next_vacant = next_vacant;
        self.len = 0;
        values
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn take_all_rebuilds_the_free_list() {
        let mut arena = Arena::with_capacity(3);
        let first = arena.insert(1);
        let second = arena.insert(2);
        let third = arena.insert(3);
        arena.remove(second);

        assert_eq!(arena.take_all(), vec![1, 3]);
        assert_eq!(arena.len(), 0);

        let keys = [arena.insert(4), arena.insert(5), arena.insert(6)];
        assert_eq!(keys, [third, second, first]);
    }
}

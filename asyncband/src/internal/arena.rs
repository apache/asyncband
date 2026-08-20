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

/// A stable index into an [`Arena`] for as long as its slot remains occupied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArenaKey(usize);

impl ArenaKey {
    /// Encodes this key so it can provide a niche when stored in an `Option`-wrapped structure.
    pub fn encode(self) -> NonZeroUsize {
        // `Slot<T>` is non-zero-sized, so a Vec of slots cannot reach `usize::MAX` elements.
        unsafe { NonZeroUsize::new_unchecked(self.0 + 1) }
    }

    /// Decodes a key produced by [`Self::encode`].
    pub fn decode(encoded: NonZeroUsize) -> Self {
        Self(encoded.get() - 1)
    }
}

/// Minimal reusable storage for internal waiter state.
#[derive(Debug)]
pub struct Arena<T> {
    slots: Vec<Slot<T>>,
    /// The next reusable slot, or `slots.len()` when every slot is occupied.
    next_vacant: usize,
    len: usize,
}

/// Values extracted from an [`Arena`], storing the common single-value case inline.
#[derive(Debug)]
pub struct ArenaValues<T> {
    first: Option<T>,
    rest: Vec<T>,
}

impl<T> IntoIterator for ArenaValues<T> {
    type Item = T;
    type IntoIter = std::iter::Chain<std::option::IntoIter<T>, std::vec::IntoIter<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.first.into_iter().chain(self.rest)
    }
}

#[derive(Debug)]
enum Slot<T> {
    Occupied(T),
    Vacant(usize),
}

impl<T> Arena<T> {
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            next_vacant: 0,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            next_vacant: 0,
            len: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> ArenaKey {
        let key = self.next_vacant;
        self.len += 1;

        if key == self.slots.len() {
            self.slots.push(Slot::Occupied(value));
            self.next_vacant = key + 1;
        } else {
            self.next_vacant = match self.slots.get(key) {
                Some(Slot::Vacant(next)) => *next,
                Some(Slot::Occupied(_)) | None => {
                    unreachable!("arena free list must point to a vacant slot")
                }
            };
            self.slots[key] = Slot::Occupied(value);
        }

        ArenaKey(key)
    }

    pub fn get(&self, key: ArenaKey) -> Option<&T> {
        match self.slots.get(key.0) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant(_)) | None => None,
        }
    }

    pub fn get_mut(&mut self, key: ArenaKey) -> Option<&mut T> {
        match self.slots.get_mut(key.0) {
            Some(Slot::Occupied(value)) => Some(value),
            Some(Slot::Vacant(_)) | None => None,
        }
    }

    pub fn remove(&mut self, key: ArenaKey) -> T {
        let index = key.0;
        let slot = self
            .slots
            .get_mut(index)
            .expect("arena key must be in bounds");
        let value = match mem::replace(slot, Slot::Vacant(self.next_vacant)) {
            Slot::Occupied(value) => value,
            vacant @ Slot::Vacant(_) => {
                *slot = vacant;
                panic!("arena key must be occupied");
            }
        };
        self.len -= 1;
        self.next_vacant = index;
        value
    }

    /// Takes every occupied value while retaining the allocation for reuse.
    #[inline]
    pub fn take_all(&mut self) -> ArenaValues<T> {
        let len = self.len;
        let mut values = ArenaValues {
            first: None,
            rest: Vec::new(),
        };
        for slot in self.slots.drain(..) {
            if let Slot::Occupied(value) = slot {
                if values.first.is_none() {
                    values.first = Some(value);
                } else {
                    if values.rest.is_empty() {
                        values.rest.reserve(len - 1);
                    }
                    values.rest.push(value);
                }
            }
        }

        self.next_vacant = 0;
        self.len = 0;
        values
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
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
    fn take_all_restarts_key_allocation() {
        let mut arena = Arena::with_capacity(3);
        let first = arena.insert(1);
        let second = arena.insert(2);
        let third = arena.insert(3);
        let capacity = arena.slots.capacity();
        arena.remove(second);

        assert_eq!(arena.take_all().into_iter().collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(arena.len(), 0);
        assert_eq!(arena.slots.capacity(), capacity);

        let keys = [arena.insert(4), arena.insert(5), arena.insert(6)];
        assert_eq!(keys, [first, second, third]);
    }
}

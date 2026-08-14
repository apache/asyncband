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

use std::num::NonZeroUsize;

use crate::internal::Arena;
use crate::internal::ArenaKey;

/// A linked waiter queue whose detached nodes remain addressable until removal.
#[derive(Debug)]
pub struct WaitList<T> {
    head: Option<WaiterId>,
    tail: Option<WaiterId>,
    nodes: Arena<Node<T>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaiterId(NonZeroUsize);

impl WaiterId {
    fn new(key: ArenaKey) -> Self {
        Self(key.encode())
    }

    fn key(self) -> ArenaKey {
        ArenaKey::decode(self.0)
    }
}

#[derive(Debug)]
struct Node<T> {
    /// `None` marks a node that has been detached but not yet removed.
    links: Option<Links>,
    value: T,
}

#[derive(Clone, Copy, Debug)]
struct Links {
    prev: Option<WaiterId>,
    next: Option<WaiterId>,
}

impl<T> WaitList<T> {
    pub const fn new() -> Self {
        Self {
            head: None,
            tail: None,
            nodes: Arena::new(),
        }
    }

    /// Registers a waiter at the head of the list.
    pub fn push_front(&mut self, value: T) -> WaiterId {
        let next = self.head;
        let id = WaiterId::new(self.nodes.insert(Node {
            links: Some(Links { prev: None, next }),
            value,
        }));

        if let Some(next) = next {
            self.linked_node_mut(next).prev = Some(id);
        } else {
            self.tail = Some(id);
        }
        self.head = Some(id);
        id
    }

    /// Registers a waiter at the tail of the list.
    pub fn push_back(&mut self, value: T) -> WaiterId {
        let prev = self.tail;
        let id = WaiterId::new(self.nodes.insert(Node {
            links: Some(Links { prev, next: None }),
            value,
        }));

        if let Some(prev) = prev {
            self.linked_node_mut(prev).next = Some(id);
        } else {
            self.head = Some(id);
        }
        self.tail = Some(id);
        id
    }

    /// Detaches a waiter if the predicate returns `true`.
    ///
    /// The node and its value remain available until
    /// [`remove_unlinked_waiter`](Self::remove_unlinked_waiter) is called. If the waiter is already
    /// detached, the predicate still runs but the list links are unchanged.
    pub fn unlink_waiter(
        &mut self,
        id: WaiterId,
        should_unlink: impl FnOnce(&mut T) -> bool,
    ) -> Option<&mut T> {
        let links = {
            let node = self.node_mut(id);
            if !should_unlink(&mut node.value) {
                return None;
            }
            node.links.take()
        };

        if let Some(Links { prev, next }) = links {
            if let Some(prev) = prev {
                self.linked_node_mut(prev).next = next;
            } else {
                self.head = next;
            }

            if let Some(next) = next {
                self.linked_node_mut(next).prev = prev;
            } else {
                self.tail = prev;
            }
        }

        Some(&mut self.node_mut(id).value)
    }

    /// Detaches the first waiter if the predicate returns `true`.
    pub fn unlink_first_waiter(
        &mut self,
        should_unlink: impl FnOnce(&mut T) -> bool,
    ) -> Option<(WaiterId, &mut T)> {
        let first = self.head?;
        self.unlink_waiter(first, should_unlink)
            .map(|waiter| (first, waiter))
    }

    /// Returns `true` if no linked waiters remain.
    pub fn is_empty(&self) -> bool {
        debug_assert_eq!(self.head.is_none(), self.tail.is_none());
        self.head.is_none()
    }

    pub fn waiter_mut(&mut self, id: WaiterId) -> &mut T {
        &mut self.node_mut(id).value
    }

    pub fn remove_unlinked_waiter(&mut self, id: WaiterId) -> T {
        assert!(
            self.node(id).links.is_none(),
            "waiter must be unlinked before removal"
        );
        self.nodes.remove(id.key()).value
    }

    fn node(&self, id: WaiterId) -> &Node<T> {
        self.nodes
            .get(id.key())
            .expect("waiter id must refer to an occupied node")
    }

    fn node_mut(&mut self, id: WaiterId) -> &mut Node<T> {
        self.nodes
            .get_mut(id.key())
            .expect("waiter id must refer to an occupied node")
    }

    fn linked_node_mut(&mut self, id: WaiterId) -> &mut Links {
        self.node_mut(id)
            .links
            .as_mut()
            .expect("linked waiter must have links")
    }

    #[cfg(test)]
    pub fn occupied_len(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detached_nodes_remain_available_until_removed() {
        let mut waiters = WaitList::new();
        let first = waiters.push_back(1);
        let second = waiters.push_back(2);

        assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, first);
        assert_eq!(*waiters.waiter_mut(first), 1);
        assert_eq!(waiters.occupied_len(), 2);

        assert_eq!(waiters.remove_unlinked_waiter(first), 1);
        assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, second);
        assert!(waiters.is_empty());
        assert_eq!(waiters.remove_unlinked_waiter(second), 2);
        assert_eq!(waiters.occupied_len(), 0);
    }

    #[test]
    fn push_front_and_back_preserve_order() {
        let mut waiters = WaitList::new();
        let middle = waiters.push_back(2);
        let first = waiters.push_front(1);
        let last = waiters.push_back(3);

        for expected in [(first, 1), (middle, 2), (last, 3)] {
            let (id, value) = waiters.unlink_first_waiter(|_| true).unwrap();
            assert_eq!((id, *value), expected);
            waiters.remove_unlinked_waiter(id);
        }
        assert!(waiters.is_empty());
    }
}

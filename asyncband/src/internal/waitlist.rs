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

use slab::Slab;

/// A sentinel-based linked list with stable slab indices.
///
/// * `sentinel`'s `next` points to the first node (regular head).
/// * `sentinel`'s `prev` points to the last node (regular tail).
/// * Unlinked nodes remain addressable by index until they are explicitly removed.
#[derive(Debug)]
pub(crate) struct WaitList<T> {
    // If `None`, the list is uninitialized and empty.
    sentinel: Option<usize>,
    nodes: Slab<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    prev: usize,
    next: usize,
    value: Option<T>,
}

impl<T> WaitList<T> {
    /// Ensures the wait list is initialized, returning the sentinel index.
    fn ensure_init(&mut self) -> usize {
        if let Some(sentinel) = self.sentinel {
            return sentinel;
        }

        let first = self.nodes.vacant_entry();
        let sentinel = first.key();
        first.insert(Node {
            prev: sentinel,
            next: sentinel,
            value: None,
        });
        self.sentinel = Some(sentinel);
        sentinel
    }

    pub(crate) const fn new() -> Self {
        Self {
            sentinel: None,
            nodes: Slab::new(),
        }
    }

    /// Registers a waiter to the head of the wait list.
    ///
    /// # Panic
    ///
    /// Panics if `idx` is `Some`.
    pub(crate) fn register_waiter_to_head(
        &mut self,
        idx: &mut Option<usize>,
        f: impl FnOnce() -> Option<T>,
    ) {
        assert!(idx.is_none());

        let sentinel = self.ensure_init();
        let value = f();
        let prev_head = self.nodes[sentinel].next;
        let new_node = Node {
            prev: sentinel,
            next: prev_head,
            value,
        };
        let new_key = self.nodes.insert(new_node);
        self.nodes[sentinel].next = new_key;
        self.nodes[prev_head].prev = new_key;
        *idx = Some(new_key);
    }

    /// Registers a waiter to the tail of the wait list.
    ///
    /// # Panic
    ///
    /// Panics if `idx` is `Some`.
    pub(crate) fn register_waiter_to_tail(
        &mut self,
        idx: &mut Option<usize>,
        f: impl FnOnce() -> Option<T>,
    ) {
        assert!(idx.is_none());

        let sentinel = self.ensure_init();
        let value = f();
        let prev_tail = self.nodes[sentinel].prev;
        let new_node = Node {
            prev: prev_tail,
            next: sentinel,
            value,
        };
        let new_key = self.nodes.insert(new_node);
        self.nodes[sentinel].prev = new_key;
        self.nodes[prev_tail].next = new_key;
        *idx = Some(new_key);
    }

    /// Unlinks a previously registered waiter from the wait list if the predicate returns
    /// `true`.
    ///
    /// The slab entry remains available until
    /// [`remove_unlinked_waiter`](Self::remove_unlinked_waiter) is called.
    /// If the waiter is already unlinked, the predicate still runs but no links are changed.
    pub(crate) fn unlink_waiter(
        &mut self,
        idx: usize,
        should_unlink: impl FnOnce(&mut T) -> bool,
    ) -> Option<&mut T> {
        let sentinel = self.sentinel.expect("wait list must be initialized");

        assert_ne!(idx, sentinel);

        fn value_mut<T>(node: &mut Node<T>) -> &mut T {
            node.value
                .as_mut()
                .expect("waiter node must contain a value")
        }

        if should_unlink(value_mut(&mut self.nodes[idx])) {
            let prev = self.nodes[idx].prev;
            let next = self.nodes[idx].next;
            let is_unlinked = prev == idx;
            assert_eq!(is_unlinked, next == idx, "waiter links must be consistent");
            if !is_unlinked {
                self.nodes[prev].next = next;
                self.nodes[next].prev = prev;
                self.nodes[idx].prev = idx;
                self.nodes[idx].next = idx;
            }
            Some(value_mut(&mut self.nodes[idx]))
        } else {
            None
        }
    }

    /// Unlinks the first waiter from the wait list if the predicate returns `true`.
    pub(crate) fn unlink_first_waiter(
        &mut self,
        should_unlink: impl FnOnce(&mut T) -> bool,
    ) -> Option<&mut T> {
        let sentinel = self.sentinel?;
        let first = self.nodes[sentinel].next;
        if first != sentinel {
            self.unlink_waiter(first, should_unlink)
        } else {
            None
        }
    }

    /// Returns `true` if the wait list is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.sentinel
            .is_none_or(|sentinel| self.nodes[sentinel].next == sentinel)
    }

    pub(crate) fn waiter_mut(&mut self, idx: usize) -> &mut T {
        self.nodes[idx]
            .value
            .as_mut()
            .expect("waiter node must contain a value")
    }

    pub(crate) fn remove_unlinked_waiter(&mut self, idx: usize) {
        let node = &self.nodes[idx];
        assert_eq!(node.prev, idx, "waiter must be unlinked before removal");
        assert_eq!(node.next, idx, "waiter must be unlinked before removal");
        self.nodes.remove(idx);
    }
}

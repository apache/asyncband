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

use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;

/// A linked waiter queue whose detached nodes remain addressable until removal.
#[derive(Debug)]
pub struct WaitList<T> {
    head: Option<WaiterId>,
    tail: Option<WaiterId>,
    nodes: Nodes<T>,
}

/// Node storage that stays inline until two nodes must exist at once.
///
/// The inline node occupies the ID an empty arena issues first, so it moves into the arena under
/// the ID its owner already holds. The arena is then retained for reuse. Sharing storage between
/// the variants keeps an idle list close to the size of the arena alone.
#[derive(Debug)]
enum Nodes<T> {
    Inline(Option<Node<T>>),
    Arena(Arena<Node<T>>),
}

impl<T> Nodes<T> {
    #[inline]
    fn insert(&mut self, node: Node<T>) -> SlotId {
        match self {
            Self::Inline(slot @ None) => {
                *slot = Some(node);
                SlotId::FIRST
            }
            Self::Inline(slot) => {
                let first = slot.take().expect("occupied inline node must exist");
                self.move_to_arena(first, node)
            }
            Self::Arena(nodes) => nodes.insert(node),
        }
    }

    #[cold]
    fn move_to_arena(&mut self, first: Node<T>, node: Node<T>) -> SlotId {
        let mut nodes = Arena::new();
        let first_id = nodes.insert(first);
        debug_assert_eq!(first_id, SlotId::FIRST);
        let id = nodes.insert(node);
        *self = Self::Arena(nodes);
        id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaiterId(SlotId);

impl WaiterId {
    fn new(slot: SlotId) -> Self {
        Self(slot)
    }

    fn slot(self) -> SlotId {
        self.0
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
            nodes: Nodes::Inline(None),
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
        match &mut self.nodes {
            Nodes::Inline(node) => node
                .take()
                .expect("waiter id must refer to an occupied node"),
            Nodes::Arena(nodes) => nodes.remove(id.slot()),
        }
        .value
    }

    #[inline]
    fn node(&self, id: WaiterId) -> &Node<T> {
        match &self.nodes {
            Nodes::Inline(node) => {
                debug_assert_eq!(id.slot(), SlotId::FIRST);
                node.as_ref()
            }
            Nodes::Arena(nodes) => nodes.get(id.slot()),
        }
        .expect("waiter id must refer to an occupied node")
    }

    #[inline]
    fn node_mut(&mut self, id: WaiterId) -> &mut Node<T> {
        match &mut self.nodes {
            Nodes::Inline(node) => {
                debug_assert_eq!(id.slot(), SlotId::FIRST);
                node.as_mut()
            }
            Nodes::Arena(nodes) => nodes.get_mut(id.slot()),
        }
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
        match &self.nodes {
            Nodes::Inline(node) => usize::from(node.is_some()),
            Nodes::Arena(nodes) => nodes.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiter_id_preserves_the_option_niche() {
        assert_eq!(size_of::<WaiterId>(), size_of::<Option<WaiterId>>());
    }

    #[test]
    fn detached_nodes_remain_available_until_removed() {
        let mut waiters = WaitList::new();
        let first = waiters.push_back(1);
        let second = waiters.push_back(2);

        assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, first);
        assert_eq!(*waiters.waiter_mut(first), 1);
        assert_eq!(waiters.occupied_len(), 2);

        assert_eq!(
            *waiters
                .unlink_waiter(first, |value| {
                    *value += 10;
                    true
                })
                .unwrap(),
            11
        );

        assert_eq!(waiters.remove_unlinked_waiter(first), 11);
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

    #[test]
    fn unlinking_a_middle_waiter_preserves_the_queue() {
        let mut waiters = WaitList::new();
        let first = waiters.push_back(1);
        let middle = waiters.push_back(2);
        let last = waiters.push_back(3);

        assert!(waiters.unlink_waiter(middle, |_| false).is_none());
        assert_eq!(*waiters.unlink_waiter(middle, |_| true).unwrap(), 2);
        assert_eq!(waiters.remove_unlinked_waiter(middle), 2);

        let replacement = waiters.push_back(4);
        for expected in [(first, 1), (last, 3), (replacement, 4)] {
            let (id, value) = waiters.unlink_first_waiter(|_| true).unwrap();
            assert_eq!((id, *value), expected);
            waiters.remove_unlinked_waiter(id);
        }
        assert!(waiters.is_empty());
    }

    #[test]
    fn a_lone_waiter_stays_inline_across_reuse() {
        let mut waiters = WaitList::new();

        for value in 0..3 {
            let id = waiters.push_back(value);
            assert!(matches!(waiters.nodes, Nodes::Inline(Some(_))));
            assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, id);
            assert_eq!(waiters.remove_unlinked_waiter(id), value);
        }
        assert!(matches!(waiters.nodes, Nodes::Inline(None)));
    }

    #[test]
    fn a_second_waiter_moves_the_first_to_the_arena_under_its_id() {
        let mut waiters = WaitList::new();
        let first = waiters.push_back(1);
        let second = waiters.push_back(2);
        assert!(matches!(waiters.nodes, Nodes::Arena(_)));

        assert_eq!(*waiters.waiter_mut(first), 1);
        assert_eq!(*waiters.waiter_mut(second), 2);
        for expected in [(first, 1), (second, 2)] {
            let (id, value) = waiters.unlink_first_waiter(|_| true).unwrap();
            assert_eq!((id, *value), expected);
            waiters.remove_unlinked_waiter(id);
        }
        assert!(waiters.is_empty());
    }

    #[test]
    fn push_front_orders_a_new_head_before_the_inline_waiter() {
        let mut waiters = WaitList::new();
        let inline = waiters.push_back(2);
        let head = waiters.push_front(1);

        for expected in [(head, 1), (inline, 2)] {
            let (id, value) = waiters.unlink_first_waiter(|_| true).unwrap();
            assert_eq!((id, *value), expected);
            waiters.remove_unlinked_waiter(id);
        }
        assert!(waiters.is_empty());
    }

    #[test]
    fn a_detached_inline_waiter_stays_detached_when_moved() {
        let mut waiters = WaitList::new();
        let first = waiters.push_back(1);
        assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, first);
        assert!(waiters.is_empty());

        let second = waiters.push_back(2);
        assert_eq!(waiters.occupied_len(), 2);
        assert_eq!(*waiters.waiter_mut(first), 1);
        assert_eq!(waiters.remove_unlinked_waiter(first), 1);

        assert_eq!(waiters.unlink_first_waiter(|_| true).unwrap().0, second);
        assert!(waiters.is_empty());
        assert_eq!(waiters.remove_unlinked_waiter(second), 2);
        assert_eq!(waiters.occupied_len(), 0);
    }

    #[test]
    fn the_arena_is_retained_after_the_queue_drains() {
        let mut waiters = WaitList::new();
        waiters.push_back(1);
        waiters.push_back(2);
        while let Some((id, _)) = waiters.unlink_first_waiter(|_| true) {
            waiters.remove_unlinked_waiter(id);
        }
        assert!(matches!(waiters.nodes, Nodes::Arena(_)));

        let id = waiters.push_back(3);
        assert!(matches!(waiters.nodes, Nodes::Arena(_)));
        assert_eq!(*waiters.waiter_mut(id), 3);
    }
}

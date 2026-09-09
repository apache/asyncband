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

//! Capacity, position, and publication are separate ownership transitions:
//!
//! - A permit owns capacity, but holds no position until synchronous `push` claims a ticket.
//! - The ticket gives one producer a slot. `READY` publishes its initialized value to the receiver.
//! - The receiver finishes reading before returning capacity. AcqRel ticket increments carry that
//!   reuse ordering even to a producer that acquired its permit on an earlier lap.
//! - Close competes with publication on the slot state. The drain owns `READY` values; a producer
//!   that encounters `CLOSED` owns its unpublished value. Neither waits for the other to resume.
//!
//! Only the non-cloneable receiver advances the read cursor. All endpoints retain the shared
//! allocation, so a publisher's slot stays alive even when receiver drop closes it concurrently.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use crate::internal::cache_padded::CachePadded;

const EMPTY: u8 = 0;
const READY: u8 = 1;
const CLOSED: u8 = 2;

pub struct Buffer<T> {
    slots: Box<[Slot<T>]>,
    tail: CachePadded<AtomicUsize>,
    closed: AtomicBool,
}

impl<T> Buffer<T> {
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity.next_power_of_two())
            .map(|_| Slot {
                state: AtomicU8::new(EMPTY),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots,
            tail: CachePadded::new(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
        }
    }

    /// Writes and publishes one message. Closing may instead return the unsent value.
    ///
    /// # Safety
    ///
    /// Own one capacity permit before calling; release it only after a failed push or after
    /// the consumer reads the published value. No user code runs between claim and publication.
    pub unsafe fn push(&self, value: T) -> Result<(), T> {
        let Ok(position) = self.claim() else {
            return Err(value);
        };
        // SAFETY: The caller owns capacity and the ticket assigned this position.
        unsafe { self.publish(position, value) }
    }

    /// Pending means a producer claimed the head but has not published it yet.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may call this, using its persistent cursor. Release one
    /// capacity permit after each successful pop, after the value has been read completely.
    pub unsafe fn pop(&self, head: &mut usize) -> Poll<Option<T>> {
        let slot = self.slot(*head);
        if slot.state.load(Ordering::Acquire) == READY {
            // SAFETY: Publication initialized the value, and only this consumer can read it.
            // Capacity is still held until this method has returned the value to its caller.
            let value = unsafe { (*slot.value.get()).assume_init_read() };
            slot.state.store(EMPTY, Ordering::Release);
            *head = head.wrapping_add(1);
            Poll::Ready(Some(value))
        } else if self.tail.load(Ordering::Acquire) == *head {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// Stops new claims and returns ownership of published values to a drain guard.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may close the buffer, once, using its current cursor.
    pub unsafe fn close(&self, head: usize) -> Drain<'_, T> {
        self.closed.store(true, Ordering::Release);
        // Cover every physical slot: a producer may have passed the open check but not yet
        // claimed its ticket. Such a late claim must also find a CLOSED slot.
        Drain {
            buffer: self,
            position: head,
            remaining: self.slots.len(),
        }
    }

    fn slot(&self, position: usize) -> &Slot<T> {
        // Power-of-two storage preserves indexing when the full-width ticket wraps. The
        // semaphore still enforces the exact requested capacity, including non-powers of two.
        &self.slots[position & (self.slots.len() - 1)]
    }

    fn claim(&self) -> Result<usize, ()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(());
        }
        // Closing may race after this check. It marks every physical slot CLOSED, so even a
        // delayed claimant will recover its own value instead of publishing into a dead queue.
        Ok(self.tail.fetch_add(1, Ordering::AcqRel))
    }

    /// Writes and publishes one message into a claimed position.
    ///
    /// # Safety
    ///
    /// Own the position from a claim, backed by a capacity permit, and publish it at most once.
    unsafe fn publish(&self, position: usize, value: T) -> Result<(), T> {
        let slot = self.slot(position);
        // SAFETY: Capacity prevents wrapping over unread slots. AcqRel tail increments carry prior
        // claimants' capacity-acquire edges even when this producer held its permit for a long
        // time. The previous consumer has therefore finished reading before this write.
        unsafe { (*slot.value.get()).write(value) };
        match slot
            .state
            .compare_exchange(EMPTY, READY, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(state) => {
                debug_assert_eq!(state, CLOSED);
                // SAFETY: Close saw an unpublished slot and did not read it. Failed publication
                // leaves exclusive ownership with this producer, including during receiver drop.
                Err(unsafe { (*slot.value.get()).assume_init_read() })
            }
        }
    }
}

struct Slot<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: Capacity and the tail ticket give a producer exclusive ownership of an empty slot.
// Release publication transfers its value to the exclusive consumer. Closing an unpublished
// slot leaves its value with the producer; closing a READY slot transfers it to the drain.
unsafe impl<T: Send> Sync for Slot<T> {}

// No reference to a stored value escapes. Every value is removed from the slot's ownership
// before running a callback or destructor that might panic.
impl<T> std::panic::UnwindSafe for Slot<T> {}
impl<T> std::panic::RefUnwindSafe for Slot<T> {}

pub struct Drain<'a, T> {
    buffer: &'a Buffer<T>,
    position: usize,
    remaining: usize,
}

impl<T> Iterator for Drain<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        while self.remaining != 0 {
            let position = self.position;
            self.remaining -= 1;
            self.position = self.position.wrapping_add(1);
            let slot = self.buffer.slot(position);
            if slot.state.swap(CLOSED, Ordering::AcqRel) == READY {
                // SAFETY: The drain won ownership of a published value. The cursor and
                // state already advanced, so a panicking destructor cannot read twice.
                return Some(unsafe { (*slot.value.get()).assume_init_read() });
            }
            // An unpublished slot stays owned by its producer, which will observe CLOSED
            // and recover its value. The shared Arc keeps this allocation alive until then.
        }
        None
    }
}

impl<T> Drop for Drain<'_, T> {
    fn drop(&mut self) {
        struct Remaining<'a, 'b, T>(&'a mut Drain<'b, T>);

        impl<T> Drop for Remaining<'_, '_, T> {
            fn drop(&mut self) {
                for value in self.0.by_ref() {
                    drop(value);
                }
            }
        }

        // A guard inside Drop is necessary: Drop itself is not called again if a payload's
        // destructor panics while this normal drain is running.
        let remaining = Remaining(self);
        for value in remaining.0.by_ref() {
            drop(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::mem;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::thread;

    use crate::mpsc::BoundedSender;
    use crate::mpsc::Permit;
    use crate::mpsc::TryRecvError;
    use crate::mpsc::bounded;

    // Exercise the scheduling window inside synchronous send, while retaining a real capacity
    // permit. Ordinary callers cannot split a claim from its publication.
    fn publish_claimed<T>(
        tx: &BoundedSender<T>,
        permit: Permit<'_, T>,
        position: usize,
        value: T,
    ) -> Result<(), T> {
        // SAFETY: The test claimed this position while holding the same capacity permit.
        unsafe { tx.shared().buffer.publish(position, value) }?;
        // Publication owns the capacity now, so the permit must not release it on drop.
        mem::forget(permit);
        tx.shared().rx_waker.wake();
        Ok(())
    }

    #[test]
    fn a_claimed_head_waits_for_publication_across_laps() {
        for capacity in [1, 3, 7] {
            for initial in [0, usize::MAX - 1] {
                let (tx, mut rx) = bounded(capacity);
                // Start an empty ring near ticket overflow instead of running usize::MAX sends.
                tx.shared().buffer.tail.store(initial, Ordering::Relaxed);
                rx.set_head(initial);
                let mut cx = Context::from_waker(Waker::noop());
                for lap in 0..8 {
                    let permit = tx.try_reserve().unwrap();
                    let position = tx.shared().buffer.claim().unwrap();
                    for offset in 1..capacity {
                        tx.try_send(lap * capacity + offset).unwrap();
                    }
                    // A full ring must differ from an empty one even with no head value ready.
                    assert!(pin!(rx.recv()).poll(&mut cx).is_pending());
                    publish_claimed(&tx, permit, position, lap * capacity).unwrap();
                    for offset in 0..capacity {
                        assert_eq!(rx.try_recv(), Ok(lap * capacity + offset));
                    }
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                }
            }
        }
    }

    #[test]
    fn a_claim_delayed_past_close_returns_its_value() {
        let (tx, rx) = bounded(3);
        let permit = tx.try_reserve().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let value = Payload {
            bytes: [7; 1024],
            drops: drops.clone(),
            _sender: tx.clone(),
        };
        let allocation = Arc::downgrade(tx.shared());
        // Pause after claim's open check, then resume its atomic ticket allocation after close.
        assert!(!tx.shared().buffer.closed.load(Ordering::Acquire));
        drop(rx);
        let position = tx.shared().buffer.tail.fetch_add(1, Ordering::AcqRel);
        let unsent = publish_claimed(&tx, permit, position, value).unwrap_err();
        assert_eq!(unsent.bytes, [7; 1024]);
        drop(unsent);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(tx);
        assert!(allocation.upgrade().is_none());
    }

    #[derive(Debug)]
    #[repr(align(128))]
    struct Payload {
        bytes: [u8; 1024],
        drops: Arc<AtomicUsize>,
        // Queued messages must not keep the shared allocation alive through a sender cycle.
        _sender: BoundedSender<Payload>,
    }

    impl Drop for Payload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn closing_reclaims_ready_values_without_waiting_for_a_paused_publisher() {
        let (tx, rx) = bounded(2);
        let allocation = Arc::downgrade(tx.shared());
        let drops = Arc::new(AtomicUsize::new(0));
        let paused = Barrier::new(2);
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();

        thread::scope(|scope| {
            let sender = &tx;
            let drops = &drops;
            let paused = &paused;
            let publisher = scope.spawn(move || {
                let permit = sender.try_reserve().unwrap();
                let position = sender.shared().buffer.claim().unwrap();
                let value = Payload {
                    bytes: [1; 1024],
                    drops: drops.clone(),
                    _sender: sender.clone(),
                };
                paused.wait();
                resume_rx.recv().unwrap();
                let unsent = publish_claimed(sender, permit, position, value).unwrap_err();
                assert_eq!(unsent.bytes, [1; 1024]);
                drop(unsent);
            });
            paused.wait();
            tx.try_send(Payload {
                bytes: [2; 1024],
                drops: drops.clone(),
                _sender: tx.clone(),
            })
            .unwrap();
            let closer = scope.spawn(move || {
                drop(rx);
                closed_tx.send(()).unwrap();
            });
            #[cfg(not(miri))]
            let closed = closed_rx.recv_timeout(std::time::Duration::from_secs(10));
            #[cfg(miri)]
            let closed = closed_rx.recv();
            let dropped_before_resume = drops.load(Ordering::Relaxed);
            // Unblock the publisher before asserting so a failed close cannot strand the scope.
            resume_tx.send(()).unwrap();
            publisher.join().unwrap();
            closer.join().unwrap();
            assert!(closed.is_ok(), "close waited for the paused publisher");
            assert_eq!(dropped_before_resume, 1);
        });

        assert_eq!(drops.load(Ordering::Relaxed), 2);
        drop(tx);
        assert!(allocation.upgrade().is_none());
    }

    #[test]
    fn publication_racing_with_close_drops_every_payload_once() {
        for _ in 0..if cfg!(miri) { 8 } else { 128 } {
            let (tx, rx) = bounded(3);
            let allocation = Arc::downgrade(tx.shared());
            let drops = Arc::new(AtomicUsize::new(0));
            let start = Barrier::new(4);
            thread::scope(|scope| {
                for byte in 0..3 {
                    let permit = tx.try_reserve().unwrap();
                    let value = Payload {
                        bytes: [byte; 1024],
                        drops: drops.clone(),
                        _sender: tx.clone(),
                    };
                    let start = &start;
                    scope.spawn(move || {
                        start.wait();
                        if let Err(error) = permit.send(value) {
                            let value = error.into_inner();
                            assert_eq!(value.bytes, [byte; 1024]);
                            drop(value);
                        }
                    });
                }
                start.wait();
                drop(rx);
            });
            assert_eq!(drops.load(Ordering::Relaxed), 3);
            drop(tx);
            assert!(allocation.upgrade().is_none());
        }
    }

    #[test]
    fn an_old_permit_can_publish_after_other_producers_wrap_the_ring() {
        let (tx, mut rx) = bounded(3);
        let old = tx.try_reserve().unwrap();
        for lap in 0..16 {
            for offset in 0..2 {
                tx.try_send([lap * 2 + offset; 1024]).unwrap();
            }
            for offset in 0..2 {
                assert_eq!(rx.try_recv(), Ok([lap * 2 + offset; 1024]));
            }
        }
        thread::scope(|scope| {
            scope
                .spawn(move || old.send([42; 1024]).unwrap())
                .join()
                .unwrap();
        });
        assert_eq!(
            pin!(rx.recv()).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok([42; 1024]))
        );
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}

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
    unsafe { tx.shared().buffer.slots().publish(position, value) }?;
    // Publication owns the capacity now; forgetting skips the permit's release on drop.
    std::mem::forget(permit);
    tx.shared().rx_waker.wake();
    Ok(())
}

#[test]
fn a_claimed_head_waits_for_publication_across_laps() {
    for capacity in [1, 3, 7] {
        for initial in [0, usize::MAX - 1] {
            let (tx, mut rx) = bounded(capacity);
            // Start an empty ring near ticket overflow instead of running usize::MAX sends.
            tx.shared()
                .buffer
                .slots()
                .tail
                .store(initial, Ordering::Relaxed);
            rx.set_head(initial);
            let mut cx = Context::from_waker(Waker::noop());
            for lap in 0..8 {
                let permit = tx.try_reserve().unwrap();
                let position = tx.shared().buffer.slots().claim().unwrap();
                for offset in 1..capacity {
                    tx.try_send(lap * capacity + offset).unwrap();
                }
                // A full ring must differ from an empty one even if no head value is ready yet.
                assert!(rx.poll_recv(&mut cx).is_pending());
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
    assert!(!tx.shared().buffer.slots().closed.load(Ordering::Acquire));
    drop(rx);
    let position = tx
        .shared()
        .buffer
        .slots()
        .tail
        .fetch_add(1, Ordering::AcqRel);
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
            let position = sender.shared().buffer.slots().claim().unwrap();
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
        rx.poll_recv(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok([42; 1024]))
    );
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

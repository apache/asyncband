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

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use asyncband::mpsc;
use asyncband::mpsc::TryRecvError;
use tests_integration::poll_once;

use super::support::WakeCounter;
use super::support::assert_completes_without_deadlock;
use super::support::expect_ready;
use super::support::poll_with;
use super::support::waker_on_clone;
use super::support::waker_on_drop;

struct HoldSender<S> {
    _sender: S,
}

// This waker must own the sender so its final drop can break the tested reference cycle.
#[allow(clippy::manual_noop_waker)]
impl<S: Send + Sync> Wake for HoldSender<S> {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn bounded_receiver_drop_releases_registered_waker() {
    let (tx, mut rx) = mpsc::bounded::<()>(1);
    let holder = Arc::new(HoldSender { _sender: tx });
    let retained = Arc::downgrade(&holder);
    let waker = Waker::from(holder);
    assert!(
        Box::pin(rx.recv())
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(rx);
    assert!(retained.upgrade().is_none());
}

#[test]
fn unbounded_receiver_drop_releases_registered_waker() {
    let (tx, mut rx) = mpsc::unbounded::<()>();
    let holder = Arc::new(HoldSender { _sender: tx });
    let retained = Arc::downgrade(&holder);
    let waker = Waker::from(holder);
    assert!(
        Box::pin(rx.recv())
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(rx);
    assert!(retained.upgrade().is_none());
}

#[test]
fn bounded_send_rechecks_capacity_freed_by_waker_clone() {
    assert_completes_without_deadlock(|| {
        let (tx, rx) = mpsc::bounded(1);
        tx.try_send(1).unwrap();
        let receiver = Arc::new(Mutex::new(rx));
        let received = AtomicBool::new(false);
        let waker = waker_on_clone({
            let receiver = receiver.clone();
            move || {
                if !received.swap(true, Ordering::Relaxed) {
                    assert_eq!(receiver.lock().unwrap().try_recv(), Ok(1));
                }
            }
        });
        assert_eq!(
            poll_with(Box::pin(tx.send(2)).as_mut(), &waker),
            Poll::Ready(Ok(()))
        );
        assert_eq!(receiver.lock().unwrap().try_recv(), Ok(2));
    });
}

#[test]
fn receive_rechecks_messages_sent_by_waker_clone() {
    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::bounded(1);
        let waker = waker_on_clone(move || tx.try_send(7).unwrap());
        assert_eq!(
            poll_with(Box::pin(rx.recv()).as_mut(), &waker),
            Poll::Ready(Ok(7))
        );

        let (tx, mut rx) = mpsc::unbounded();
        let waker = waker_on_clone(move || tx.send(7).unwrap());
        assert_eq!(
            poll_with(Box::pin(rx.recv()).as_mut(), &waker),
            Poll::Ready(Ok(7))
        );
    });
}

#[test]
fn wake_callbacks_can_send_into_the_same_channel() {
    struct SendOnWake(Box<dyn Fn() + Send + Sync>);
    impl Wake for SendOnWake {
        fn wake(self: Arc<Self>) {
            (self.0)();
        }
    }

    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::bounded(2);
        let waker = Waker::from(Arc::new(SendOnWake(Box::new({
            let tx = tx.clone();
            move || tx.try_send(2).unwrap()
        }))));
        assert!(poll_with(Box::pin(rx.recv()).as_mut(), &waker).is_pending());
        tx.try_send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));

        let (tx, mut rx) = mpsc::unbounded();
        let waker = Waker::from(Arc::new(SendOnWake(Box::new({
            let tx = tx.clone();
            move || tx.send(2).unwrap()
        }))));
        assert!(poll_with(Box::pin(rx.recv()).as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
    });
}

#[test]
fn bounded_waiter_waker_replacement_and_cancellation_can_reenter() {
    assert_completes_without_deadlock(|| {
        for replace in [false, true] {
            let (tx, mut rx) = mpsc::bounded(1);
            tx.try_send(0).unwrap();
            let drops = Arc::new(AtomicUsize::new(0));
            let waker = waker_on_drop({
                let tx = tx.clone();
                let drops = drops.clone();
                move || {
                    assert_eq!(tx.try_send(9), Err(mpsc::TrySendError::Full(9)));
                    drops.fetch_add(1, Ordering::Relaxed);
                }
            });
            let mut send = Box::pin(tx.send(1));
            assert!(poll_with(send.as_mut(), &waker).is_pending());
            drop(waker);
            if replace {
                assert!(poll_once(send.as_mut()).is_pending());
                assert_eq!(drops.load(Ordering::Relaxed), 1);
            }
            drop(send);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
            assert_eq!(rx.try_recv(), Ok(0));
            tx.try_send(2).unwrap();
            assert_eq!(rx.try_recv(), Ok(2));
        }
    });
}

#[test]
fn bounded_receiver_waker_replacement_can_send() {
    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::bounded(1);
        let waker = waker_on_drop(move || tx.try_send(7).unwrap());
        assert!(poll_with(Box::pin(rx.recv()).as_mut(), &waker).is_pending());
        drop(waker);
        let (waker, wakes) = WakeCounter::new();
        let poll = poll_with(Box::pin(rx.recv()).as_mut(), &waker);
        if poll.is_pending() {
            assert!(wakes.count() > 0);
            assert_eq!(rx.try_recv(), Ok(7));
        } else {
            assert_eq!(poll, Poll::Ready(Ok(7)));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    });
}

#[test]
fn unbounded_replaced_and_disconnected_wakers_can_send() {
    assert_completes_without_deadlock(|| {
        for disconnected in [false, true] {
            let (tx, mut rx) = mpsc::unbounded();
            let drops = Arc::new(AtomicUsize::new(0));
            let waker = waker_on_drop({
                let drops = drops.clone();
                move || {
                    assert_eq!(tx.send(7).is_err(), disconnected);
                    drops.fetch_add(1, Ordering::Relaxed);
                }
            });
            assert!(
                Box::pin(rx.recv())
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            if disconnected {
                drop(rx);
            } else {
                // Replacing the waker can enqueue a message during this poll. Either immediate
                // completion or a notified Pending is valid, but the message must not be lost.
                let (waker, counter) = WakeCounter::new();
                let poll = poll_with(Box::pin(rx.recv()).as_mut(), &waker);
                if poll.is_pending() {
                    assert!(counter.count() > 0);
                    assert_eq!(rx.try_recv(), Ok(7));
                } else {
                    assert_eq!(poll, Poll::Ready(Ok(7)));
                }
                assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
            }
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        }
    });
}

#[test]
fn unbounded_disconnect_drops_partial_and_queued_batches_outside_lock() {
    struct Value(Option<Arc<dyn Fn() + Send + Sync>>);
    impl Drop for Value {
        fn drop(&mut self) {
            if let Some(callback) = &self.0 {
                callback();
            }
        }
    }

    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::unbounded();
        let drops = Arc::new(AtomicUsize::new(0));
        let callback: Arc<dyn Fn() + Send + Sync> = {
            let tx = tx.clone();
            let drops = drops.clone();
            Arc::new(move || {
                // This exercises both a live receiver and disconnection. The marker has no
                // callback, so destroying an unsuccessful send cannot recursively send again.
                let _ = tx.send(Value(None));
                drops.fetch_add(1, Ordering::Relaxed);
            })
        };
        for _ in 0..8192 {
            assert!(tx.send(Value(Some(callback.clone()))).is_ok());
        }
        for _ in 0..17 {
            drop(rx.try_recv().unwrap());
        }
        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 8192);
    });
}

#[cfg(panic = "unwind")]
#[test]
fn bounded_disconnect_finishes_cleanup_when_a_callback_panics() {
    struct Value {
        id: usize,
        drops: Arc<[AtomicUsize; 5]>,
        panic_on_drop: bool,
        _sender: Option<mpsc::BoundedSender<Value>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
            assert!(!self.panic_on_drop, "payload destructor panicked");
        }
    }
    struct Notify {
        woken: AtomicBool,
        panic_on_wake: bool,
    }
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Relaxed);
            assert!(!self.panic_on_wake, "wake callback panicked");
        }
    }

    for panic_on_wake in [false, true] {
        let (tx, rx) = mpsc::bounded(3);
        let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        for id in 0..3 {
            assert!(
                tx.try_send(Value {
                    id,
                    drops: drops.clone(),
                    panic_on_drop: id == 0 && !panic_on_wake,
                    _sender: Some(tx.clone()),
                })
                .is_ok()
            );
        }
        let notify = [false, true].map(|second| {
            Arc::new(Notify {
                woken: AtomicBool::new(false),
                panic_on_wake: !second && panic_on_wake,
            })
        });
        let wakers = notify.each_ref().map(|notify| Waker::from(notify.clone()));
        let mut sends = (3..5)
            .map(|id| {
                Box::pin(tx.send(Value {
                    id,
                    drops: drops.clone(),
                    panic_on_drop: false,
                    _sender: None,
                }))
            })
            .collect::<Vec<_>>();
        for (send, waker) in sends.iter_mut().zip(&wakers) {
            assert!(poll_with(send.as_mut(), waker).is_pending());
        }
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx))).is_err());
        assert!(
            notify
                .iter()
                .all(|notify| notify.woken.load(Ordering::Relaxed))
        );
        for count in &drops[..3] {
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
        for (id, send) in (3..5).zip(&mut sends) {
            assert_eq!(drops[id].load(Ordering::Relaxed), 0);
            let error = match expect_ready(poll_once(send.as_mut())) {
                Err(error) => error,
                Ok(()) => panic!("the receiver is disconnected"),
            };
            assert_eq!(error.into_inner().id, id);
            assert_eq!(drops[id].load(Ordering::Relaxed), 1);
        }
    }
}

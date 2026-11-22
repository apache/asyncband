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

use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::AtomicU8;
use std::task::Wake;
use std::thread;

use super::*;

#[test]
#[should_panic(expected = "watch channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    let (tx, _rx) = channel(0);
    tx.shared.slot.write().version = u64::MAX;
    tx.shared.version.store(u64::MAX);
    tx.send(1).unwrap();
}

#[test]
#[should_panic(expected = "watch channel version counter overflowed")]
fn send_replace_panics_on_version_overflow() {
    let (tx, _rx) = channel(0);
    tx.shared.slot.write().version = u64::MAX;
    tx.shared.version.store(u64::MAX);
    tx.send_replace(1);
}

#[test]
fn version_overflow_drops_input_outside_locks_and_preserves_state() {
    struct InspectOnDrop {
        id: usize,
        inspect: Option<Box<dyn FnOnce()>>,
    }

    impl Clone for InspectOnDrop {
        fn clone(&self) -> Self {
            Self {
                id: self.id,
                inspect: None,
            }
        }
    }

    impl Drop for InspectOnDrop {
        fn drop(&mut self) {
            if let Some(inspect) = self.inspect.take() {
                inspect();
            }
        }
    }

    let (tx, rx) = channel(InspectOnDrop {
        id: 7,
        inspect: None,
    });
    tx.shared.slot.write().version = u64::MAX;
    tx.shared.version.store(u64::MAX);

    for use_send_replace in [false, true] {
        let unlocked = Rc::new(Cell::new(None));
        let observed = unlocked.clone();
        let shared = tx.shared.clone();
        let payload = InspectOnDrop {
            id: 8,
            inspect: Some(Box::new(move || {
                let both_free =
                    shared.slot.try_write().is_some() && shared.waiters.try_lock().is_some();
                observed.set(Some(both_free));
            })),
        };

        let result = catch_unwind(AssertUnwindSafe(|| {
            if use_send_replace {
                let _ = tx.send_replace(payload);
            } else {
                let _ = tx.send(payload);
            }
        }));

        assert!(result.is_err());
        assert_eq!(unlocked.get(), Some(true));
        assert_eq!(rx.get().id, 7);
    }
}

#[test]
fn terminal_sender_drop_wakes_outside_locks() {
    struct ProbeWaker<T> {
        shared: Arc<Shared<T>>,
        state: AtomicU8,
    }

    impl<T: Send + Sync + 'static> Wake for ProbeWaker<T> {
        fn wake(self: Arc<Self>) {
            let unlocked = self.shared.closed.load(Ordering::Acquire)
                && self.shared.waiters.try_lock().is_some()
                && self.shared.slot.try_write().is_some();
            self.state
                .store(if unlocked { 2 } else { 1 }, Ordering::Release);
        }
    }

    let (tx, mut rx) = channel(0usize);
    let probe = Arc::new(ProbeWaker {
        shared: tx.shared.clone(),
        state: AtomicU8::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    let mut changed = pin!(rx.changed());

    assert!(changed.as_mut().poll(&mut cx).is_pending());
    drop(tx);

    assert_eq!(probe.state.load(Ordering::Acquire), 2);
    assert_eq!(
        changed.as_mut().poll(&mut cx),
        Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
fn concurrent_senders_commit_every_version() {
    const SENDERS: usize = 4;
    const SENDS_PER_THREAD: usize = if cfg!(miri) { 32 } else { 1_000 };

    let (tx, rx) = channel(0);
    let workers = (0..SENDERS)
        .map(|sender_index| {
            let tx = tx.clone();
            thread::spawn(move || {
                for offset in 0..SENDS_PER_THREAD {
                    tx.send(sender_index * SENDS_PER_THREAD + offset).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap();
    }

    let expected = (SENDERS * SENDS_PER_THREAD) as u64;
    assert_eq!(tx.shared.slot.read().version, expected);
    assert_eq!(tx.shared.version.load(), expected);
    assert!(rx.get() < SENDERS * SENDS_PER_THREAD);
}

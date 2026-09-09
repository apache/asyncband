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
use crate::mpsc::TryRecvError;
use crate::mpsc::bounded;

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
fn publication_racing_with_close_drops_every_payload_once() {
    for _ in 0..if cfg!(miri) { 8 } else { 128 } {
        let (tx, rx) = bounded(3);
        let allocation = Arc::downgrade(&tx.shared);
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
fn a_held_permit_remains_usable_after_other_senders_make_progress() {
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

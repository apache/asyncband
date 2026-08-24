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

//! Loom models for the RwLock scheduler.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Release;
use std::task::Poll;

use loom::future::block_on;
use loom::model::Builder;
use loom::sync::Arc;
use loom::sync::atomic::AtomicBool;
use loom::sync::atomic::AtomicUsize;
use loom::sync::mpsc;
use loom::thread;

use super::raw::RawRwLock;

const MAX_READERS: usize = 2;

/// Runs one model with CI-friendly exploration bounds.
fn check(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = Builder::new();
    builder.max_threads = 4;
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = 400;
    }
    if std::env::var_os("LOOM_MAX_PREEMPTIONS").is_none() {
        builder.preemption_bound = Some(3);
    }
    builder.check(model);
}

/// Confirms that no ownership or queued waiter remains.
fn assert_fully_unlocked(lock: &RawRwLock) {
    assert!(lock.try_write(), "lock ownership or a waiter was leaked");
    lock.unlock_write();
}

/// Polls an acquisition once and confirms that it queued.
fn poll_pending_once<F>(future: Pin<&mut F>)
where
    F: Future<Output = ()>,
{
    let mut future = future;
    block_on(poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }));
}

/// Checks that read and write ownership never overlap.
#[test]
fn loom_reader_writer_exclusion() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        let readers = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(AtomicBool::new(false));

        let reader_thread = {
            let lock = lock.clone();
            let readers = readers.clone();
            let writer = writer.clone();
            thread::spawn(move || {
                block_on(lock.read());
                assert!(!writer.load(Acquire));
                readers.fetch_add(1, AcqRel);
                thread::yield_now();
                assert!(!writer.load(Acquire));
                readers.fetch_sub(1, AcqRel);
                lock.unlock_read();
            })
        };

        let writer_thread = {
            let lock = lock.clone();
            let readers = readers.clone();
            let writer = writer.clone();
            thread::spawn(move || {
                block_on(lock.write());
                assert_eq!(readers.load(Acquire), 0);
                assert!(!writer.swap(true, AcqRel));
                thread::yield_now();
                assert_eq!(readers.load(Acquire), 0);
                writer.store(false, Release);
                lock.unlock_write();
            })
        };

        reader_thread.join().unwrap();
        writer_thread.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks writer publication racing the final reader release.
#[test]
fn loom_writer_publication_races_last_reader_release() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.read());

        let writer = {
            let lock = lock.clone();
            thread::spawn(move || {
                block_on(lock.write());
                lock.unlock_write();
            })
        };

        lock.unlock_read();
        writer.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks reader publication racing a writer release.
#[test]
fn loom_reader_publication_races_writer_release() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.write());

        let reader = {
            let lock = lock.clone();
            thread::spawn(move || {
                block_on(lock.read());
                lock.unlock_read();
            })
        };

        lock.unlock_write();
        reader.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks cancellation immediately before or after a writer grant.
#[test]
fn loom_writer_cancellation_races_grant() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.read());
        let (published, published_rx) = mpsc::channel();

        let cancellation = {
            let lock = lock.clone();
            thread::spawn(move || {
                let mut acquire = Box::pin(lock.write());
                poll_pending_once(acquire.as_mut());
                published.send(()).unwrap();
                thread::yield_now();
                drop(acquire);
            })
        };

        published_rx.recv().unwrap();
        lock.unlock_read();
        cancellation.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks cancellation immediately before or after an upgrade grant.
#[test]
fn loom_upgrade_cancellation_races_grant() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.upgradable_read());
        block_on(lock.read());
        let (published, published_rx) = mpsc::channel();

        let cancellation = {
            let lock = lock.clone();
            thread::spawn(move || {
                let mut upgrade = Box::pin(lock.upgrade());
                poll_pending_once(upgrade.as_mut());
                published.send(()).unwrap();
                thread::yield_now();
                drop(upgrade);
            })
        };

        published_rx.recv().unwrap();
        lock.unlock_read();
        cancellation.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks that cancelling the queue head wakes its follower.
#[test]
fn loom_queue_head_cancellation_wakes_follower() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.write());

        let (head_published, head_published_rx) = mpsc::channel();
        let (cancel_head, cancel_head_rx) = mpsc::channel();
        let head = {
            let lock = lock.clone();
            thread::spawn(move || {
                let mut acquire = Box::pin(lock.write());
                poll_pending_once(acquire.as_mut());
                head_published.send(()).unwrap();
                cancel_head_rx.recv().unwrap();
                drop(acquire);
            })
        };
        head_published_rx.recv().unwrap();

        let (follower_published, follower_published_rx) = mpsc::channel();
        let follower = {
            let lock = lock.clone();
            thread::spawn(move || {
                let mut acquire = Box::pin(lock.read());
                poll_pending_once(acquire.as_mut());
                follower_published.send(()).unwrap();
                block_on(acquire.as_mut());
                lock.unlock_read();
            })
        };
        follower_published_rx.recv().unwrap();

        cancel_head.send(()).unwrap();
        lock.unlock_write();
        head.join().unwrap();
        follower.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

/// Checks that an upgrade precedes a later queued writer.
#[test]
fn loom_upgrade_precedes_queued_writer() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        let acquisition_order = Arc::new(AtomicUsize::new(0));
        block_on(lock.upgradable_read());
        block_on(lock.read());

        let (writer_published, writer_published_rx) = mpsc::channel();
        let writer = {
            let lock = lock.clone();
            let acquisition_order = acquisition_order.clone();
            thread::spawn(move || {
                let mut acquire = Box::pin(lock.write());
                poll_pending_once(acquire.as_mut());
                writer_published.send(()).unwrap();
                block_on(acquire.as_mut());
                assert_eq!(
                    acquisition_order.compare_exchange(1, 2, AcqRel, Acquire),
                    Ok(1)
                );
                lock.unlock_write();
            })
        };
        writer_published_rx.recv().unwrap();

        let (upgrade_published, upgrade_published_rx) = mpsc::channel();
        let upgrade = {
            let lock = lock.clone();
            let acquisition_order = acquisition_order.clone();
            thread::spawn(move || {
                let mut acquire = Box::pin(lock.upgrade());
                poll_pending_once(acquire.as_mut());
                upgrade_published.send(()).unwrap();
                block_on(acquire.as_mut());
                assert_eq!(
                    acquisition_order.compare_exchange(0, 1, AcqRel, Acquire),
                    Ok(0)
                );
                lock.unlock_write();
            })
        };
        upgrade_published_rx.recv().unwrap();

        lock.unlock_read();
        upgrade.join().unwrap();
        writer.join().unwrap();
        assert_eq!(acquisition_order.load(Acquire), 2);
        assert_fully_unlocked(&lock);
    });
}

/// Checks a write-to-read downgrade racing reader publication.
#[test]
fn loom_downgrade_races_reader_publication() {
    check(|| {
        let lock = Arc::new(RawRwLock::new(MAX_READERS));
        block_on(lock.write());

        let reader = {
            let lock = lock.clone();
            thread::spawn(move || {
                block_on(lock.read());
                lock.unlock_read();
            })
        };

        lock.downgrade_write_to_read();
        assert!(!lock.try_write());
        lock.unlock_read();
        reader.join().unwrap();
        assert_fully_unlocked(&lock);
    });
}

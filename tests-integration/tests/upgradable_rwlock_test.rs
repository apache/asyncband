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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;
use tests_integration::waker_on_wake;
use tokio_test::assert_pending;
use tokio_test::assert_ready;
use tokio_test::task::spawn;

fn lock(limit: usize) -> UpgradableRwLock<usize> {
    UpgradableRwLock::with_max_readers(0, NonZeroUsize::new(limit).unwrap())
}

#[test]
fn upgrade_mutate_and_downgrade_without_unlocking() {
    let lock = lock(3);
    let upgradable = FutureExt::block_on(lock.upgradable_read());
    let first = lock.try_read().unwrap();
    let second = FutureExt::block_on(lock.read());
    assert!(lock.try_read().is_none());
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_write().is_none());
    drop((first, second));

    let mut write = FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable));
    *write = 42;
    assert!(lock.try_read().is_none());
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_write().is_none());
    let read = write.downgrade();
    assert_eq!(*read, 42);
    assert_eq!(*lock.try_read().unwrap(), 42);
    assert!(lock.try_write().is_none());
    drop(read);
    assert!(lock.try_write().is_some());
}

#[test]
fn reservation_is_unique_and_drop_wakes_the_next_upgradable_reader() {
    let lock = lock(2);
    let first = lock.try_upgradable_read().unwrap();
    let mut second = spawn(lock.upgradable_read());
    assert_pending!(second.poll());
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_read().is_some());
    drop(first);
    assert!(second.is_woken());
    // A granted reservation belongs to the queued future even before it is polled again.
    assert!(lock.try_upgradable_read().is_none());
    let second = assert_ready!(second.poll());
    assert!(lock.try_upgradable_read().is_none());
    drop(second);
    assert!(lock.try_write().is_some());
}

#[test]
fn writer_queued_before_upgradable_acquisition_goes_first() {
    let lock = lock(3);
    let read = lock.try_read().unwrap();
    let mut writer = spawn(lock.write());
    let mut upgradable = spawn(lock.upgradable_read());
    assert_pending!(writer.poll());
    assert_pending!(upgradable.poll());
    assert!(lock.try_read().is_none());
    drop(read);
    assert!(writer.is_woken());
    let mut write = assert_ready!(writer.poll());
    *write = 1;
    assert!(upgradable.is_woken());
    assert_pending!(upgradable.poll());
    drop(write);
    assert!(upgradable.is_woken());
    let upgradable = assert_ready!(upgradable.poll());
    assert_eq!(*upgradable, 1);
    assert_eq!(
        *FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable)),
        1
    );
}

#[test]
fn upgrade_passes_a_writer_queued_after_its_reservation() {
    let lock = lock(3);
    let upgradable = lock.try_upgradable_read().unwrap();
    let reader = lock.try_read().unwrap();
    let mut writer = spawn(lock.write());
    assert_pending!(writer.poll());
    // A writer waiting only for the reservation does not block ordinary readers.
    assert!(lock.try_read().is_some());
    let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert_pending!(upgrade.poll());
    let mut later_reader = spawn(lock.read());
    assert_pending!(later_reader.poll());
    assert!(lock.try_read().is_none());
    drop(reader);
    assert!(upgrade.is_woken());
    assert_pending!(writer.poll());
    let mut upgraded = assert_ready!(upgrade.poll());
    *upgraded = 1;
    assert_pending!(writer.poll());
    assert_pending!(later_reader.poll());
    drop(upgraded);
    // This reader queued for data permits before the writer obtained the reservation.
    let later_reader = assert_ready!(later_reader.poll());
    assert_eq!(*later_reader, 1);
    assert_pending!(writer.poll());
    drop(later_reader);
    let mut writer = assert_ready!(writer.poll());
    *writer = 2;
    drop(writer);
    assert_eq!(*lock.try_read().unwrap(), 2);
}

#[test]
fn writer_queued_after_pending_upgrade_waits_for_exclusive_guard() {
    let lock = lock(2);
    let upgradable = lock.try_upgradable_read().unwrap();
    let read = lock.try_read().unwrap();
    let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert_pending!(upgrade.poll());
    let mut writer = spawn(lock.write());
    assert_pending!(writer.poll());
    drop(read);
    let upgraded = assert_ready!(upgrade.poll());
    assert_pending!(writer.poll());
    drop(upgraded);
    assert!(writer.is_woken());
    drop(assert_ready!(writer.poll()));
}

#[test]
fn readers_queued_before_upgrade_finish_first() {
    let lock = lock(2);
    let upgradable = lock.try_upgradable_read().unwrap();
    let held = lock.try_read().unwrap();
    let mut earlier = spawn(lock.read());
    assert_pending!(earlier.poll());
    let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert_pending!(upgrade.poll());
    drop(held);
    assert!(earlier.is_woken());
    assert_pending!(upgrade.poll());
    let earlier = assert_ready!(earlier.poll());
    drop(earlier);
    assert!(upgrade.is_woken());
    drop(assert_ready!(upgrade.poll()));
    assert!(lock.try_write().is_some());
}

#[test]
fn cancel_upgrade_before_poll_while_partial_and_after_grant() {
    for stage in 0..3 {
        let lock = lock(4);
        let upgradable = lock.try_upgradable_read().unwrap();
        let mut reader = Some(lock.try_read().unwrap());
        let mut writer = spawn(lock.write());
        assert_pending!(writer.poll());
        let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
        if stage > 0 {
            // Two of the three additional permits are assigned before returning Pending.
            assert_pending!(upgrade.poll());
        }
        if stage == 2 {
            drop(reader.take());
            assert!(upgrade.is_woken());
        }
        drop(upgrade);
        assert!(writer.is_woken());
        if stage < 2 {
            assert_pending!(writer.poll());
        }
        drop(reader);
        drop(assert_ready!(writer.poll()));
        let upgradable = lock.try_upgradable_read().unwrap();
        let readers = (0..3).map(|_| lock.try_read().unwrap()).collect::<Vec<_>>();
        assert!(lock.try_read().is_none());
        drop((readers, upgradable));
        assert!(lock.try_write().is_some());
    }
}

#[test]
fn cancelling_upgrade_wakes_queued_reader_and_next_reservation() {
    let lock = lock(3);
    let upgradable = lock.try_upgradable_read().unwrap();
    let held = lock.try_read().unwrap();
    let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert_pending!(upgrade.poll());
    let mut reader = spawn(lock.read());
    let mut next = spawn(lock.upgradable_read());
    assert_pending!(reader.poll());
    assert_pending!(next.poll());
    drop(upgrade);
    assert!(reader.is_woken());
    assert!(next.is_woken());
    let reader = assert_ready!(reader.poll());
    let next = assert_ready!(next.poll());
    assert!(lock.try_read().is_none());
    drop((held, reader, next));
    assert!(lock.try_write().is_some());
}

#[test]
fn cancellation_at_either_acquisition_queue_returns_all_resources() {
    for upgradable_request in [false, true] {
        for stage in 0..3 {
            let lock = lock(2);
            let held = lock.try_upgradable_read().unwrap();
            let mut request = Box::pin(async {
                if upgradable_request {
                    drop(lock.upgradable_read().await);
                } else {
                    drop(lock.write().await);
                }
            });
            assert_pending!(poll_once(request.as_mut()));
            // Release the reservation but retain one data permit.
            let read = held.downgrade();
            if stage > 0 {
                // Fill the other slot so even an upgradable acquisition has to wait for data.
                let other = lock.try_read().unwrap();
                assert_pending!(poll_once(request.as_mut()));
                if stage == 2 {
                    drop((read, other));
                } else {
                    drop(request);
                    drop((read, other));
                    assert!(lock.try_write().is_some());
                    continue;
                }
            } else {
                drop(read);
            }
            // Drop while the reservation or data permits have been granted, before repolling.
            drop(request);
            assert!(lock.try_write().is_some());
            assert!(lock.try_upgradable_read().is_some());
        }
    }
}

#[test]
fn dropping_and_downgrading_upgradable_guards_wake_writers() {
    for downgrade in [false, true] {
        let lock = lock(2);
        let upgradable = lock.try_upgradable_read().unwrap();
        let mut writer = spawn(lock.write());
        assert_pending!(writer.poll());
        if downgrade {
            let read = upgradable.downgrade();
            assert!(writer.is_woken());
            assert_pending!(writer.poll());
            assert!(lock.try_read().is_none());
            assert_eq!(*read, 0);
            drop(read);
        } else {
            drop(upgradable);
        }
        assert!(writer.is_woken());
        drop(assert_ready!(writer.poll()));
        assert!(lock.try_upgradable_read().is_some());
    }
}

#[test]
fn try_upgrade_failure_preserves_guard_and_reservation() {
    let lock = lock(3);
    let upgradable = lock.try_upgradable_read().unwrap();
    let read = lock.try_read().unwrap();
    let mut writer = spawn(lock.write());
    assert_pending!(writer.poll());
    let upgradable = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap_err();
    assert_eq!(*upgradable, 0);
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_read().is_some());
    assert_pending!(writer.poll());
    drop(read);
    let write = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap();
    assert_pending!(writer.poll());
    drop(write);
    drop(assert_ready!(writer.poll()));
}

#[test]
fn reader_limits_and_repeated_cycles_include_one_and_usize_max() {
    for limit in [1, 2, 5, usize::MAX] {
        let lock = lock(limit);
        for value in 1..=20 {
            let upgradable = FutureExt::block_on(lock.upgradable_read());
            let mut write = if value % 2 == 0 {
                RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap()
            } else {
                FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable))
            };
            assert_eq!(*write, value - 1);
            *write = value;
            let read = write.downgrade();
            assert!(lock.try_write().is_none());
            if limit <= 5 {
                let others = (1..limit)
                    .map(|_| lock.try_read().unwrap())
                    .collect::<Vec<_>>();
                assert!(lock.try_read().is_none());
                assert!(lock.try_upgradable_read().is_none());
                drop(others);
            }
            drop(read);
            let upgradable = lock.try_upgradable_read().unwrap();
            assert_eq!(*upgradable.downgrade(), value);
        }
    }
}

#[test]
fn one_reader_upgrade_does_not_wait_behind_queued_readers() {
    for try_upgrade in [false, true] {
        let lock = lock(1);
        let upgradable = lock.try_upgradable_read().unwrap();
        let mut reader = spawn(lock.read());
        let mut writer = spawn(lock.write());
        assert_pending!(reader.poll());
        assert_pending!(writer.poll());
        let mut write = if try_upgrade {
            RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap()
        } else {
            FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable))
        };
        *write = 7;
        assert_pending!(reader.poll());
        let retained = write.downgrade();
        assert_pending!(reader.poll());
        assert_pending!(writer.poll());
        drop(retained);
        let read = assert_ready!(reader.poll());
        assert_eq!(*read, 7);
        drop(read);
        drop(assert_ready!(writer.poll()));
    }
}

#[test]
fn reservation_requests_keep_fifo_order_without_upgradable_barging() {
    let lock = lock(2);
    let held = lock.try_upgradable_read().unwrap();
    let mut first = spawn(lock.write());
    let mut second = spawn(lock.upgradable_read());
    let mut third = spawn(lock.write());
    assert_pending!(first.poll());
    assert_pending!(second.poll());
    assert_pending!(third.poll());
    drop(held);
    assert!(lock.try_write().is_none());
    assert!(lock.try_upgradable_read().is_none());
    assert_pending!(third.poll());
    let first = assert_ready!(first.poll());
    assert_pending!(second.poll());
    drop(first);
    let second = assert_ready!(second.poll());
    assert_pending!(third.poll());
    drop(second);
    drop(assert_ready!(third.poll()));
}

#[test]
fn ordinary_writer_blocks_later_readers_once_waiting_for_data() {
    let lock = lock(3);
    let held = lock.try_read().unwrap();
    let mut writer = spawn(lock.write());
    let mut later = spawn(lock.read());
    assert_pending!(writer.poll());
    assert_pending!(later.poll());
    assert!(lock.try_read().is_none());
    drop(held);
    let writer = assert_ready!(writer.poll());
    assert_pending!(later.poll());
    drop(writer);
    drop(assert_ready!(later.poll()));
}

#[test]
fn failed_try_acquisitions_release_the_reservation() {
    let lock = lock(1);
    let held = lock.try_read().unwrap();
    assert!(lock.try_write().is_none());
    assert!(lock.try_upgradable_read().is_none());
    drop(held);
    drop(lock.try_upgradable_read().unwrap());
    let write = lock.try_write().unwrap();
    assert!(lock.try_upgradable_read().is_none());
    drop(write);
    assert!(lock.try_upgradable_read().is_some());
}

#[test]
fn cross_thread_guard_transfer() {
    let lock = UpgradableRwLock::new((0, 0));
    let upgradable = lock.try_upgradable_read().unwrap();
    thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut write = FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable));
                *write = (1, 1);
            })
            .join()
            .unwrap();
        let upgradable = lock.try_upgradable_read().unwrap();
        assert_eq!(*upgradable, (1, 1));
        let read = upgradable.downgrade();
        let mut writer = spawn(lock.write());
        assert_pending!(writer.poll());
        scope.spawn(move || drop(read)).join().unwrap();
        let mut writer = assert_ready!(writer.poll());
        *writer = (2, 2);
    });
    assert_eq!(*lock.try_read().unwrap(), (2, 2));
}

#[test]
fn concurrent_readers_writers_and_upgrades_preserve_values() {
    let lock = Arc::new(UpgradableRwLock::new((0, 0)));
    let rounds = if cfg!(miri) { 3 } else { 100 };
    let start = Barrier::new(6);
    thread::scope(|scope| {
        for role in 0..6 {
            let lock = lock.clone();
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..rounds {
                    match role % 3 {
                        0 => {
                            let read = FutureExt::block_on(lock.read());
                            assert_eq!(read.0, read.1);
                            thread::yield_now();
                        }
                        1 => {
                            let mut write = FutureExt::block_on(lock.write());
                            write.0 += 1;
                            thread::yield_now();
                            write.1 += 1;
                        }
                        _ => {
                            let read = FutureExt::block_on(lock.upgradable_read());
                            assert_eq!(read.0, read.1);
                            thread::yield_now();
                            let mut write =
                                FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(read));
                            write.0 += 1;
                            thread::yield_now();
                            write.1 += 1;
                            let read = write.downgrade();
                            assert_eq!(read.0, read.1);
                        }
                    }
                }
            });
        }
    });
    assert_eq!(*lock.try_read().unwrap(), (4 * rounds, 4 * rounds));
}

#[test]
fn constructors_unsized_values_and_exclusive_borrows() {
    let mut lock = const { UpgradableRwLock::new(1) };
    *lock.get_mut() = 2;
    assert_eq!(lock.into_inner(), 2);
    assert_eq!(UpgradableRwLock::<usize>::default().into_inner(), 0);
    assert_eq!(UpgradableRwLock::from(3).into_inner(), 3);
    let lock: Box<UpgradableRwLock<[usize]>> = Box::new(UpgradableRwLock::new([0, 1]));
    let mut write =
        RwLockUpgradableReadGuard::try_upgrade(lock.try_upgradable_read().unwrap()).unwrap();
    write[0] = 2;
    assert_eq!(&*write.downgrade(), &[2, 1]);
}

#[test]
fn cancelled_reservation_waiter_does_not_block_its_successor() {
    let lock = lock(2);
    let held = lock.try_upgradable_read().unwrap();
    let mut cancelled = spawn(lock.write());
    let mut successor = spawn(lock.upgradable_read());
    assert_pending!(cancelled.poll());
    assert_pending!(successor.poll());
    drop(cancelled);
    assert_pending!(successor.poll());
    drop(held);
    assert!(successor.is_woken());
    drop(assert_ready!(successor.poll()));
    assert!(lock.try_write().is_some());
}

#[test]
fn cancelling_an_upgrade_races_with_the_last_readers_release() {
    let rounds = if cfg!(miri) { 3 } else { 100 };
    for _ in 0..rounds {
        let lock = lock(3);
        let upgradable = lock.try_upgradable_read().unwrap();
        let reader = lock.try_read().unwrap();
        let mut upgrade = spawn(RwLockUpgradableReadGuard::upgrade(upgradable));
        let mut writer = spawn(lock.write());
        assert_pending!(upgrade.poll());
        assert_pending!(writer.poll());
        let start = Barrier::new(2);
        thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                drop(reader);
            });
            start.wait();
            drop(upgrade);
        });
        assert!(writer.is_woken());
        drop(assert_ready!(writer.poll()));
        let readers = (0..3).map(|_| lock.try_read().unwrap()).collect::<Vec<_>>();
        assert!(lock.try_read().is_none());
        drop(readers);
        assert!(lock.try_upgradable_read().is_some());
    }
}

#[test]
fn upgrade_observes_reader_writes_without_a_thread_join() {
    assert_completes_without_deadlock(|| {
        for queued in [false, true] {
            let lock = UpgradableRwLock::with_max_readers(
                AtomicUsize::new(0),
                NonZeroUsize::new(2).unwrap(),
            );
            let upgradable = lock.try_upgradable_read().unwrap();
            let reader = lock.try_read().unwrap();
            let released = AtomicBool::new(false);
            let mut upgrade = Box::pin(RwLockUpgradableReadGuard::upgrade(upgradable));
            if queued {
                assert_pending!(poll_once(upgrade.as_mut()));
            }
            thread::scope(|scope| {
                scope.spawn(|| {
                    reader.store(42, Ordering::Relaxed);
                    drop(reader);
                    released.store(true, Ordering::Relaxed);
                });
                // The flag only controls the schedule. Relaxed ordering and the no-op waker
                // cannot publish the reader's write; the lock must provide that synchronization.
                while !released.load(Ordering::Relaxed) {
                    thread::yield_now();
                }
                let mut write = assert_ready!(poll_once(upgrade.as_mut()));
                // A non-atomic read lets Miri verify that the upgrade acquired the reader's
                // release, both when permits were immediately available and when queued.
                assert_eq!(*write.get_mut(), 42);
            });
        }
    });
}

#[test]
fn downgraded_writer_publishes_to_readers_without_a_thread_join() {
    assert_completes_without_deadlock(|| {
        for upgradable_reader in [false, true] {
            let lock = UpgradableRwLock::new(0);
            let mut write =
                RwLockUpgradableReadGuard::try_upgrade(lock.try_upgradable_read().unwrap())
                    .unwrap();
            thread::scope(|scope| {
                let holder = scope.spawn(move || {
                    *write = 42;
                    write.downgrade()
                });
                // A try-acquisition may observe the old unavailable state. Retry until it
                // succeeds, without using another synchronization primitive to publish the value.
                let value = loop {
                    let value = if upgradable_reader {
                        lock.try_upgradable_read().map(|read| *read)
                    } else {
                        lock.try_read().map(|read| *read)
                    };
                    if let Some(value) = value {
                        break value;
                    }
                    thread::yield_now();
                };
                assert_eq!(value, 42);
                // The thread's result owns the downgraded guard until this join, so dropping
                // that guard cannot supply the synchronization missing from a downgrade.
                drop(holder.join().unwrap());
            });
        }
    });
}

#[test]
fn upgrade_waker_replacement_and_grant_allow_reentry() {
    assert_completes_without_deadlock(|| {
        let lock = Arc::new(lock(2));
        let callbacks = Arc::new(AtomicUsize::new(0));
        let reenter = || {
            let lock = lock.clone();
            let callbacks = callbacks.clone();
            move || {
                // All permits are assigned to the upgrader and the existing reader, or to the
                // granted upgrade. Registering and cancelling a reader reenters the data queue.
                let mut read = Box::pin(lock.read());
                assert_pending!(poll_once(read.as_mut()));
                drop(read);
                callbacks.fetch_add(1, Ordering::Relaxed);
            }
        };
        let upgradable = lock.try_upgradable_read().unwrap();
        let reader = lock.try_read().unwrap();
        let mut upgrade = Box::pin(RwLockUpgradableReadGuard::upgrade(upgradable));
        let old_waker = waker_on_drop(reenter());
        assert_pending!(poll_with(upgrade.as_mut(), &old_waker));
        drop(old_waker);
        let new_waker = waker_on_wake(reenter());
        assert_pending!(poll_with(upgrade.as_mut(), &new_waker));
        assert_eq!(callbacks.load(Ordering::Relaxed), 1);
        drop(reader);
        assert_eq!(callbacks.load(Ordering::Relaxed), 2);
        // Cancellation after the wake must also reclaim every assigned permit.
        drop(upgrade);
        assert!(lock.try_write().is_some());
    });
}

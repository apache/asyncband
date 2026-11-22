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

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::rwlock::MappedRwLockReadGuard;
use asyncband::rwlock::RwLockReadGuard;
use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use tests_integration::TestTask;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;
use tests_integration::waker_on_wake;

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
#[allow(
    clippy::replace_box,
    reason = "Exercise allocation replacement through shared readers."
)]
fn upgraded_writer_can_downgrade_then_map_shared_interior_mutability() {
    for limit in [2, 3, usize::MAX] {
        for mode in 0..3 {
            let lock = UpgradableRwLock::with_max_readers(
                Some(RefCell::new(Box::new(0))),
                NonZeroUsize::new(limit).unwrap(),
            );
            let upgradable = lock.try_upgradable_read().unwrap();
            let mut write = match mode {
                0 => RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap(),
                1 => FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable)),
                _ => {
                    let reader = lock.try_read().unwrap();
                    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
                    assert!(upgrade.poll().is_pending());
                    *reader.as_ref().unwrap().borrow_mut() = Box::new(7);
                    drop(reader);
                    let write = expect_ready(upgrade.poll());
                    assert_eq!(**write.as_ref().unwrap().borrow(), 7);
                    write
                }
            };
            *write.as_mut().unwrap().get_mut() = Box::new(1);
            // Project shared access only after downgrading the original write guard.
            let read = RwLockReadGuard::map(write.downgrade(), |value| value);
            let read = MappedRwLockReadGuard::filter_map(read, Option::as_ref).unwrap();
            let shared = &*read;
            let other = lock.try_read().unwrap();
            *other.as_ref().unwrap().borrow_mut() = Box::new(2);
            assert_eq!(**shared.borrow(), 2);
            drop(other);

            let next = lock.try_upgradable_read().unwrap();
            let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(next));
            assert!(upgrade.poll().is_pending());
            assert_eq!(**shared.borrow(), 2);
            drop(read);
            let mut write = expect_ready(upgrade.poll());
            *write.as_mut().unwrap().get_mut() = Box::new(3);
            drop(write);
            assert_eq!(**lock.try_read().unwrap().as_ref().unwrap().borrow(), 3);
        }
    }
}

#[test]
#[allow(
    clippy::replace_box,
    reason = "Exercise allocation replacement through shared readers."
)]
fn cancelling_upgrade_preserves_a_mapped_readers_reference_and_permit() {
    let lock = UpgradableRwLock::with_max_readers(
        Some(RefCell::new(Box::new(0))),
        NonZeroUsize::new(3).unwrap(),
    );
    let write =
        RwLockUpgradableReadGuard::try_upgrade(lock.try_upgradable_read().unwrap()).unwrap();
    let read = RwLockReadGuard::filter_map(write.downgrade(), Option::as_ref).unwrap();
    let shared = &*read;
    let next = lock.try_upgradable_read().unwrap();
    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(next));
    // The upgrade acquires one additional permit while the mapped reader retains the last.
    assert!(upgrade.poll().is_pending());
    drop(upgrade);
    let next = lock.try_upgradable_read().unwrap();
    *next.as_ref().unwrap().borrow_mut() = Box::new(1);
    assert_eq!(**shared.borrow(), 1);
    drop(next);
    assert!(lock.try_write().is_none());
    drop(read);
    assert!(lock.try_write().is_some());
}

#[test]
fn reservation_is_unique_and_drop_wakes_the_next_upgradable_reader() {
    let lock = lock(2);
    let first = lock.try_upgradable_read().unwrap();
    let mut second = TestTask::new(lock.upgradable_read());
    assert!(second.poll().is_pending());
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_read().is_some());
    drop(first);
    assert!(second.is_woken());
    // A granted reservation belongs to the queued future even before it is polled again.
    assert!(lock.try_upgradable_read().is_none());
    let second = expect_ready(second.poll());
    assert!(lock.try_upgradable_read().is_none());
    drop(second);
    assert!(lock.try_write().is_some());
}

#[test]
fn writer_queued_before_upgradable_acquisition_goes_first() {
    let lock = lock(3);
    let read = lock.try_read().unwrap();
    let mut writer = TestTask::new(lock.write());
    let mut upgradable = TestTask::new(lock.upgradable_read());
    assert!(writer.poll().is_pending());
    assert!(upgradable.poll().is_pending());
    assert!(lock.try_read().is_none());
    drop(read);
    assert!(writer.is_woken());
    let mut write = expect_ready(writer.poll());
    *write = 1;
    assert!(upgradable.is_woken());
    assert!(upgradable.poll().is_pending());
    drop(write);
    assert!(upgradable.is_woken());
    let upgradable = expect_ready(upgradable.poll());
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
    let mut writer = TestTask::new(lock.write());
    assert!(writer.poll().is_pending());
    // A writer waiting only for the reservation does not block ordinary readers.
    assert!(lock.try_read().is_some());
    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert!(upgrade.poll().is_pending());
    let mut later_reader = TestTask::new(lock.read());
    assert!(later_reader.poll().is_pending());
    assert!(lock.try_read().is_none());
    drop(reader);
    assert!(upgrade.is_woken());
    assert!(writer.poll().is_pending());
    let mut upgraded = expect_ready(upgrade.poll());
    *upgraded = 1;
    assert!(writer.poll().is_pending());
    assert!(later_reader.poll().is_pending());
    drop(upgraded);
    // This reader queued for data permits before the writer obtained the reservation.
    let later_reader = expect_ready(later_reader.poll());
    assert_eq!(*later_reader, 1);
    assert!(writer.poll().is_pending());
    drop(later_reader);
    let mut writer = expect_ready(writer.poll());
    *writer = 2;
    drop(writer);
    assert_eq!(*lock.try_read().unwrap(), 2);
}

#[test]
fn writer_queued_after_pending_upgrade_waits_for_exclusive_guard() {
    let lock = lock(2);
    let upgradable = lock.try_upgradable_read().unwrap();
    let read = lock.try_read().unwrap();
    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert!(upgrade.poll().is_pending());
    let mut writer = TestTask::new(lock.write());
    assert!(writer.poll().is_pending());
    drop(read);
    let upgraded = expect_ready(upgrade.poll());
    assert!(writer.poll().is_pending());
    drop(upgraded);
    assert!(writer.is_woken());
    drop(expect_ready(writer.poll()));
}

#[test]
fn readers_queued_before_upgrade_finish_first() {
    let lock = lock(2);
    let upgradable = lock.try_upgradable_read().unwrap();
    let held = lock.try_read().unwrap();
    let mut earlier = TestTask::new(lock.read());
    assert!(earlier.poll().is_pending());
    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert!(upgrade.poll().is_pending());
    drop(held);
    assert!(earlier.is_woken());
    assert!(upgrade.poll().is_pending());
    let earlier = expect_ready(earlier.poll());
    drop(earlier);
    assert!(upgrade.is_woken());
    drop(expect_ready(upgrade.poll()));
    assert!(lock.try_write().is_some());
}

#[test]
fn cancel_upgrade_before_poll_while_partial_and_after_grant() {
    for stage in 0..3 {
        let lock = lock(4);
        let upgradable = lock.try_upgradable_read().unwrap();
        let mut reader = Some(lock.try_read().unwrap());
        let mut writer = TestTask::new(lock.write());
        assert!(writer.poll().is_pending());
        let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
        if stage > 0 {
            // Two of the three additional permits are assigned before returning Pending.
            assert!(upgrade.poll().is_pending());
        }
        if stage == 2 {
            drop(reader.take());
            assert!(upgrade.is_woken());
        }
        drop(upgrade);
        assert!(writer.is_woken());
        if stage < 2 {
            assert!(writer.poll().is_pending());
        }
        drop(reader);
        drop(expect_ready(writer.poll()));
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
    let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
    assert!(upgrade.poll().is_pending());
    let mut reader = TestTask::new(lock.read());
    let mut next = TestTask::new(lock.upgradable_read());
    assert!(reader.poll().is_pending());
    assert!(next.poll().is_pending());
    drop(upgrade);
    assert!(reader.is_woken());
    assert!(next.is_woken());
    let reader = expect_ready(reader.poll());
    let next = expect_ready(next.poll());
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
            assert!(poll_once(request.as_mut()).is_pending());
            // Release the reservation but retain one data permit.
            let read = held.downgrade();
            if stage > 0 {
                // Fill the other slot so even an upgradable acquisition has to wait for data.
                let other = lock.try_read().unwrap();
                assert!(poll_once(request.as_mut()).is_pending());
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
        let mut writer = TestTask::new(lock.write());
        assert!(writer.poll().is_pending());
        if downgrade {
            let read = upgradable.downgrade();
            assert!(writer.is_woken());
            assert!(writer.poll().is_pending());
            assert!(lock.try_read().is_none());
            assert_eq!(*read, 0);
            drop(read);
        } else {
            drop(upgradable);
        }
        assert!(writer.is_woken());
        drop(expect_ready(writer.poll()));
        assert!(lock.try_upgradable_read().is_some());
    }
}

#[test]
fn try_upgrade_failure_preserves_guard_and_reservation() {
    let lock = lock(3);
    let upgradable = lock.try_upgradable_read().unwrap();
    let read = lock.try_read().unwrap();
    let mut writer = TestTask::new(lock.write());
    assert!(writer.poll().is_pending());
    let upgradable = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap_err();
    assert_eq!(*upgradable, 0);
    assert!(lock.try_upgradable_read().is_none());
    assert!(lock.try_read().is_some());
    assert!(writer.poll().is_pending());
    drop(read);
    let write = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap();
    assert!(writer.poll().is_pending());
    drop(write);
    drop(expect_ready(writer.poll()));
}

#[test]
fn reader_limits_and_repeated_cycles_include_one_and_usize_max() {
    for limit in [1, 2, 5, usize::MAX >> 1, (usize::MAX >> 1) + 1, usize::MAX] {
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
        let mut reader = TestTask::new(lock.read());
        let mut writer = TestTask::new(lock.write());
        assert!(reader.poll().is_pending());
        assert!(writer.poll().is_pending());
        let mut write = if try_upgrade {
            RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap()
        } else {
            FutureExt::block_on(RwLockUpgradableReadGuard::upgrade(upgradable))
        };
        *write = 7;
        assert!(reader.poll().is_pending());
        let retained = write.downgrade();
        assert!(reader.poll().is_pending());
        assert!(writer.poll().is_pending());
        drop(retained);
        let read = expect_ready(reader.poll());
        assert_eq!(*read, 7);
        drop(read);
        drop(expect_ready(writer.poll()));
    }
}

#[test]
fn reservation_requests_keep_fifo_order_without_upgradable_barging() {
    let lock = lock(2);
    let held = lock.try_upgradable_read().unwrap();
    let mut first = TestTask::new(lock.write());
    let mut second = TestTask::new(lock.upgradable_read());
    let mut third = TestTask::new(lock.write());
    assert!(first.poll().is_pending());
    assert!(second.poll().is_pending());
    assert!(third.poll().is_pending());
    drop(held);
    assert!(lock.try_write().is_none());
    assert!(lock.try_upgradable_read().is_none());
    assert!(third.poll().is_pending());
    let first = expect_ready(first.poll());
    assert!(second.poll().is_pending());
    drop(first);
    let second = expect_ready(second.poll());
    assert!(third.poll().is_pending());
    drop(second);
    drop(expect_ready(third.poll()));
}

#[test]
fn ordinary_writer_blocks_later_readers_once_waiting_for_data() {
    let lock = lock(3);
    let held = lock.try_read().unwrap();
    let mut writer = TestTask::new(lock.write());
    let mut later = TestTask::new(lock.read());
    assert!(writer.poll().is_pending());
    assert!(later.poll().is_pending());
    assert!(lock.try_read().is_none());
    drop(held);
    let writer = expect_ready(writer.poll());
    assert!(later.poll().is_pending());
    drop(writer);
    drop(expect_ready(later.poll()));
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
        let mut writer = TestTask::new(lock.write());
        assert!(writer.poll().is_pending());
        scope.spawn(move || drop(read)).join().unwrap();
        let mut writer = expect_ready(writer.poll());
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
    let mut cancelled = TestTask::new(lock.write());
    let mut successor = TestTask::new(lock.upgradable_read());
    assert!(cancelled.poll().is_pending());
    assert!(successor.poll().is_pending());
    drop(cancelled);
    assert!(successor.poll().is_pending());
    drop(held);
    assert!(successor.is_woken());
    drop(expect_ready(successor.poll()));
    assert!(lock.try_write().is_some());
}

#[test]
fn cancelling_an_upgrade_races_with_the_last_readers_release() {
    let rounds = if cfg!(miri) { 3 } else { 100 };
    for _ in 0..rounds {
        let lock = lock(3);
        let upgradable = lock.try_upgradable_read().unwrap();
        let reader = lock.try_read().unwrap();
        let mut upgrade = TestTask::new(RwLockUpgradableReadGuard::upgrade(upgradable));
        let mut writer = TestTask::new(lock.write());
        assert!(upgrade.poll().is_pending());
        assert!(writer.poll().is_pending());
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
        drop(expect_ready(writer.poll()));
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
                assert!(poll_once(upgrade.as_mut()).is_pending());
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
                let mut write = expect_ready(poll_once(upgrade.as_mut()));
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
                assert!(poll_once(read.as_mut()).is_pending());
                drop(read);
                callbacks.fetch_add(1, Ordering::Relaxed);
            }
        };
        let upgradable = lock.try_upgradable_read().unwrap();
        let reader = lock.try_read().unwrap();
        let mut upgrade = Box::pin(RwLockUpgradableReadGuard::upgrade(upgradable));
        let old_waker = waker_on_drop(reenter());
        assert!(poll_with(upgrade.as_mut(), &old_waker).is_pending());
        drop(old_waker);
        let new_waker = waker_on_wake(reenter());
        assert!(poll_with(upgrade.as_mut(), &new_waker).is_pending());
        assert_eq!(callbacks.load(Ordering::Relaxed), 1);
        drop(reader);
        assert_eq!(callbacks.load(Ordering::Relaxed), 2);
        // Cancellation after the wake must also reclaim every assigned permit.
        drop(upgrade);
        assert!(lock.try_write().is_some());
    });
}

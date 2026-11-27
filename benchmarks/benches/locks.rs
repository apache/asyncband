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

use std::array;
use std::future::Future;
use std::hint::black_box;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::pin;
use std::sync::Arc;

use asyncband::mutex;
use asyncband::rwlock;
use asyncband::semaphore;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use benchmarks::runtime;
use benchmarks::runtime::WORKERS;
use tokio::sync;

struct Asyncband;
struct Tokio;

trait MutexFamily {
    type Lock: Send + Sync + 'static;
    type Guard<'a>: DerefMut<Target = usize>
    where
        Self::Lock: 'a;

    fn new_mutex(value: usize) -> Self::Lock;
    fn lock(lock: &Self::Lock) -> impl Future<Output = Self::Guard<'_>> + Send;
    fn try_lock(lock: &Self::Lock) -> Option<Self::Guard<'_>>;
}

impl MutexFamily for Asyncband {
    type Lock = mutex::Mutex<usize>;
    type Guard<'a> = mutex::MutexGuard<'a, usize>;

    fn new_mutex(value: usize) -> Self::Lock {
        mutex::Mutex::new(value)
    }

    fn lock(lock: &Self::Lock) -> impl Future<Output = Self::Guard<'_>> + Send {
        lock.lock()
    }

    fn try_lock(lock: &Self::Lock) -> Option<Self::Guard<'_>> {
        lock.try_lock()
    }
}

impl MutexFamily for Tokio {
    type Lock = sync::Mutex<usize>;
    type Guard<'a> = sync::MutexGuard<'a, usize>;

    fn new_mutex(value: usize) -> Self::Lock {
        sync::Mutex::new(value)
    }

    fn lock(lock: &Self::Lock) -> impl Future<Output = Self::Guard<'_>> + Send {
        lock.lock()
    }

    fn try_lock(lock: &Self::Lock) -> Option<Self::Guard<'_>> {
        lock.try_lock().ok()
    }
}

trait RwLockFamily {
    type Lock: Send + Sync + 'static;
    type ReadGuard<'a>: Deref<Target = usize>
    where
        Self::Lock: 'a;
    type WriteGuard<'a>: DerefMut<Target = usize>
    where
        Self::Lock: 'a;

    fn new_rwlock(value: usize) -> Self::Lock;
    fn read(lock: &Self::Lock) -> impl Future<Output = Self::ReadGuard<'_>> + Send;
    fn write(lock: &Self::Lock) -> impl Future<Output = Self::WriteGuard<'_>> + Send;
    fn try_read(lock: &Self::Lock) -> Option<Self::ReadGuard<'_>>;
}

impl RwLockFamily for Asyncband {
    type Lock = rwlock::RwLock<usize>;
    type ReadGuard<'a> = rwlock::RwLockReadGuard<'a, usize>;
    type WriteGuard<'a> = rwlock::RwLockWriteGuard<'a, usize>;

    fn new_rwlock(value: usize) -> Self::Lock {
        rwlock::RwLock::new(value)
    }

    fn read(lock: &Self::Lock) -> impl Future<Output = Self::ReadGuard<'_>> + Send {
        lock.read()
    }

    fn write(lock: &Self::Lock) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        lock.write()
    }

    fn try_read(lock: &Self::Lock) -> Option<Self::ReadGuard<'_>> {
        lock.try_read()
    }
}

impl RwLockFamily for Tokio {
    type Lock = sync::RwLock<usize>;
    type ReadGuard<'a> = sync::RwLockReadGuard<'a, usize>;
    type WriteGuard<'a> = sync::RwLockWriteGuard<'a, usize>;

    fn new_rwlock(value: usize) -> Self::Lock {
        sync::RwLock::new(value)
    }

    fn read(lock: &Self::Lock) -> impl Future<Output = Self::ReadGuard<'_>> + Send {
        lock.read()
    }

    fn write(lock: &Self::Lock) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        lock.write()
    }

    fn try_read(lock: &Self::Lock) -> Option<Self::ReadGuard<'_>> {
        lock.try_read().ok()
    }
}

trait SemaphoreFamily {
    type Semaphore: Send + Sync + 'static;
    type Permit<'a>
    where
        Self::Semaphore: 'a;

    fn new_semaphore(permits: usize) -> Self::Semaphore;
    fn available_permits(sem: &Self::Semaphore) -> usize;
    fn release_one(sem: &Self::Semaphore);
    fn try_acquire_one(sem: &Self::Semaphore) -> Option<Self::Permit<'_>>;
    fn acquire_one(sem: &Self::Semaphore) -> impl Future<Output = Self::Permit<'_>> + Send;
    fn forget(permit: Self::Permit<'_>);
}

impl SemaphoreFamily for Asyncband {
    type Semaphore = semaphore::Semaphore;
    type Permit<'a> = semaphore::SemaphorePermit<'a>;

    fn new_semaphore(permits: usize) -> Self::Semaphore {
        semaphore::Semaphore::new(permits)
    }

    fn available_permits(sem: &Self::Semaphore) -> usize {
        sem.available_permits()
    }

    fn release_one(sem: &Self::Semaphore) {
        sem.release(1);
    }

    fn try_acquire_one(sem: &Self::Semaphore) -> Option<Self::Permit<'_>> {
        sem.try_acquire(1)
    }

    fn acquire_one(sem: &Self::Semaphore) -> impl Future<Output = Self::Permit<'_>> + Send {
        sem.acquire(1)
    }

    fn forget(permit: Self::Permit<'_>) {
        permit.forget();
    }
}

impl SemaphoreFamily for Tokio {
    type Semaphore = sync::Semaphore;
    type Permit<'a> = sync::SemaphorePermit<'a>;

    fn new_semaphore(permits: usize) -> Self::Semaphore {
        sync::Semaphore::new(permits)
    }

    fn available_permits(sem: &Self::Semaphore) -> usize {
        sem.available_permits()
    }

    fn release_one(sem: &Self::Semaphore) {
        sem.add_permits(1);
    }

    fn try_acquire_one(sem: &Self::Semaphore) -> Option<Self::Permit<'_>> {
        sem.try_acquire().ok()
    }

    async fn acquire_one(sem: &Self::Semaphore) -> Self::Permit<'_> {
        sem.acquire().await.expect("semaphore should stay open")
    }

    fn forget(permit: Self::Permit<'_>) {
        permit.forget();
    }
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mutex/uncontended",
        55_000_000,
        implementations!([Asyncband, Tokio], |M, bencher| {
            let mutex = M::new_mutex(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut guard = poll_ready(M::lock(&mutex), &mut cx);
                *guard = guard.wrapping_add(1);
            });
            black_box(mutex);
        }),
    );

    runner.compare(
        "mutex/handoff-4-waiters",
        2_600_000,
        implementations!([Asyncband, Tokio], |M, bencher| {
            let mutex = M::new_mutex(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let held = poll_ready(M::lock(&mutex), &mut cx);
                let [f0, f1, f2, f3] = [
                    M::lock(&mutex),
                    M::lock(&mutex),
                    M::lock(&mutex),
                    M::lock(&mutex),
                ];
                let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
                for waiter in &mut waiters {
                    poll_pending(waiter.as_mut(), &mut cx);
                }
                drop(held);
                for waiter in &mut waiters {
                    let mut guard = poll_pinned_ready(waiter.as_mut(), &mut cx);
                    *guard = guard.wrapping_add(1);
                }
            });
            black_box(mutex);
        }),
    );

    runner.compare(
        "mutex/tasks-contended-4w",
        1_200,
        implementations!([Asyncband, Tokio], |M, bencher| {
            let runtime = runtime::multi_thread(WORKERS);
            let mutex = Arc::new(M::new_mutex(0usize));
            let mut expected = 0usize;
            bencher.bench_local(|| {
                let handles: [_; 8] = array::from_fn(|_| {
                    let mutex = Arc::clone(&mutex);
                    runtime.spawn(async move {
                        for _ in 0..512 {
                            let mut guard = M::lock(&mutex).await;
                            *guard = guard.wrapping_add(1);
                        }
                    })
                });
                runtime.block_on(async {
                    for handle in handles {
                        handle.await.expect("mutex task panicked");
                    }
                });
                expected = expected.wrapping_add(4_096);
                assert_eq!(
                    *M::try_lock(&mutex).expect("mutex should be unlocked"),
                    expected
                );
            });
            drop(runtime);
        }),
    );

    runner.compare(
        "rwlock/read-uncontended",
        60_000_000,
        implementations!([Asyncband, Tokio], |L, bencher| {
            let lock = L::new_rwlock(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let guard = poll_ready(L::read(&lock), &mut cx);
                black_box(*guard);
            });
        }),
    );

    runner.compare(
        "rwlock/write-uncontended",
        55_000_000,
        implementations!([Asyncband, Tokio], |L, bencher| {
            let lock = L::new_rwlock(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut guard = poll_ready(L::write(&lock), &mut cx);
                *guard = guard.wrapping_add(1);
            });
            black_box(lock);
        }),
    );

    runner.compare(
        "rwlock/readers-then-writer",
        4_300_000,
        implementations!([Asyncband, Tokio], |L, bencher| {
            let lock = L::new_rwlock(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let readers = [
                    poll_ready(L::read(&lock), &mut cx),
                    poll_ready(L::read(&lock), &mut cx),
                    poll_ready(L::read(&lock), &mut cx),
                    poll_ready(L::read(&lock), &mut cx),
                ];
                let mut writer = pin!(L::write(&lock));
                poll_pending(writer.as_mut(), &mut cx);
                drop(readers);
                let mut guard = poll_pinned_ready(writer.as_mut(), &mut cx);
                *guard = guard.wrapping_add(1);
            });
            black_box(lock);
        }),
    );

    runner.compare(
        "rwlock/tasks-read-heavy-4w",
        1_300,
        implementations!([Asyncband, Tokio], |L, bencher| {
            let runtime = runtime::multi_thread(WORKERS);
            let lock = Arc::new(L::new_rwlock(0usize));
            let mut expected = 0usize;
            bencher.bench_local(|| {
                let handles: [_; 8] = array::from_fn(|_| {
                    let lock = Arc::clone(&lock);
                    runtime.spawn(async move {
                        for _ in 0..64 {
                            for _ in 0..8 {
                                let guard = L::read(&lock).await;
                                black_box(*guard);
                            }
                            let mut guard = L::write(&lock).await;
                            *guard = guard.wrapping_add(1);
                        }
                    })
                });
                runtime.block_on(async {
                    for handle in handles {
                        handle.await.expect("rwlock task panicked");
                    }
                });
                expected = expected.wrapping_add(8 * 64);
                assert_eq!(
                    *L::try_read(&lock).expect("rwlock should be unlocked"),
                    expected
                );
            });
            drop(runtime);
        }),
    );

    runner.local("rwlock/upgradable-upgrade", 27_000_000, |bencher| {
        let lock = rwlock::UpgradableRwLock::new(0usize);
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let guard = poll_ready(lock.upgradable_read(), &mut cx);
            let mut write = poll_ready(rwlock::RwLockUpgradableReadGuard::upgrade(guard), &mut cx);
            *write = write.wrapping_add(1);
        });
        black_box(lock);
    });

    runner.local(
        "rwlock/upgradable-contended-upgrade",
        3_900_000,
        |bencher| {
            let lock = rwlock::UpgradableRwLock::new(0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let upgradable = poll_ready(lock.upgradable_read(), &mut cx);
                let readers = [
                    poll_ready(lock.read(), &mut cx),
                    poll_ready(lock.read(), &mut cx),
                    poll_ready(lock.read(), &mut cx),
                    poll_ready(lock.read(), &mut cx),
                ];
                let mut upgrade = pin!(rwlock::RwLockUpgradableReadGuard::upgrade(upgradable));
                poll_pending(upgrade.as_mut(), &mut cx);
                drop(readers);
                let mut write = poll_pinned_ready(upgrade.as_mut(), &mut cx);
                *write = write.wrapping_add(1);
            });
            black_box(lock);
        },
    );

    runner.compare(
        "semaphore/try-acquire-release",
        60_000_000,
        implementations!([Asyncband, Tokio], |S, bencher| {
            let sem = S::new_semaphore(8);
            bencher.bench_local(|| {
                let permit = S::try_acquire_one(&sem).expect("permits should be available");
                black_box(&permit);
            });
        }),
    );

    runner.compare(
        "semaphore/handoff-permit",
        10_500_000,
        implementations!([Asyncband, Tokio], |S, bencher| {
            let sem = S::new_semaphore(0);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut waiter = pin!(S::acquire_one(&sem));
                poll_pending(waiter.as_mut(), &mut cx);
                S::release_one(&sem);
                let permit = poll_pinned_ready(waiter.as_mut(), &mut cx);
                S::forget(permit);
            });
        }),
    );

    runner.compare(
        "semaphore/cancel-pending",
        18_000_000,
        implementations!([Asyncband, Tokio], |S, bencher| {
            let sem = S::new_semaphore(0);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                {
                    let mut waiter = pin!(S::acquire_one(&sem));
                    poll_pending(waiter.as_mut(), &mut cx);
                }
                assert_eq!(S::available_permits(&sem), 0);
            });
        }),
    );

    runner.compare(
        "semaphore/tasks-contended-4w",
        600,
        implementations!([Asyncband, Tokio], |S, bencher| {
            let runtime = runtime::multi_thread(WORKERS);
            let sem = Arc::new(S::new_semaphore(4));
            bencher.bench_local(|| {
                let handles: [_; 16] = array::from_fn(|_| {
                    let sem = Arc::clone(&sem);
                    runtime.spawn(async move {
                        for _ in 0..64 {
                            let permit = S::acquire_one(&sem).await;
                            black_box(&permit);
                            tokio::task::yield_now().await;
                            drop(permit);
                        }
                    })
                });
                runtime.block_on(async {
                    for handle in handles {
                        handle.await.expect("semaphore task panicked");
                    }
                });
                assert_eq!(S::available_permits(&sem), 4);
            });
            drop(runtime);
        }),
    );
}

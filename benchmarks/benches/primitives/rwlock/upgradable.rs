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

use std::pin::pin;

use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use divan::Bencher;
use divan::black_box;

const READER_COUNTS: &[usize] = &[1, 4, 16];

#[divan::bench]
fn read_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.read(), &mut context);
        black_box(*guard)
    });
}

#[divan::bench]
fn write_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut guard = poll_ready(lock.write(), &mut context);
        *guard = black_box(guard.wrapping_add(1));
        black_box(*guard)
    });
}

#[divan::bench]
fn eight_reads_then_one_write(bencher: Bencher) {
    const READS_PER_WRITE: usize = 8;

    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        for _ in 0..READS_PER_WRITE {
            let guard = poll_ready(lock.read(), &mut context);
            black_box(*guard);
        }
        let mut guard = poll_ready(lock.write(), &mut context);
        *guard = black_box(guard.wrapping_add(1));
        black_box(*guard)
    });
}

#[divan::bench]
fn upgradable_read_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        black_box(*guard)
    });
}

#[divan::bench]
fn upgradable_read_then_upgrade(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        let mut writer = poll_ready(RwLockUpgradableReadGuard::upgrade(guard), &mut context);
        *writer = black_box(writer.wrapping_add(1));
        black_box(*writer)
    });
}

#[divan::bench]
fn upgradable_read_then_try_upgrade(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        let mut writer = RwLockUpgradableReadGuard::try_upgrade(guard)
            .expect("uncontended upgrade should succeed");
        *writer = black_box(writer.wrapping_add(1));
        black_box(*writer)
    });
}

#[divan::bench]
fn upgradable_read_then_downgrade(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        let read = guard.downgrade();
        black_box(*read)
    });
}

// The upgrade waits for the readers to release their data permits, so this covers the slow path
// where the reservation is already held and only the permit transfer remains.
#[divan::bench(args = READER_COUNTS)]
fn readers_then_queued_upgrade(bencher: Bencher, reader_count: usize) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        let readers = (0..reader_count)
            .map(|_| poll_ready(lock.read(), &mut context))
            .collect::<Vec<_>>();
        let mut upgrade = pin!(RwLockUpgradableReadGuard::upgrade(guard));
        poll_pending(upgrade.as_mut(), &mut context);

        drop(readers);
        let mut writer = poll_pinned_ready(upgrade.as_mut(), &mut context);
        *writer = writer.wrapping_add(1);
        black_box(*writer)
    });
}

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

use std::hint::black_box;
use std::pin::pin;

use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const READER_COUNTS: &[usize] = &[1, 4, 16];

fn read_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.read(), &mut context);
        black_box(*guard)
    });
}

fn write_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut guard = poll_ready(lock.write(), &mut context);
        *guard = black_box(guard.wrapping_add(1));
        black_box(*guard)
    });
}

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

fn upgradable_read_reuse(bencher: Bencher) {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.upgradable_read(), &mut context);
        black_box(*guard)
    });
}

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

pub fn register(runner: &mut Runner) {
    primitive!(runner, read_reuse, 29_000_000);
    primitive!(runner, write_reuse, 10_000_000);
    primitive!(runner, eight_reads_then_one_write, 2_600_000);
    primitive!(runner, upgradable_read_reuse, 15_000_000);
    primitive!(runner, upgradable_read_then_upgrade, 8_300_000);
    primitive!(runner, upgradable_read_then_try_upgrade, 10_000_000);
    primitive!(runner, upgradable_read_then_downgrade, 15_000_000);
    for &readers in READER_COUNTS {
        primitive!(runner, readers_then_queued_upgrade(readers), 1_000_000);
    }
}

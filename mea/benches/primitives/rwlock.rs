// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use divan::Bencher;
use divan::black_box;
use mea::rwlock::RwLock;

use super::support::noop_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const READER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn read_heavy_reuse(bencher: Bencher) {
    const READS_PER_WRITE: usize = 8;

    let lock = RwLock::new(0usize);
    let mut context = noop_context();

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

#[divan::bench(args = READER_COUNTS)]
fn writer_handoff(bencher: Bencher, reader_count: usize) {
    let mut context = noop_context();

    bencher.bench_local(|| {
        let lock = Arc::new(RwLock::new(0usize));
        let readers = (0..reader_count)
            .map(|_| poll_ready(lock.clone().read_owned(), &mut context))
            .collect::<Vec<_>>();
        let mut writer = Box::pin(lock.clone().write_owned());
        poll_pending(writer.as_mut(), &mut context);

        drop(readers);
        let mut guard = poll_pinned_ready(writer.as_mut(), &mut context);
        *guard = guard.wrapping_add(1);
        black_box(*guard)
    });
}

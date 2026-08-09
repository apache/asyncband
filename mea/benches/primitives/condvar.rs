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

use std::pin::pin;

use divan::Bencher;
use divan::black_box;
use mea::condvar::Condvar;
use mea::mutex::Mutex;

use super::support::noop_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = noop_context();

    bencher.bench_local(|| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let guard = poll_ready(mutex.lock(), &mut context);

        {
            let mut wait = pin!(condvar.wait(guard));
            poll_pending(wait.as_mut(), &mut context);
        }

        black_box(mutex.try_lock().is_some())
    });
}

#[divan::bench]
fn notify_waiter(bencher: Bencher) {
    let mut context = noop_context();

    bencher.bench_local(|| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let guard = poll_ready(mutex.lock(), &mut context);
        let mut wait = pin!(condvar.wait(guard));
        poll_pending(wait.as_mut(), &mut context);

        condvar.notify_one();
        let guard = poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(&guard);
        drop(guard);
    });
}

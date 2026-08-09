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

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::pin::pin;

use divan::Bencher;
use divan::black_box;
use mea::admission::FairShare;

use super::support::noop_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

type DeterministicFairShare = FairShare<usize, BuildHasherDefault<DefaultHasher>>;

fn fair_share() -> DeterministicFairShare {
    FairShare::with_hasher(1, BuildHasherDefault::default())
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = noop_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);

        {
            let mut pending = pin!(admission.acquire(black_box(1usize)));
            poll_pending(pending.as_mut(), &mut context);
        }

        assert_eq!(admission.num_waiters(), 0);
        drop(held);
        black_box(admission.available_permits())
    });
}

#[divan::bench]
fn handoff(bencher: Bencher) {
    let mut context = noop_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);
        let mut pending = pin!(admission.acquire(black_box(1usize)));
        poll_pending(pending.as_mut(), &mut context);

        drop(held);
        let permit = poll_pinned_ready(pending.as_mut(), &mut context);
        black_box(&permit);
        drop(permit);

        black_box(admission.available_permits())
    });
}

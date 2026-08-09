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

use divan::Bencher;
use divan::black_box;
use mea::semaphore::Semaphore;

#[divan::bench]
fn release(bencher: Bencher) {
    bencher
        .with_inputs(|| Semaphore::new(0))
        .bench_local_values(|semaphore| {
            semaphore.release(black_box(1));
            black_box(semaphore)
        });
}

#[divan::bench]
fn try_acquire_release(bencher: Bencher) {
    let semaphore = Semaphore::new(1);

    bencher.bench_local(|| {
        drop(black_box(semaphore.try_acquire(black_box(1)).unwrap()));
    });
}

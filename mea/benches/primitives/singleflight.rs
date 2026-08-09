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
use mea::singleflight::Group;

use super::support::defer_input_drop;
use super::support::noop_context;
use super::support::poll_ready;

#[divan::bench]
fn work_ready(bencher: Bencher) {
    let mut context = noop_context();
    bencher
        .with_inputs(Group::<usize, usize>::new)
        .bench_local_values(|group| {
            let result = black_box(poll_ready(
                group.work(black_box(0), || async { black_box(1) }),
                &mut context,
            ));
            defer_input_drop(group, result)
        });
}

#[divan::bench]
fn try_work_error(bencher: Bencher) {
    let mut context = noop_context();
    bencher
        .with_inputs(Group::<usize, usize>::new)
        .bench_local_values(|group| {
            let result = black_box(poll_ready(
                group.try_work(black_box(0), || async { Err::<usize, ()>(()) }),
                &mut context,
            ));
            defer_input_drop(group, result)
        });
}

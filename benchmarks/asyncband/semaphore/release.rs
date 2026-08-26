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

use asyncband::semaphore::Semaphore;
use divan::Bencher;
use divan::black_box;

#[divan::bench]
fn fulfill_debt_repeatedly(bencher: Bencher) {
    const CYCLES: usize = 64;

    bencher.bench_local(|| {
        let semaphore = Semaphore::new(0);
        for _ in 0..CYCLES {
            semaphore.reduce_permits(black_box(1));
            semaphore.release(black_box(1));
        }
        black_box(semaphore.available_permits())
    });
}

#[divan::bench]
fn release(bencher: Bencher) {
    bencher
        .with_inputs(|| Semaphore::new(0))
        .bench_local_values(|semaphore| {
            semaphore.release(black_box(1));
            black_box(semaphore)
        });
}

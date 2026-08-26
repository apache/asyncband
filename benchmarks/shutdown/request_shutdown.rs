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

use asyncband::shutdown;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench(args = RECEIVER_COUNTS)]
fn signal_and_join(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (shutdown, guard) = shutdown::new();
        let guards = (0..receiver_count)
            .map(|_| guard.clone())
            .collect::<Vec<_>>();
        drop(guard);

        let mut waits = guards
            .iter()
            .map(|guard| Box::pin(guard.shutdown_requested()))
            .collect::<Vec<_>>();
        for wait in &mut waits {
            poll_pending(wait.as_mut(), &mut context);
        }

        shutdown.request_shutdown();
        for mut wait in waits {
            poll_pinned_ready(wait.as_mut(), &mut context);
        }
        drop(guards);
        poll_ready(shutdown, &mut context);
        black_box(())
    });
}

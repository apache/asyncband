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

use std::future::IntoFuture;
use std::pin::pin;

use asyncband::oneshot::channel;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use divan::Bencher;

#[divan::bench]
fn repoll_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (_sender, receiver) = channel::<usize>();
    let mut receiver = pin!(receiver.into_future());
    poll_pending(receiver.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(receiver.as_mut(), &mut context));
}

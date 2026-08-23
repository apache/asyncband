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

use asyncband::watch;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_ready;

#[divan::bench]
fn send_and_borrow(bencher: Bencher) {
    let (sender, receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.borrow())
    });
}

#[divan::bench]
fn send_and_changed(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(poll_ready(receiver.changed(), &mut context).unwrap())
    });
}

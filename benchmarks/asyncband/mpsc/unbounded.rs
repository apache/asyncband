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

use asyncband::mpsc;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

#[divan::bench]
fn reregister_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (_sender, mut receiver) = mpsc::unbounded::<usize>();
    let mut recv = Box::pin(receiver.recv());
    poll_pending(recv.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(recv.as_mut(), &mut context));
}

#[divan::bench]
fn wake_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = mpsc::unbounded();

    bencher.bench_local(|| {
        let mut recv = Box::pin(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);
        sender.send(black_box(usize::MAX)).unwrap();
        black_box(poll_pinned_ready(recv.as_mut(), &mut context).unwrap())
    });
}

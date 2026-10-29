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

use std::future::poll_fn;
use std::hint::black_box;
use std::sync::mpsc;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;

fn ready(bencher: Bencher) {
    bencher.bench_local(|| async { black_box(42usize) }.block_on());
}

fn self_wake(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut polled = false;
        poll_fn(|context| {
            if polled {
                Poll::Ready(())
            } else {
                polled = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .block_on()
    });
}

fn cross_thread_wake(bencher: Bencher) {
    let (wakers_tx, wakers_rx) = mpsc::channel::<Waker>();
    let worker = thread::spawn(move || {
        for waker in wakers_rx {
            waker.wake();
        }
    });

    bencher.bench_local(|| {
        let mut polled = false;
        poll_fn(|context| {
            if polled {
                Poll::Ready(())
            } else {
                polled = true;
                wakers_tx.send(context.waker().clone()).unwrap();
                Poll::Pending
            }
        })
        .block_on()
    });

    drop(wakers_tx);
    worker.join().unwrap();
}

pub fn register(runner: &mut Runner) {
    runner.case(concat!(module_path!(), "::ready"), 100_000_000, 1, ready);
    runner.case(
        concat!(module_path!(), "::self_wake"),
        60_000_000,
        1,
        self_wake,
    );
    runner.case(
        concat!(module_path!(), "::cross_thread_wake"),
        60_000,
        1,
        cross_thread_wake,
    );
}

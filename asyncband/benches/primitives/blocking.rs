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
use std::sync::mpsc;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use asyncband::blocking::block_on;
use divan::Bencher;
use divan::black_box;

#[divan::bench]
fn ready(bencher: Bencher) {
    bencher.bench_local(|| block_on(async { black_box(42usize) }));
}

#[divan::bench]
fn self_wake(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut polled = false;
        block_on(poll_fn(|context| {
            if polled {
                Poll::Ready(())
            } else {
                polled = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }))
    });
}

#[divan::bench]
fn cross_thread_wake(bencher: Bencher) {
    let (wakers_tx, wakers_rx) = mpsc::channel::<Waker>();
    let worker = thread::spawn(move || {
        for waker in wakers_rx {
            waker.wake();
        }
    });

    bencher.bench_local(|| {
        let mut polled = false;
        block_on(poll_fn(|context| {
            if polled {
                Poll::Ready(())
            } else {
                polled = true;
                wakers_tx.send(context.waker().clone()).unwrap();
                Poll::Pending
            }
        }))
    });

    drop(wakers_tx);
    worker.join().unwrap();
}

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

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use asyncband::barrier::Barrier;
use asyncband::blocking::FutureExt as _;
use asyncband::blocking::block_on;
use asyncband::mutex::Mutex;
use asyncband::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_primitives_compose_across_modules() {
    let barrier = Arc::new(Barrier::new(3));
    let values = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = oneshot::channel();

    let first_barrier = barrier.clone();
    let first_values = values.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_values.lock().await.push(1);
    });

    let second_barrier = barrier.clone();
    let second_values = values.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_values.lock().await.push(2);
        done_tx.send(()).unwrap();
    });

    barrier.wait().await;
    done_rx.await.unwrap();
    first.await.unwrap();
    second.await.unwrap();

    let mut values = values.lock().await;
    values.sort_unstable();
    assert_eq!(*values, [1, 2]);
}

#[test]
fn blocking_bridge_composes_with_public_primitives() {
    let mutex = Mutex::new(1);
    *mutex.lock().block_on() += 1;
    assert_eq!(*block_on(mutex.lock()), 2);

    let (sender, receiver) = oneshot::channel();
    let producer = thread::spawn(move || sender.send(7).unwrap());

    assert_eq!(block_on(receiver), Ok(7));
    producer.join().unwrap();
}

#[test]
fn timed_out_wait_cancels_an_asyncband_future() {
    let (sender, receiver) = oneshot::channel();

    assert_eq!(receiver.wait_timeout(Duration::ZERO), None);
    assert_eq!(sender.send(7).unwrap_err().into_inner(), 7);
}

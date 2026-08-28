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

use super::*;

#[test]
#[should_panic(expected = "watch channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    let (tx, _rx) = channel(0);
    tx.shared.state.lock().version = u64::MAX;
    tx.send(1).unwrap();
}

#[test]
#[should_panic(expected = "watch sender count overflowed")]
fn sender_clone_panics_on_count_overflow() {
    let (tx, _rx) = channel(0);
    tx.shared.state.lock().senders = usize::MAX;
    let _ = tx.clone();
}

#[test]
#[should_panic(expected = "watch receiver count overflowed")]
fn receiver_clone_panics_on_count_overflow() {
    let (tx, rx) = channel(0);
    tx.shared.state.lock().receivers = usize::MAX;
    let _ = rx.clone();
}

#[test]
#[should_panic(expected = "watch receiver count overflowed")]
fn subscribe_panics_on_count_overflow() {
    let (tx, _rx) = channel(0);
    tx.shared.state.lock().receivers = usize::MAX;
    let _ = tx.subscribe();
}

#[test]
fn concurrent_senders_commit_every_version() {
    const SENDERS: usize = 4;
    const SENDS_PER_THREAD: usize = 1_000;

    let (tx, rx) = channel(0);
    let workers = (0..SENDERS)
        .map(|sender_index| {
            let tx = tx.clone();
            std::thread::spawn(move || {
                for offset in 0..SENDS_PER_THREAD {
                    tx.send(sender_index * SENDS_PER_THREAD + offset).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(
        tx.shared.state.lock().version,
        (SENDERS * SENDS_PER_THREAD) as u64
    );
    assert!(*rx.borrow() < SENDERS * SENDS_PER_THREAD);
}

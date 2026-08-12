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

use std::future::pending;

use super::*;
use crate::test_runtime;

#[test]
fn test_single_pair() {
    let (tx, rx) = new_pair();
    let handle = test_runtime().spawn(async move { rx.is_shutdown().await });
    tx.shutdown();
    pollster::block_on(tx.await_shutdown());
    pollster::block_on(handle).unwrap();
}

#[test]
fn test_multiple_tasks() {
    let (tx, rx) = new_pair();
    for _i in 0..100 {
        let rx = rx.clone();
        test_runtime().spawn(async move { rx.is_shutdown().await });
    }
    drop(rx);
    tx.shutdown();
    pollster::block_on(tx.await_shutdown());
}

#[test]
fn test_multiple_senders() {
    let (tx, rx) = new_pair();
    for _i in 0..100 {
        let rx = rx.clone();
        test_runtime().spawn(async move { rx.is_shutdown().await });
    }
    drop(rx);
    let tx_clone = tx.clone();
    tx.shutdown();
    pollster::block_on(tx.await_shutdown());
    pollster::block_on(tx_clone.await_shutdown());
}

#[test]
fn test_is_shutdown_now() {
    let (tx, rx) = new_pair();
    assert!(!rx.is_shutdown_now());
    tx.shutdown();
    assert!(rx.is_shutdown_now());
}

#[test]
fn test_is_shutdown_owned_not_capture_self() {
    struct State {
        rx: ShutdownRecv,
    }

    async fn run_state(_state: &mut State) {
        pending::<()>().await;
    }

    let (tx, rx) = new_pair();
    let mut state = State { rx };
    test_runtime().spawn(async move {
        let is_shutdown = state.rx.is_shutdown_owned();
        tokio::select! {
            _ = is_shutdown => (),
            _ = run_state(&mut state) => (),
        }
    });
    tx.shutdown();
    pollster::block_on(tx.await_shutdown());
}

#[test]
fn test_watch_does_not_block_shutdown() {
    let (tx, rx) = new_pair();
    let watch = rx.watch();
    drop(rx);

    tx.shutdown();
    assert!(watch.is_shutdown_now());
    pollster::block_on(tx.await_shutdown());
}

#[test]
fn test_watch_is_shutdown() {
    let (tx, rx) = new_pair();
    let watch = rx.watch();
    let handle = test_runtime().spawn(async move { watch.is_shutdown().await });
    drop(rx);

    tx.shutdown();
    pollster::block_on(tx.await_shutdown());
    pollster::block_on(handle).unwrap();
}

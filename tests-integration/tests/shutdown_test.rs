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

use std::future::pending;
use std::pin::pin;

use asyncband::blocking::FutureExt;
use asyncband::shutdown::*;
use tests_integration::poll_once;
use tests_integration::test_runtime;

#[test]
fn test_single_pair() {
    let (shutdown, guard) = new();
    let handle = test_runtime().spawn(async move { guard.shutdown_requested().await });
    FutureExt::block_on(shutdown);
    FutureExt::block_on(handle).unwrap();
}

#[test]
fn test_multiple_tasks() {
    let (shutdown, guard) = new();
    for _i in 0..100 {
        let guard = guard.clone();
        test_runtime().spawn(async move { guard.shutdown_requested().await });
    }
    drop(guard);
    FutureExt::block_on(shutdown);
}

#[test]
fn test_multiple_control_handles() {
    let (shutdown, guard) = new();
    for _i in 0..100 {
        let guard = guard.clone();
        test_runtime().spawn(async move { guard.shutdown_requested().await });
    }
    drop(guard);
    let shutdown_clone = shutdown.clone();
    shutdown.request_shutdown();
    FutureExt::block_on(shutdown);
    FutureExt::block_on(shutdown_clone);
}

#[test]
fn test_is_shutdown_requested() {
    let (shutdown, guard) = new();
    assert!(!guard.is_shutdown_requested());
    shutdown.request_shutdown();
    assert!(guard.is_shutdown_requested());
}

#[test]
fn test_shutdown_requested_owned_does_not_capture_self() {
    struct State {
        guard: ShutdownGuard,
    }

    async fn run_state(_state: &mut State) {
        pending::<()>().await;
    }

    let (shutdown, guard) = new();
    let mut state = State { guard };
    test_runtime().spawn(async move {
        let shutdown_requested = state.guard.shutdown_requested_owned();
        tokio::select! {
            _ = shutdown_requested => (),
            _ = run_state(&mut state) => (),
        }
    });
    FutureExt::block_on(shutdown);
}

#[test]
fn test_watch_does_not_block_completion() {
    let (shutdown, guard) = new();
    let watch = guard.into_watch();

    FutureExt::block_on(shutdown);
    assert!(watch.is_shutdown_requested());
}

#[test]
fn test_dropping_unpolled_shutdown_does_not_request_shutdown() {
    let (shutdown, guard) = new();
    let watch = guard.watch();

    drop(shutdown);
    assert!(!watch.is_shutdown_requested());
}

#[test]
fn test_polling_shutdown_requests_and_waits_for_completion() {
    let (shutdown, guard) = new();
    let watch = guard.watch();
    let mut shutdown = pin!(shutdown);

    assert!(poll_once(shutdown.as_mut()).is_pending());
    assert!(watch.is_shutdown_requested());

    drop(guard);
    assert!(poll_once(shutdown.as_mut()).is_ready());
}

#[test]
fn test_dropping_polled_shutdown_keeps_request_sticky() {
    let (shutdown, guard) = new();
    let watch = guard.watch();

    {
        let mut shutdown = pin!(shutdown);
        assert!(poll_once(shutdown.as_mut()).is_pending());
    }

    assert!(watch.is_shutdown_requested());
}

#[test]
fn test_disabled_select_branch_does_not_request_shutdown() {
    FutureExt::block_on(async {
        let (shutdown, guard) = new();
        let watch = guard.watch();

        tokio::select! {
            _ = shutdown, if false => unreachable!(),
            _ = std::future::ready(()) => {}
        }

        assert!(!watch.is_shutdown_requested());
    });
}

#[test]
fn test_watch_observes_shutdown_request() {
    let (shutdown, guard) = new();
    let watch = guard.watch();
    let handle = test_runtime().spawn(async move { watch.shutdown_requested().await });
    drop(guard);

    FutureExt::block_on(shutdown);
    FutureExt::block_on(handle).unwrap();
}

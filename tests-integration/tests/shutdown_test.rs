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
use std::future::ready;
use std::pin::pin;

use asyncband::blocking::FutureExt;
use asyncband::shutdown::*;
use tests_integration::TestTask;
use tests_integration::poll_once;

#[test]
fn test_multiple_tasks() {
    const TASKS: usize = if cfg!(miri) { 16 } else { 100 };
    let (shutdown, guard) = new();
    let mut tasks = (0..TASKS)
        .map(|_| {
            let guard = guard.clone();
            let mut task = TestTask::new(async move {
                guard.shutdown_requested().await;
            });
            assert!(task.poll().is_pending());
            task
        })
        .collect::<Vec<_>>();
    drop(guard);

    let mut shutdown = TestTask::new(shutdown);
    assert!(shutdown.poll().is_pending());

    for mut task in tasks.drain(..) {
        assert!(task.is_woken());
        assert!(task.poll().is_ready());
    }
    assert!(shutdown.is_woken());
    assert!(shutdown.poll().is_ready());
}

#[test]
fn test_multiple_control_handles() {
    const TASKS: usize = if cfg!(miri) { 16 } else { 100 };
    let (shutdown, guard) = new();
    let mut tasks = (0..TASKS)
        .map(|_| {
            let guard = guard.clone();
            let mut task = TestTask::new(async move {
                guard.shutdown_requested().await;
            });
            assert!(task.poll().is_pending());
            task
        })
        .collect::<Vec<_>>();
    drop(guard);

    let mut shutdown_clone = TestTask::new(shutdown.clone());
    shutdown.request_shutdown();
    let mut shutdown = TestTask::new(shutdown);
    assert!(shutdown.poll().is_pending());
    assert!(shutdown_clone.poll().is_pending());

    for mut task in tasks.drain(..) {
        assert!(task.is_woken());
        assert!(task.poll().is_ready());
    }
    assert!(shutdown.is_woken());
    assert!(shutdown_clone.is_woken());
    assert!(shutdown.poll().is_ready());
    assert!(shutdown_clone.poll().is_ready());
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
    let mut worker = TestTask::new(async move {
        let shutdown_requested = state.guard.shutdown_requested_owned();
        tokio::select! {
            _ = shutdown_requested => (),
            _ = run_state(&mut state) => unreachable!(),
        }
    });
    assert!(worker.poll().is_pending());

    let mut shutdown = TestTask::new(shutdown);
    assert!(shutdown.poll().is_pending());
    assert!(worker.is_woken());
    assert!(worker.poll().is_ready());
    drop(worker);

    assert!(shutdown.is_woken());
    assert!(shutdown.poll().is_ready());
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
            _ = ready(()) => {}
        }

        assert!(!watch.is_shutdown_requested());
    });
}

#[test]
fn test_watch_observes_shutdown_request() {
    let (shutdown, guard) = new();
    let watch = guard.watch();
    let mut observer = TestTask::new(async move { watch.shutdown_requested().await });
    assert!(observer.poll().is_pending());
    drop(guard);

    FutureExt::block_on(shutdown);
    assert!(observer.is_woken());
    assert!(observer.poll().is_ready());
}

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

use std::cell::Cell;
use std::future::IntoFuture;
use std::future::pending;
use std::future::poll_fn;
use std::future::ready;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use asyncband::barrier::Barrier;
use asyncband::blocking::FutureExt as _;
use asyncband::mpsc;
use asyncband::oneshot;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

#[tokio::test]
async fn selection_remains_send_and_works_on_an_executor() {
    let (sender, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        asyncband::select! {
            result = receiver => result.unwrap(),
            _ = pending::<()>() => unreachable!(),
        }
    });
    sender.send(String::from("ready")).unwrap();
    assert_eq!(task.await.unwrap(), "ready");
}

#[test]
fn selection_consumes_only_one_ready_message() {
    let (left_tx, mut left_rx) = mpsc::unbounded();
    let (right_tx, mut right_rx) = mpsc::unbounded();
    left_tx.send(String::from("left")).unwrap();
    right_tx.send(String::from("right")).unwrap();

    let selected_left = async {
        asyncband::select! {
            value = left_rx.recv() => {
                assert_eq!(value.unwrap(), "left");
                true
            },
            value = right_rx.recv() => {
                assert_eq!(value.unwrap(), "right");
                false
            },
        }
    }
    .block_on();

    if selected_left {
        assert!(left_rx.try_recv().is_err());
        assert_eq!(right_rx.try_recv().unwrap(), "right");
    } else {
        assert_eq!(left_rx.try_recv().unwrap(), "left");
        assert!(right_rx.try_recv().is_err());
    }
}

#[test]
fn ready_errors_are_delivered_without_polling_lower_priority_branches() {
    let (sender, receiver) = oneshot::channel::<String>();
    drop(sender);
    async {
        asyncband::select! {
            biased;
            result = receiver => assert!(result.is_err()),
            _ = poll_fn(|_| -> Poll<()> { panic!("polled after choosing a result") }) => {},
        }
    }
    .block_on();
}

#[test]
fn a_later_branch_can_wake_an_earlier_pending_branch() {
    let signalled = Cell::new(false);
    let mut selection = pin!(async {
        asyncband::select! {
            biased;
            value = poll_fn(|_| {
                if signalled.get() { Poll::Ready(42) } else { Poll::Pending }
            }) => value,
            _ = poll_fn(|cx| {
                if !signalled.replace(true) {
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            }) => unreachable!(),
        }
    });
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(selection.as_mut(), &waker).is_pending());
    assert_eq!(wakes.take(), 1);
    assert_eq!(expect_ready(poll_with(selection.as_mut(), &waker)), 42);
}

#[test]
fn pending_branches_receive_the_latest_parent_waker() {
    let (left_tx, left_rx) = oneshot::channel::<i32>();
    let (right_tx, right_rx) = oneshot::channel::<String>();
    let mut selection = pin!(async {
        asyncband::select! {
            result = left_rx => result.unwrap(),
            result = right_rx => result.unwrap().len() as i32,
        }
    });
    let (old_waker, old_wakes) = WakeCounter::new();
    let (new_waker, new_wakes) = WakeCounter::new();
    assert!(poll_with(selection.as_mut(), &old_waker).is_pending());
    assert!(poll_with(selection.as_mut(), &new_waker).is_pending());
    left_tx.send(7).unwrap();
    assert_eq!(old_wakes.count(), 0);
    assert_eq!(new_wakes.count(), 1);
    assert_eq!(expect_ready(poll_with(selection.as_mut(), &new_waker)), 7);
    assert!(right_tx.send(String::from("cancelled")).is_err());
}

#[test]
fn dropping_the_selection_cancels_all_owned_branches() {
    let (left_tx, left_rx) = oneshot::channel::<()>();
    let (right_tx, right_rx) = oneshot::channel::<()>();
    {
        let mut selection = pin!(async {
            asyncband::select! {
                result = left_rx => result,
                result = right_rx => result,
            }
        });
        assert!(poll_once(selection.as_mut()).is_pending());
    }
    assert!(left_tx.send(()).is_err());
    assert!(right_tx.send(()).is_err());
}

#[test]
fn cancelling_a_granted_reservation_releases_capacity_before_the_handler() {
    let (sender, mut receiver) = mpsc::bounded(1);
    let mut held = Some(sender.try_reserve().unwrap());
    async {
        asyncband::select! {
            biased;
            _ = sender.reserve() => panic!("capacity is initially occupied"),
            _ = poll_fn(|_| {
                // The earlier branch receives a capacity grant after it returned Pending.
                drop(held.take());
                Poll::Ready(())
            }) => sender.try_send(7).unwrap(),
        }
    }
    .block_on();
    assert_eq!(receiver.try_recv().unwrap(), 7);
}

#[test]
fn borrowed_barrier_arrival_survives_another_branch_winning() {
    let barrier = Barrier::new(2);
    let mut arrival = pin!(barrier.wait());
    async {
        asyncband::select! {
            biased;
            _ = arrival.as_mut() => panic!("second participant has not arrived"),
            _ = ready(()) => {},
        }
    }
    .block_on();

    assert!(barrier.wait().block_on().is_leader());
    assert!(!arrival.block_on().is_leader());
}

struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

struct OwnedReady(DropFlag);

impl Future for OwnedReady {
    type Output = usize;

    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<usize> {
        assert!(!self.0.0.get());
        Poll::Ready(42)
    }
}

#[test]
fn completed_future_is_dropped_before_the_handler() {
    let dropped = Rc::new(Cell::new(false));
    let future = OwnedReady(DropFlag(dropped.clone()));
    async {
        asyncband::select! {
            value = future => {
                assert!(dropped.get());
                assert_eq!(value, 42);
            },
        }
    }
    .block_on();
}

struct DisabledAwaitable {
    converted: Rc<Cell<bool>>,
    drop_flag: DropFlag,
}

impl IntoFuture for DisabledAwaitable {
    type Output = DropFlag;
    type IntoFuture = std::future::Ready<DropFlag>;

    fn into_future(self) -> Self::IntoFuture {
        self.converted.set(true);
        ready(self.drop_flag)
    }
}

#[test]
fn disabled_branches_are_constructed_converted_and_dropped_before_fallback() {
    let converted = Rc::new(Cell::new(false));
    let dropped = Rc::new(Cell::new(false));
    let conditions = Cell::new(0);
    async {
        asyncband::select! {
            _ = {
                assert_eq!(conditions.get(), 2);
                DisabledAwaitable {
                    converted: converted.clone(),
                    drop_flag: DropFlag(dropped.clone()),
                }
            }, if { conditions.set(conditions.get() + 1); false } => panic!("disabled branch ran"),
            _ = async { panic!("disabled branch was polled") },
                if { conditions.set(conditions.get() + 1); false } => {},
            else => {
                assert!(converted.get());
                assert!(dropped.get());
            },
        }
    }
    .block_on();
}

#[test]
fn conditions_are_not_reevaluated_and_fallback_does_not_replace_pending() {
    let condition_checks = Cell::new(0);
    let (sender, receiver) = oneshot::channel();
    let mut selection = pin!(async {
        asyncband::select! {
            value = receiver, if { condition_checks.set(condition_checks.get() + 1); true } => value,
            else => panic!("enabled branch is pending"),
        }
    });
    assert!(poll_once(selection.as_mut()).is_pending());
    assert!(poll_once(selection.as_mut()).is_pending());
    assert_eq!(condition_checks.get(), 1);
    sender.send(9).unwrap();
    assert_eq!(expect_ready(poll_once(selection.as_mut())), Ok(9));
}

#[test]
#[should_panic(expected = "select! has no enabled branches")]
fn all_disabled_without_fallback_panics() {
    async {
        asyncband::select! {
            _ = pending::<()>(), if false => {},
        }
    }
    .block_on();
}

#[test]
fn handlers_release_borrows_and_preserve_enclosing_control_flow() {
    async fn run() -> Result<Vec<String>, ()> {
        let local = Rc::new(String::from("local"));
        let mut values = Vec::new();
        loop {
            asyncband::select! {
                biased;
                result = async {
                    pending::<()>().await;
                    values.push(String::from("unfinished"));
                } => result,
                (mut value, marker) = async { (local.to_string(), true) } => {
                    ready(Ok::<_, ()>(())).await?;
                    value.push('!');
                    values.push(value);
                    assert!(marker);
                    if values.len() == 1 {
                        continue;
                    }
                    break;
                },
            }
        }
        asyncband::select! {
            should_return = ready(true) => {
                if should_return {
                    return Ok(values);
                }
            },
        }
        Err(())
    }

    assert_eq!(run().block_on().unwrap(), ["local!", "local!"]);
}

#[test]
fn retained_oneshot_is_disabled_after_completion() {
    let (sender, receiver) = oneshot::channel();
    sender.send(String::from("done")).unwrap();
    let mut receiver = pin!(receiver.into_future());
    let mut output = None;
    async {
        loop {
            asyncband::select! {
                result = receiver.as_mut(), if output.is_none() => output = Some(result.unwrap()),
                else => break,
            }
        }
    }
    .block_on();
    assert_eq!(output.as_deref(), Some("done"));
}

#[test]
fn panicking_poll_drops_previously_registered_branches() {
    let (sender, receiver) = oneshot::channel::<()>();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        async {
            asyncband::select! {
                biased;
                _ = receiver => {},
                _ = poll_fn(|_| -> Poll<()> { panic!("poll failed") }) => {},
            }
        }
        .block_on();
    }));
    assert!(result.is_err());
    assert!(sender.send(()).is_err());
}

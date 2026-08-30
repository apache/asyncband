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
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::task::Poll;

use super::Group;
use crate::test_support::poll_once;

// These tests stay next to the implementation because they inspect private state.

fn entry_count<K, V, S>(group: &Group<K, V, S>) -> usize {
    group.entries.lock().len()
}

#[test]
fn panicked_work_removes_empty_entry() {
    let group = Group::<&str, String>::new();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut work = pin!(group.work("key", || async {
            panic!("oops");
        }));
        let _ = poll_once(work.as_mut());
    }));

    assert!(result.is_err());
    assert_eq!(entry_count(&group), 0);

    let mut retry = pin!(group.work("key", || async { "success".to_owned() }));
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready("success".to_owned()));
}

#[test]
fn cancelled_work_removes_empty_entry() {
    let group = Group::<&str, &str>::new();

    {
        let mut work = pin!(group.work("key", || async { std::future::pending::<&str>().await }));
        assert!(poll_once(work.as_mut()).is_pending());
        assert_eq!(entry_count(&group), 1);
    }

    assert_eq!(entry_count(&group), 0);
}

#[test]
fn failed_try_work_removes_empty_entry() {
    let group = Group::new();

    let mut work = pin!(group.try_work("key", || async { Err::<&str, &str>("error") }));
    assert_eq!(poll_once(work.as_mut()), Poll::Ready(Err("error")));
    assert_eq!(entry_count(&group), 0);

    let mut retry = pin!(group.try_work("key", || async { Ok::<&str, ()>("success") }));
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready(Ok("success")));
}

#[test]
fn failed_try_work_preserves_entry_for_waiter_retry() {
    let group = Group::new();
    let released = Cell::new(false);

    let first = group.try_work("key", || async {
        poll_fn(|_| {
            released
                .get()
                .then_some(())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;
        Err::<&str, &str>("fail")
    });
    let mut first = pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = group.try_work("key", || async { Ok::<&str, &str>("success") });
    let mut retry = pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    released.set(true);
    assert_eq!(poll_once(first.as_mut()), Poll::Ready(Err("fail")));

    assert_eq!(entry_count(&group), 1);
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready(Ok("success")));
    assert_eq!(entry_count(&group), 0);
}

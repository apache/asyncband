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

use std::future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use asyncband::blocking::FutureExt;
use asyncband::task_group::TaskGroup;
use tokio::sync::oneshot;

struct PublishOnDrop<'a>(&'a AtomicUsize);

impl Drop for PublishOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn independently_spawned_tasks_are_joined_in_completion_order() {
    let (mut group, registrar) = TaskGroup::new();
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();

    let first = tokio::spawn(
        registrar
            .track(async move {
                first_rx.await.unwrap();
                1
            })
            .unwrap(),
    );
    let second = tokio::spawn(
        registrar
            .track(async move {
                second_rx.await.unwrap();
                2
            })
            .unwrap(),
    );
    group.close();

    second_tx.send(()).unwrap();
    assert_eq!(group.join_next().await, Some(2));
    first_tx.send(()).unwrap();
    assert_eq!(group.join_next().await, Some(1));
    assert_eq!(group.join_next().await, None);

    first.await.unwrap();
    second.await.unwrap();
}

#[tokio::test]
async fn a_child_can_register_nested_work_before_close() {
    let (mut group, registrar) = TaskGroup::new();
    let child_registrar = registrar.clone();
    let (registered_tx, registered_rx) = oneshot::channel();

    tokio::spawn(
        registrar
            .track(async move {
                tokio::spawn(child_registrar.track(async { 2 }).unwrap());
                registered_tx.send(()).unwrap();
                1
            })
            .unwrap(),
    );

    registered_rx.await.unwrap();
    group.close();
    let mut outputs = group.join().await;
    outputs.sort_unstable();
    assert_eq!(outputs, [1, 2]);
}

#[tokio::test]
async fn aborting_a_spawned_task_releases_its_registration() {
    let (mut group, registrar) = TaskGroup::<()>::new();
    let task = tokio::spawn(registrar.track(future::pending()).unwrap());
    group.close();

    task.abort();
    assert!(task.await.is_err());
    tokio::time::timeout(Duration::from_secs(1), group.wait())
        .await
        .expect("the dropped task must not keep the group active");
}

#[tokio::test]
async fn wait_discards_outputs() {
    let (mut group, registrar) = TaskGroup::new();
    tokio::spawn(registrar.track(async { 1 }).unwrap());
    tokio::spawn(registrar.track(async { 2 }).unwrap());
    group.close();

    group.wait().await;
    assert_eq!(group.join_next().await, None);
}

#[test]
fn cancelled_tasks_publish_their_destructor_writes_before_wait_returns() {
    let values = std::array::from_fn::<_, 8, _>(|_| AtomicUsize::new(0));
    let (mut group, registrar) = TaskGroup::new();
    let tasks = values
        .iter()
        .map(|value| {
            let publish = PublishOnDrop(value);
            registrar
                .track(async move {
                    let _publish = publish;
                    future::pending::<()>().await;
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    group.close();

    std::thread::scope(|scope| {
        for task in tasks {
            scope.spawn(move || drop(task));
        }
        FutureExt::block_on(group.wait());
        assert!(
            values
                .iter()
                .all(|value| value.load(Ordering::Relaxed) == 1)
        );
    });
}

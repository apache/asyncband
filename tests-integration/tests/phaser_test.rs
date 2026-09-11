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

use asyncband::phaser::Phaser;

#[tokio::test]
async fn participant_can_wait_from_a_spawned_task() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    let first_wait = tokio::spawn(async move { first.wait().await });

    tokio::task::yield_now().await;
    let phase = second.arrive().unwrap();
    assert_eq!(first_wait.await.unwrap().unwrap(), phaser.phase());
    assert_ne!(phase, phaser.phase());
}

#[tokio::test]
async fn observer_waits_without_becoming_a_party() {
    let phaser = Phaser::new();
    let observed = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let second = phaser.register_one().unwrap();
    let observer_phaser = phaser.clone();
    let observer = tokio::spawn(async move { observer_phaser.wait(observed).await });

    tokio::task::yield_now().await;
    assert_eq!(phaser.registered_parties(), 2);
    first.arrive().unwrap();
    second.deregister().unwrap();

    assert_eq!(observer.await.unwrap().unwrap(), phaser.phase());
    assert_eq!(phaser.registered_parties(), 1);
}

#[test]
fn arrivals_publish_each_workers_writes_across_threads() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    let phaser = Phaser::new();
    let values = std::array::from_fn::<_, 4, _>(|_| AtomicUsize::new(0));
    let participants = phaser.register(values.len()).unwrap();
    std::thread::scope(|scope| {
        for (id, mut participant) in participants.enumerate() {
            let values = &values;
            scope.spawn(move || {
                pollster::block_on(async {
                    for round in 1..=16 {
                        values[id].store(round, Ordering::Relaxed);
                        participant.wait().await.unwrap();
                        assert!(
                            values
                                .iter()
                                .all(|value| value.load(Ordering::Relaxed) == round)
                        );
                        // Keep the next round's writers behind this read-side rendezvous.
                        participant.wait().await.unwrap();
                    }
                });
            });
        }
    });
}

#[tokio::test]
async fn a_failed_task_can_close_the_group_without_reporting_phase_completion() {
    let phaser = Phaser::new();
    let mut worker = phaser.register_one().unwrap();
    let failing = phaser.register_one().unwrap();
    let observed = phaser.phase();
    let (arrived, arrival) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        worker.arrive().unwrap();
        arrived.send(()).unwrap();
        worker.wait().await
    });
    arrival.await.unwrap();
    failing.phaser().close();
    drop(failing);
    assert!(task.await.unwrap().is_err());
    assert_eq!(phaser.phase(), observed);
    assert_eq!(phaser.registered_parties(), 0);
}

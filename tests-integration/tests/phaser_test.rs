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

use asyncband::phaser::Phaser;

#[tokio::test]
async fn participant_can_wait_from_a_spawned_task() {
    let phaser = Arc::new(Phaser::new());
    let mut first = phaser.register();
    let mut second = phaser.register();

    let first_wait = tokio::spawn(async move { first.arrive_and_wait().await });

    tokio::task::yield_now().await;
    let phase = second.arrive();
    assert_eq!(first_wait.await.unwrap(), phaser.phase());
    assert_ne!(phase, phaser.phase());
}

#[tokio::test]
async fn observer_waits_without_becoming_a_party() {
    let phaser = Arc::new(Phaser::new());
    let observed = phaser.phase();
    let mut first = phaser.register();
    let second = phaser.register();
    let observer_phaser = Arc::clone(&phaser);
    let observer = tokio::spawn(async move { observer_phaser.wait_for_advance(observed).await });

    tokio::task::yield_now().await;
    assert_eq!(phaser.registered_parties(), 2);
    first.arrive();
    second.arrive_and_deregister();

    assert_eq!(observer.await.unwrap(), phaser.phase());
    assert_eq!(phaser.registered_parties(), 1);
}

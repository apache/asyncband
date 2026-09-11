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

use std::task::Poll;

use super::Phaser;
use crate::test_support::poll_once;

#[test]
fn phase_identity_wraps_without_an_ordering_contract() {
    let phaser = Phaser::new();
    phaser.state.lock().phase = u64::MAX;
    let observed = phaser.phase();
    let mut participant = phaser.register_one().unwrap();

    assert_eq!(participant.arrive().unwrap(), observed);
    assert_eq!(phaser.phase(), 0);
    assert_ne!(phaser.phase(), observed);
}

#[test]
fn a_late_waiter_observes_completion_across_counter_wraparound() {
    let phaser = Phaser::new();
    phaser.state.lock().phase = u64::MAX;
    let mut participant = phaser.register_one().unwrap();
    participant.arrive().unwrap();
    phaser.close();
    assert_eq!(
        poll_once(Box::pin(participant.wait()).as_mut()),
        Poll::Ready(Ok(0))
    );
}

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

use asyncband::barrier::Barrier;
use tests_integration::TestTask;
use tests_integration::expect_ready;

#[test]
fn barrier_can_be_constructed_in_const_context() {
    static BARRIER: Barrier = Barrier::new(2);

    let _ = Barrier::new(u32::MAX);

    let mut first = TestTask::new(BARRIER.wait());
    assert!(first.poll().is_pending());
    let mut second = TestTask::new(BARRIER.wait());
    assert!(expect_ready(second.poll()).is_leader());
    assert!(!expect_ready(first.poll()).is_leader());
}

#[test]
fn zero_and_one_participant_generations_never_block() {
    for participants in [0, 1] {
        let barrier = Barrier::new(participants);
        for _ in 0..3 {
            let mut wait = TestTask::new(barrier.wait());
            assert!(expect_ready(wait.poll()).is_leader());
        }
    }
}

#[test]
fn one_leader_completes_each_two_participant_generation() {
    let b = Barrier::new(2);

    let mut f1 = TestTask::new(b.wait());
    assert!(f1.poll().is_pending());

    let mut f2 = TestTask::new(b.wait());
    let f2_leader = expect_ready(f2.poll()).is_leader();
    let f1_leader = expect_ready(f1.poll()).is_leader();

    assert!(f1_leader || f2_leader);
    assert!(!(f1_leader && f2_leader));
}

#[test]
fn cancelled_wait_still_counts_as_arrival() {
    let b = Barrier::new(2);

    let mut cancelled = TestTask::new(b.wait());
    assert!(cancelled.poll().is_pending());
    drop(cancelled);

    let mut remaining = TestTask::new(b.wait());
    assert!(expect_ready(remaining.poll()).is_leader());
}

#[test]
fn dropping_a_woken_wait_keeps_the_next_generation_registered() {
    let b = Barrier::new(2);

    let mut old = TestTask::new(b.wait());
    assert!(old.poll().is_pending());
    let mut old_leader = TestTask::new(b.wait());
    assert!(expect_ready(old_leader.poll()).is_leader());

    let mut next = TestTask::new(b.wait());
    assert!(next.poll().is_pending());
    drop(old);

    let mut next_leader = TestTask::new(b.wait());
    assert!(expect_ready(next_leader.poll()).is_leader());
    assert!(next.is_woken());
    assert!(!expect_ready(next.poll()).is_leader());
}

#[test]
fn repeated_generations_elect_exactly_one_leader() {
    let b = Barrier::new(100);

    for _ in 0..10 {
        let mut wait = vec![];
        for _ in 0..99 {
            let mut f = TestTask::new(b.wait());
            assert!(f.poll().is_pending());
            wait.push(f);
        }

        for f in &mut wait {
            assert!(f.poll().is_pending());
        }

        // pass the barrier
        let mut f = TestTask::new(b.wait());
        let mut found_leader = expect_ready(f.poll()).is_leader();
        for mut f in wait {
            let leader = expect_ready(f.poll());
            if leader.is_leader() {
                assert!(!found_leader);
                found_leader = true;
            }
        }
        assert!(found_leader);
    }
}

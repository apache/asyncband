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

use tokio_test::assert_pending;
use tokio_test::assert_ready;
use tokio_test::task::spawn;

use crate::barrier::Barrier;

#[test]
fn zero_does_not_block() {
    let b = Barrier::new(0);
    {
        let mut f = spawn(b.wait());
        let leader = assert_ready!(f.poll());
        assert!(leader.is_leader());
    }
    {
        let mut f = spawn(b.wait());
        let leader = assert_ready!(f.poll());
        assert!(leader.is_leader());
    }
}

#[test]
fn single() {
    let b = Barrier::new(1);
    {
        let mut f = spawn(b.wait());
        let leader = assert_ready!(f.poll());
        assert!(leader.is_leader());
    }
    {
        let mut f = spawn(b.wait());
        let leader = assert_ready!(f.poll());
        assert!(leader.is_leader());
    }
    {
        let mut f = spawn(b.wait());
        let leader = assert_ready!(f.poll());
        assert!(leader.is_leader());
    }
}

#[test]
fn tango() {
    let b = Barrier::new(2);

    let mut f1 = spawn(b.wait());
    assert_pending!(f1.poll());

    let mut f2 = spawn(b.wait());
    let f2_leader = assert_ready!(f2.poll()).is_leader();
    let f1_leader = assert_ready!(f1.poll()).is_leader();

    assert!(f1_leader || f2_leader);
    assert!(!(f1_leader && f2_leader));
}

#[test]
fn lots() {
    let b = Barrier::new(100);

    for _ in 0..10 {
        let mut wait = Vec::new();
        for _ in 0..99 {
            let mut f = spawn(b.wait());
            assert_pending!(f.poll());
            wait.push(f);
        }

        for f in &mut wait {
            assert_pending!(f.poll());
        }

        // pass the barrier
        let mut f = spawn(b.wait());
        let mut found_leader = assert_ready!(f.poll()).is_leader();
        for mut f in wait {
            let leader = assert_ready!(f.poll());
            if leader.is_leader() {
                assert!(!found_leader);
                found_leader = true;
            }
        }
        assert!(found_leader);
    }
}

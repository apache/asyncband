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

//! Group local arrivals before one global rendezvous, then release the local workers.
//!
//! A group representative contributes one root participant. Each group runs an explicit driver
//! task and uses separate local counters. A local ready phase must never authorize the next round
//! until the root has also completed.
//! The example uses a fixed cohort for three rounds; automatic parent registration and arbitrary
//! concurrent changes to a hierarchical participant set are not provided by this composition.
//! No performance advantage over a flat Phaser is claimed without workload-specific measurement.
//!
//! Run: cargo run -p examples --example phaser_groups

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use asyncband::phaser::Closed;
use asyncband::phaser::Phaser;
use asyncband::phaser::PhaserParticipant;

const GROUPS: usize = 2;
const WORKERS_PER_GROUP: usize = 2;
const ROUNDS: u64 = 3;

struct AbortOnDrop {
    phasers: [Phaser; 3],
    armed: bool,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if self.armed {
            for phaser in &self.phasers {
                phaser.close();
            }
        }
    }
}

struct LocalMember {
    // Abort before dropping either participant, including when a task is never polled.
    abort: AbortOnDrop,
    ready: PhaserParticipant,
    resume: PhaserParticipant,
}

impl LocalMember {
    fn register(root: &Phaser, ready: &Phaser, resume: &Phaser) -> Result<Self, Closed> {
        Ok(Self {
            abort: AbortOnDrop {
                phasers: [root.clone(), ready.clone(), resume.clone()],
                armed: true,
            },
            ready: ready.register_one()?,
            resume: resume.register_one()?,
        })
    }
}

async fn worker(
    mut member: LocalMember,
    id: usize,
    values: Arc<Vec<AtomicU64>>,
    fail: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for round in 1..=ROUNDS {
        if fail && round == 2 {
            // The abort guard closes the root before any participant is withdrawn.
            return Err("input validation failed".into());
        }
        values[id].store(round, Ordering::Relaxed);
        member.ready.wait().await?;
        member.resume.wait().await?;
        // Every group, not merely this worker's local group, must have published this round.
        assert!(
            values
                .iter()
                .all(|value| value.load(Ordering::Relaxed) >= round)
        );
    }
    member.abort.armed = false;
    Ok(())
}

struct GroupDriver {
    // Keep the root obligation behind the local abort guard in the same owned task argument.
    local: LocalMember,
    root: PhaserParticipant,
}

async fn drive_group(mut driver: GroupDriver) -> Result<(), Box<dyn Error + Send + Sync>> {
    for _ in 0..ROUNDS {
        driver.local.ready.wait().await?;
        driver.root.wait().await?;
        driver.local.resume.wait().await?;
    }
    driver.local.abort.armed = false;
    Ok(())
}

struct CloseRootOnDrop(Phaser);

impl Drop for CloseRootOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    run_groups(false).await?;
    assert!(run_groups(true).await.unwrap_err().is::<Closed>());
    println!("group failure: root closure propagated to every local group");
    Ok(())
}

async fn run_groups(fail_one_worker: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
    let root = Phaser::new();
    let mut coordinator = root.register_one()?;
    // Created after the participant so cancellation closes the root before withdrawing it.
    let _close_root = CloseRootOnDrop(root.clone());
    let values = Arc::new(
        (0..GROUPS * WORKERS_PER_GROUP)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let mut tasks = Vec::new();
    for group in 0..GROUPS {
        let ready = Phaser::new();
        let resume = Phaser::new();
        let driver = LocalMember::register(&root, &ready, &resume)?;
        let representative = root.register_one()?;
        for worker_id in 0..WORKERS_PER_GROUP {
            let member = LocalMember::register(&root, &ready, &resume)?;
            let id = group * WORKERS_PER_GROUP + worker_id;
            tasks.push(tokio::spawn(worker(
                member,
                id,
                values.clone(),
                fail_one_worker && id == 0,
            )));
        }
        tasks.push(tokio::spawn(drive_group(GroupDriver {
            local: driver,
            root: representative,
        })));
    }
    assert_eq!(root.registered_parties(), GROUPS + 1);

    for round in 1..=ROUNDS {
        if let Err(error) = coordinator.wait().await {
            // Root closure propagates through the group drivers to their local waiters.
            root.close();
            let mut failures = 0;
            for task in tasks {
                let error = task.await.expect("group task panicked").unwrap_err();
                if !error.is::<Closed>() {
                    assert_eq!(error.to_string(), "input validation failed");
                    failures += 1;
                }
            }
            assert_eq!(failures, 1);
            assert_eq!(root.phase(), 1);
            return Err(error.into());
        }
        println!("root: all {GROUPS} groups completed round {round}");
    }
    for task in tasks {
        task.await.expect("group task panicked")?;
    }
    assert!(
        values
            .iter()
            .all(|value| value.load(Ordering::Relaxed) == ROUNDS)
    );
    println!("grouped fan-in: four workers synchronized through two root representatives");
    Ok(())
}

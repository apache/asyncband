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

//! Finalize each round before releasing workers, and close the group on failure or cancellation.
//!
//! An application coordinator aggregates results and checks convergence between two rendezvous
//! points. The coordinator may await I/O. Merely running code after one wait, even in a barrier
//! leader, would not stop other workers from starting their next round.
//!
//! Membership is fixed within this protocol; changes must update both groups at a common round
//! boundary. Each phaser has its own counter, distinct from the application's iteration number.
//!
//! Run: cargo run -p examples --example phaser_completion

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use asyncband::phaser::Closed;
use asyncband::phaser::Phaser;
use asyncband::phaser::PhaserParticipant;

// Own this guard before constructing a task future so cancelling an unpolled task also aborts.
struct CloseOnDrop([Phaser; 2]);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        for phaser in &self.0 {
            phaser.close();
        }
    }
}

struct Member {
    // Fields drop in declaration order: close before withdrawing any arrival obligation.
    _close: CloseOnDrop,
    ready: PhaserParticipant,
    resume: PhaserParticipant,
}

impl Member {
    fn register(ready: &Phaser, resume: &Phaser) -> Result<Self, Closed> {
        Ok(Self {
            _close: CloseOnDrop([ready.clone(), resume.clone()]),
            ready: ready.register_one()?,
            resume: resume.register_one()?,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Closed> {
    finalize_until_converged().await?;
    failure_closes_the_group().await?;
    cancelling_an_unpolled_task_closes_the_group().await?;
    Ok(())
}

async fn compute(
    mut member: Member,
    id: usize,
    values: Arc<Vec<AtomicU64>>,
    published: Arc<AtomicU64>,
) -> Result<(), Closed> {
    for round in 1_u64.. {
        // The resume rendezvous must publish the previous aggregate before this read.
        assert_eq!(published.load(Ordering::Relaxed), (round - 1) * 6);
        values[id].store((id as u64 + 1) * round, Ordering::Relaxed);
        member.ready.wait().await?;
        member.resume.wait().await?;
    }
    unreachable!()
}

async fn finalize_until_converged() -> Result<(), Closed> {
    let ready = Phaser::new();
    let resume = Phaser::new();
    let mut coordinator = Member::register(&ready, &resume)?;
    let values = Arc::new((0..3).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let published = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    // Register everyone before polling any worker; the coordinator also keeps both phases open.
    for id in 0..3 {
        tasks.push(tokio::spawn(compute(
            Member::register(&ready, &resume)?,
            id,
            values.clone(),
            published.clone(),
        )));
    }

    loop {
        coordinator.ready.wait().await?;
        let sum: u64 = values
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        // An async checkpoint can be awaited here while workers wait at resume.
        tokio::task::yield_now().await;
        published.store(sum, Ordering::Relaxed);
        println!("coordinator: published aggregate {sum}");
        if sum >= 18 {
            // Stop without releasing anyone into another computation round.
            ready.close();
            resume.close();
            break;
        }
        coordinator.resume.wait().await?;
    }
    for task in tasks {
        assert_eq!(task.await.expect("worker panicked"), Err(Closed));
    }
    assert_eq!(published.load(Ordering::Relaxed), 18);
    println!("convergence: all workers stopped after the third aggregate");
    Ok(())
}

async fn wait_once(mut member: Member) -> Result<(), Closed> {
    member.ready.wait().await?;
    member.resume.wait().await?;
    Ok(())
}

async fn fail(_member: Member) -> Result<(), &'static str> {
    // The job error stays in its result; Closed tells peers that no next round is available.
    Err("input validation failed")
}

async fn failure_closes_the_group() -> Result<(), Closed> {
    let ready = Phaser::new();
    let resume = Phaser::new();
    let healthy = Member::register(&ready, &resume)?;
    let failing = Member::register(&ready, &resume)?;
    let (peer, failure) = tokio::join!(wait_once(healthy), fail(failing));
    assert_eq!(peer, Err(Closed));
    assert_eq!(failure, Err("input validation failed"));
    assert_eq!(ready.phase(), 0);
    assert_eq!(resume.phase(), 0);
    println!("failure: peers observed Closed, not successful phase completion");
    Ok(())
}

async fn cancelling_an_unpolled_task_closes_the_group() -> Result<(), Closed> {
    let ready = Phaser::new();
    let resume = Phaser::new();
    let peer = Member::register(&ready, &resume)?;
    let cancelled = wait_once(Member::register(&ready, &resume)?);
    drop(cancelled);
    assert_eq!(wait_once(peer).await, Err(Closed));
    assert_eq!(ready.phase(), 0);
    println!("cancellation: dropping an unpolled task closed both gates");
    Ok(())
}

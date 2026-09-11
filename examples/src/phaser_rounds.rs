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

//! A start gate, changing membership, split arrival/wait, observers, and cancellation retry.
//!
//! Java mappings: register/bulkRegister become owned participant handles; arriveAndAwaitAdvance
//! becomes participant.wait; arrive/awaitAdvance become arrive/wait or an independent observer.
//! The setup participant prevents early workers from completing the initial phase before the
//! whole batch is registered. Unlike Java's default policy, an empty phaser stays reusable.
//!
//! Run: cargo run -p examples --example phaser_rounds

use asyncband::phaser::Closed;
use asyncband::phaser::Phaser;
use asyncband::phaser::PhaserParticipant;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Closed> {
    start_gate().await?;
    changing_membership().await?;
    cancellation_retry().await?;
    Ok(())
}

async fn start_gate() -> Result<(), Closed> {
    let phaser = Phaser::new();
    let setup = phaser.register()?;
    let mut tasks = Vec::new();
    for mut participant in phaser.register_many(3)? {
        tasks.push(tokio::spawn(async move {
            participant.wait().await?;
            // Initialization is complete; real work may now start.
            Ok::<_, Closed>(())
        }));
    }
    setup.deregister()?;
    for task in tasks {
        task.await.expect("worker panicked")?;
    }
    assert_eq!(phaser.registered_parties(), 0);
    assert!(!phaser.is_closed());
    println!("start gate: all three workers released; the empty phaser remains reusable");
    Ok(())
}

async fn work(mut participant: PhaserParticipant, rounds: usize) -> Result<(), Closed> {
    for _ in 0..rounds {
        let observed = participant.arrive()?;
        // This work does not hold up the other participants' arrivals.
        tokio::task::yield_now().await;
        let next = participant.wait().await?;
        assert_ne!(observed, next);
    }
    participant.deregister()?;
    Ok(())
}

async fn changing_membership() -> Result<(), Closed> {
    let phaser = Phaser::new();
    let mut coordinator = phaser.register()?;
    let worker = tokio::spawn(work(phaser.register()?, 3));

    let progress = phaser.clone();
    let observer = tokio::spawn(async move {
        let mut observed = progress.phase();
        while let Ok(next) = progress.wait_for_advance(observed).await {
            println!("observer: phase {observed} -> {next}");
            // A slow observer may skip phases; it never delays workers.
            observed = next;
        }
    });
    let target = phaser.clone();
    let target_wait = tokio::spawn(async move { wait_until(&target, 2).await });

    coordinator.wait().await?;
    // The coordinator has not arrived in the next phase, so this registration joins that phase.
    let joining_worker = tokio::spawn(work(phaser.register()?, 2));
    assert_eq!(phaser.registered_parties(), 3);
    coordinator.wait().await?;
    coordinator.wait().await?;

    worker.await.expect("worker panicked")?;
    joining_worker.await.expect("joining worker panicked")?;
    assert!(target_wait.await.expect("target observer panicked")? >= 2);
    coordinator.deregister()?;
    phaser.close();
    observer.await.expect("progress observer panicked");
    println!("dynamic membership: a second worker joined after the first round");
    Ok(())
}

/// A caller-side numeric threshold, for a run known not to cross counter wraparound.
/// Java's awaitPhase example uses the same loop over observed advances. This observer does not
/// register, drive the computation, or guarantee one notification for every intermediate phase.
async fn wait_until(phaser: &Phaser, target: u64) -> Result<u64, Closed> {
    let mut observed = phaser.phase();
    while observed < target {
        observed = phaser.wait_for_advance(observed).await?;
    }
    Ok(observed)
}

async fn cancellation_retry() -> Result<(), Closed> {
    let phaser = Phaser::new();
    let mut participant = phaser.register()?;
    let mut peer = phaser.register()?;
    let observed = phaser.phase();

    tokio::select! {
        biased;
        result = participant.wait() => panic!("peer has not arrived: {result:?}"),
        // Poll the wait once, then cancel it deterministically without a timer or sleep.
        _ = std::future::ready(()) => {}
    }
    assert_eq!(phaser.arrived_parties(), 1);
    peer.arrive()?;
    assert_ne!(phaser.phase(), observed);

    assert_eq!(participant.wait().await?, phaser.phase());
    assert_eq!(phaser.arrived_parties(), 0);
    println!("cancellation: retry observed the completed round without arriving in the next one");
    Ok(())
}

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

//! Run with `cargo run --package examples --example notify_vs_event`.
//!
//! Three notification needs: wake one worker, notify existing observers of a change, and keep a
//! readiness gate open. Tokio Notify combines the first two; ManualResetEvent expresses the third.
//! See https://docs.rs/tokio/1.53.1/tokio/sync/struct.Notify.html for Tokio's contracts.

use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use asyncband::event::AutoResetEvent;
use asyncband::event::ManualResetEvent;
use asyncband::watch;
use tokio::sync::Notify;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    wake_one_worker().await;
    broadcast_vs_readiness().await;
    broadcast_change_with_watch().await;
}

async fn wake_one_worker() {
    // A worker rechecks external work after waking. Repeated signals can coalesce while idle.
    // See coalesced_worker.rs for a complete worker using this pattern.
    let notify = Notify::new();
    notify.notify_one();
    notify.notify_one();
    notify.notified().await;
    assert!(poll_once(pin!(notify.notified())).is_pending());

    let event = AutoResetEvent::new();
    event.set();
    event.set();
    event.wait().await;
    assert!(!event.try_wait());
    println!("Wake one: both Notify and AutoResetEvent retain one unused signal");

    // This comparison assumes one consumer. Tokio also has Notified::enable() for registering
    // before checking an external queue; AutoResetEvent registers on first poll only.
}

async fn broadcast_vs_readiness() {
    let notify = Notify::new();
    let first = notify.notified();
    let second = notify.notified();
    notify.notify_waiters();
    // Both futures existed before the broadcast, so even these unpolled futures receive it.
    tokio::join!(first, second);
    assert!(poll_once(pin!(notify.notified())).is_pending()); // A late waiter misses it.

    let gate = ManualResetEvent::new();
    let mut registered = pin!(gate.wait());
    let mut unpolled = pin!(gate.wait());
    assert!(poll_once(registered.as_mut()).is_pending());
    gate.set();
    gate.reset();
    assert!(poll_once(registered.as_mut()).is_ready());
    assert!(poll_once(unpolled.as_mut()).is_pending());
    // A set/reset pulse misses an unpolled wait, so it cannot replace notify_waiters().

    gate.set();
    unpolled.await;
    gate.wait().await; // A late waiter also passes while the gate remains set.
    gate.reset();
    println!("Notify broadcasts once; ManualResetEvent stays ready until reset");
}

async fn broadcast_change_with_watch() {
    // For "the state changed; all existing observers should recheck", subscribe before checking
    // the external state. Each observer has its own progress, so they do not compete for a signal.
    let (changes, mut first) = watch::channel(());
    let mut second = changes.subscribe();
    changes.send(()).unwrap();
    let (a, b) = tokio::join!(first.changed(), second.changed());
    a.unwrap();
    b.unwrap();

    let mut late = changes.subscribe();
    assert!(poll_once(pin!(late.changed())).is_pending());
    println!("Broadcast change: watch<()> notifies each prior subscription; late ones wait");

    // This is an explicit subscription protocol: a retained receiver remembers unseen changes
    // across waits and cancellation. Tokio establishes a new boundary for each notified() future.
    // Repeated changes may coalesce. A fresh subscription starts observing from its creation.
}

// Poll once to show a wait stays pending, without hanging the example or relying on a timeout.
fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

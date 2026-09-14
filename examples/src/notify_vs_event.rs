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

//! Migrate two uses of Tokio Notify: wake a cache worker to flush queued entries, and let search
//! requests wait until an index reaches the revision they need. Each scenario has a Tokio version
//! followed by an Asyncband version with the same application behavior.
//!
//! Asyncband has no single primitive preserving Notify's combined notify_one/notify_waiters
//! contract on the same waiters. The migrations below separate worker and reader notifications.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::event::AutoResetEvent;
use asyncband::watch;
use tokio::sync::Notify;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    cache_worker_with_notify().await;
    cache_worker_with_event().await;
    index_readers_with_notify().await;
    index_readers_with_watch().await;
}

// One worker owns flushing. Producers keep the entries in a queue and notify it after enqueueing.
// This demonstration ends after three entries; a service would keep processing until shutdown.
async fn cache_worker_with_notify() {
    let entries = Mutex::new(VecDeque::new());
    let changed = Notify::new();
    let worker = async {
        let mut flushed = Vec::new();
        while flushed.len() < 3 {
            let entry = entries.lock().unwrap().pop_front();
            if let Some(entry) = entry {
                // A real worker writes this entry to storage here.
                flushed.push(entry);
            } else {
                changed.notified().await;
            }
        }
        flushed
    };
    let enqueue = async {
        for entry in ["alice", "bob", "carol"] {
            entries.lock().unwrap().push_back(entry);
            changed.notify_one();
        }
    };

    let (flushed, ()) = tokio::join!(biased; worker, enqueue);
    assert_eq!(flushed, ["alice", "bob", "carol"]);
    println!("Notify worker flushed {flushed:?}");
}

// Keep the queue and processing loop. Replace notify_one/notified with set/wait.
// Signals may coalesce, but entries do not: the worker drains the queue before waiting again.
// With competing consumers, migrate the queue to mpmc or redesign its registration protocol:
// AutoResetEvent has no counterpart to Notified::enable(), which registers before checking work.
async fn cache_worker_with_event() {
    let entries = Mutex::new(VecDeque::new());
    let changed = AutoResetEvent::new();
    let worker = async {
        let mut flushed = Vec::new();
        while flushed.len() < 3 {
            let entry = entries.lock().unwrap().pop_front();
            if let Some(entry) = entry {
                flushed.push(entry);
            } else {
                // With one consumer, an enqueue between the empty check and this wait leaves
                // a signal for us. Checking the queue again also handles leftover signals.
                changed.wait().await;
            }
        }
        flushed
    };
    let enqueue = async {
        for entry in ["alice", "bob", "carol"] {
            entries.lock().unwrap().push_back(entry);
            changed.set();
        }
    };

    let (flushed, ()) = tokio::join!(biased; worker, enqueue);
    assert_eq!(flushed, ["alice", "bob", "carol"]);
    println!("AutoResetEvent worker flushed {flushed:?}");
}

// A search request must wait for its writes to become searchable. Each request has a required
// index revision; publishing a revision wakes all requests so they can recheck their own target.
async fn index_readers_with_notify() {
    let indexed = AtomicUsize::new(0);
    let changed = Notify::new();
    let publish = async {
        for revision in 1..=2 {
            // The indexer finishes applying this revision before publishing it.
            indexed.store(revision, Ordering::Release);
            changed.notify_waiters();
            tokio::task::yield_now().await;
        }
    };

    let (first, second, ()) = tokio::join!(biased;
        wait_for_index_with_notify(&indexed, &changed, 1),
        wait_for_index_with_notify(&indexed, &changed, 2),
        publish,
    );
    assert!(first >= 1);
    assert!(second >= 2);
    // A late request succeeds by checking the index, even though it missed the notification.
    let late = wait_for_index_with_notify(&indexed, &changed, 2).await;
    assert_eq!(late, 2);
    println!("Notify readers reached revisions {first}, {second}, {late}");
}

async fn wait_for_index_with_notify(
    indexed: &AtomicUsize,
    changed: &Notify,
    required: usize,
) -> usize {
    loop {
        // Create the future BEFORE checking. notify_waiters reaches existing futures even if
        // they have not been polled, covering an update between the check and the await.
        let notified = changed.notified();
        let revision = indexed.load(Ordering::Acquire);
        if revision >= required {
            return revision;
        }
        notified.await;
    }
}

// Publish the indexed revision through watch, replacing both the atomic and the broadcast.
// Each request subscribes before checking and independently waits for its required revision.
// A retained receiver remembers unseen changes across cancelled waits. That suits a revision
// predicate; preserving Notify's per-wait broadcast boundary instead requires fresh subscriptions.
// A ManualResetEvent set/reset pulse would miss unpolled waits; leaving it set admits future waits.
async fn index_readers_with_watch() {
    let (indexed, mut first_request) = watch::channel(0);
    let mut second_request = indexed.subscribe();
    let publish = async {
        for revision in 1..=2 {
            // Publishing also works when there are temporarily no requests listening.
            indexed.send_replace(revision);
            tokio::task::yield_now().await;
        }
    };

    let (first, second, ()) = tokio::join!(biased;
        wait_for_index_with_watch(&mut first_request, 1),
        wait_for_index_with_watch(&mut second_request, 2),
        publish,
    );
    assert!(first >= 1);
    assert!(second >= 2);
    let mut late_request = indexed.subscribe();
    let late = wait_for_index_with_watch(&mut late_request, 2).await;
    assert_eq!(late, 2);
    println!("Watch readers reached revisions {first}, {second}, {late}");
}

async fn wait_for_index_with_watch(indexed: &mut watch::Receiver<usize>, required: usize) -> usize {
    loop {
        let revision = indexed.get();
        if revision >= required {
            return revision;
        }
        // The receiver remembers updates between get() and changed(), including ones published
        // before changed() is first polled. Intermediate revisions may coalesce; the target
        // predicate, rather than a notification count, determines when this request can proceed.
        indexed.changed().await.unwrap();
    }
}

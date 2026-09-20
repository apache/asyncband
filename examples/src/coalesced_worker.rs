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

//! One worker rebuilds a snapshot of the latest requested revision. Intermediate revisions may
//! coalesce: this is not a queue of jobs that must each run, or a broadcast to multiple observers.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::event::AutoResetEvent;

#[derive(Default)]
struct Rebuilder {
    requested: AtomicUsize,
    stopped: AtomicBool,
    changed: AutoResetEvent,
}

impl Rebuilder {
    fn request(&self, revision: usize) {
        self.requested.fetch_max(revision, Ordering::Release);
        self.changed.set();
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.changed.set();
    }

    async fn run(&self) {
        let mut rebuilt = 0;
        loop {
            // Observe stop before the revision so all requests preceding stop are included.
            let stopped = self.stopped.load(Ordering::Acquire);
            let requested = self.requested.load(Ordering::Acquire);
            if requested != rebuilt {
                println!("Rebuild snapshot at revision {requested}");
                rebuilt = requested;
            }
            if stopped {
                break;
            }
            // A request between the check and this wait leaves a consumable signal. There is
            // only one worker, so no other observer can consume that signal instead.
            self.changed.wait().await;
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let rebuilder = Rebuilder::default();
    tokio::join!(biased; rebuilder.run(), async {
        // The worker is already waiting. This burst still requires only the latest snapshot.
        rebuilder.request(1);
        rebuilder.request(2);
        rebuilder.request(3);
        tokio::task::yield_now().await;

        // After rebuilding revision 3, the worker waits for another update.
        rebuilder.request(4);
        rebuilder.stop();
    });
}

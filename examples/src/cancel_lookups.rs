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

//! Cancel redundant replica lookups after the first answer arrives.
//!
//! Cancellation belongs to this request, not the service's lifetime. Workers receive only an
//! observer; the caller owns cancellation authority and task handles. The signal does not wait
//! for workers to exit, so the caller joins them separately. A lookup that finishes before
//! observing cancellation may still return an answer.

use std::time::Duration;

use asyncband::cancellation::CancellationSource;
use asyncband::cancellation::CancellationToken;
use tokio::task::JoinSet;

async fn lookup(
    replica: &'static str,
    latency: Duration,
    token: CancellationToken,
) -> Option<&'static str> {
    // The timer stands in for caller-provided I/O; cancellation itself has no runtime dependency.
    tokio::select! {
        _ = token.cancelled() => None,
        _ = tokio::time::sleep(latency) => Some(replica),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let source = CancellationSource::new();
    let mut lookups = JoinSet::new();
    for (replica, millis) in [("nearby", 5), ("regional", 50), ("remote", 100)] {
        lookups.spawn(lookup(
            replica,
            Duration::from_millis(millis),
            source.token(),
        ));
    }

    let winner = lookups.join_next().await.unwrap().unwrap().unwrap();
    source.cancel();

    let mut cancelled = 0;
    while let Some(result) = lookups.join_next().await {
        if result.unwrap().is_none() {
            cancelled += 1;
        }
    }
    println!("Answer from {winner}; {cancelled} other lookups observed cancellation");
}

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

use std::sync::Arc;
use std::sync::Barrier;

use super::*;

#[tokio::test]
async fn concurrent_complete_calls_commit_exactly_one_value() {
    let (completer, completion) = channel();
    let completer = Arc::new(completer);
    let barrier = Arc::new(Barrier::new(3));

    let workers = [10, 20].map(|value| {
        let completer = completer.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            completer.complete(value)
        })
    });

    barrier.wait();
    let results = workers.map(|worker| worker.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);

    let rejected = results
        .into_iter()
        .find_map(Result::err)
        .expect("one completion must be rejected")
        .into_inner();
    let completed = *completion.wait().await.unwrap();
    assert_ne!(completed, rejected);
}

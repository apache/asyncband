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

//! Share one immutable job result with current and late observers.
//!
//! A oneshot channel has one receiver. Serving these independent consumers with oneshot would
//! require one channel per consumer and manual fan-out, and a late consumer would need separate
//! result storage. Completion retains the result and exposes it to every observer.

use asyncband::completion;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (completer, completion) = completion::channel();
    let dashboard = completion.clone();
    let audit_log = completion.clone();

    let worker = tokio::spawn(async move {
        tokio::task::yield_now().await;
        completer
            .complete(String::from("build 42 succeeded"))
            .unwrap();
    });

    let (dashboard_result, audit_result) = tokio::join!(dashboard.wait(), audit_log.wait());
    let dashboard_result = dashboard_result.unwrap();
    let audit_result = audit_result.unwrap();
    worker.await.unwrap();

    let late_observer = completion.clone();
    let late_result = late_observer.wait().await.unwrap();

    assert!(std::ptr::eq(dashboard_result, audit_result));
    assert!(std::ptr::eq(dashboard_result, late_result));
    println!("{late_result}");
}

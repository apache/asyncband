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

use std::future::IntoFuture;
use std::time::Duration;

use asyncband::waitgroup::WaitGroup;

#[path = "support/runtime.rs"]
mod runtime_support;
use runtime_support::test_runtime;

#[test]
fn test_wait_group_drop() {
    let wg = WaitGroup::new();
    for _i in 0..100 {
        let w = wg.clone();
        test_runtime().spawn(async move {
            drop(w);
        });
    }
    pollster::block_on(wg.into_future());
}

#[test]
fn test_wait_group_await() {
    let wg = WaitGroup::new();
    for _i in 0..100 {
        let w = wg.clone();
        test_runtime().spawn(async move {
            w.await;
        });
    }
    pollster::block_on(wg.into_future());
}

#[test]
fn test_wait_group_timeout() {
    let wg = WaitGroup::new();
    let _wg_clone = wg.clone();
    let timeout = test_runtime().block_on(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(50)) => true ,
            _ = wg => false,
        }
    });
    assert!(timeout);
}

#[test]
fn test_wait_group_cancel() {
    let wg = WaitGroup::new();
    let wg_clone = wg.clone().into_future();
    let wg_clone_2 = wg.clone().into_future();
    test_runtime().block_on(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::ZERO) => {},
            _ = wg_clone => {}
        }
    });
    let fut = test_runtime().spawn(async move {
        wg_clone_2.await;
    });
    std::thread::sleep(Duration::from_millis(50));
    drop(wg);
    let timeout = test_runtime().block_on(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(60)) => true ,
            _ = fut => false,
        }
    });
    assert!(!timeout);
}

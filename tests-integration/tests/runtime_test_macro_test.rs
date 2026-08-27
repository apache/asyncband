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

use tests_integration::runtime_test;

#[runtime_test(only(compio))]
async fn runtime_specific_test() {
    runtime::spawn(async {}).await.unwrap();
}

#[runtime_test]
async fn spawn_uses_a_worker_thread() {
    let caller = std::thread::current().id();
    let worker = runtime::spawn(async { std::thread::current().id() })
        .await
        .unwrap();

    assert_ne!(caller, worker);
}

#[runtime_test]
async fn spawn_local_stays_on_the_current_thread() {
    let caller = std::thread::current().id();
    let local_value = std::rc::Rc::new(());
    let worker = runtime::spawn_local(async move {
        runtime::yield_once().await;
        assert_eq!(std::rc::Rc::strong_count(&local_value), 1);
        std::thread::current().id()
    })
    .await
    .unwrap();

    assert_eq!(caller, worker);
}

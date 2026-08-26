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

use asyncband::once::LazyCell;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // The library stores exactly the future returned by the initializer. Explicitly boxing that
    // future keeps the cell movable, without requiring dynamic dispatch.
    let boxed = LazyCell::new(|| Box::pin(async { "boxed".to_owned() }));
    assert_eq!(LazyCell::force(&boxed).await, "boxed");

    // Returning the async future directly stores it inline. The caller pins the cell before the
    // first poll, so the future stays at a stable address without a heap allocation.
    let inline = LazyCell::new(async || "inline".to_owned());
    let mut inline = std::pin::pin!(inline);
    assert_eq!(LazyCell::force_pin(inline.as_ref()).await, "inline");

    LazyCell::force_pin_mut(inline.as_mut())
        .await
        .push_str(" future");
    assert_eq!(
        LazyCell::get(&inline).expect("value is initialized"),
        "inline future"
    );

    // Arc::pin allocates the cell once and keeps its inline future at a stable address. Cloning the
    // pinned Arc lets multiple tasks resume the same future without a separate future allocation.
    let shared = Arc::pin(LazyCell::new(async || {
        tokio::task::yield_now().await;
        "shared".to_owned()
    }));

    let first = tokio::spawn({
        let shared = shared.clone();
        async move { LazyCell::force_pin(shared.as_ref()).await.clone() }
    });
    let second = tokio::spawn({
        let shared = shared.clone();
        async move { LazyCell::force_pin(shared.as_ref()).await.clone() }
    });

    assert_eq!(first.await.unwrap(), "shared");
    assert_eq!(second.await.unwrap(), "shared");
}

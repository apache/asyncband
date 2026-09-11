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

use divan::black_box;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use super::adapters::Channel;
pub use crate::mpmc_support::support::BATCH_MESSAGES;
pub use crate::mpmc_support::support::runtime;

pub const CONSUMERS: &[usize] = &[1, 2, 4, 8];

pub struct TaskBatch {
    start: Arc<tokio::sync::Barrier>,
    tasks: JoinSet<(usize, usize)>,
}

impl TaskBatch {
    pub fn new<C: Channel>(runtime: &Runtime, consumers: usize) -> Self {
        let (mut sender, receiver) = C::channel();
        let start = Arc::new(tokio::sync::Barrier::new(consumers + 2));
        let mut tasks = JoinSet::new();
        let producer_start = start.clone();
        tasks.spawn_on(
            async move {
                producer_start.wait().await;
                for value in 0..BATCH_MESSAGES {
                    C::send(&mut sender, black_box(value)).await;
                }
                // The sender is moved once and dropped on completion so consumers can drain.
                (0, 0)
            },
            runtime.handle(),
        );
        for _ in 0..consumers {
            let receiver = receiver.clone();
            let start = start.clone();
            tasks.spawn_on(
                async move {
                    start.wait().await;
                    let mut count = 0;
                    let mut checksum = 0usize;
                    // Consumers compete freely, with no fixed per-consumer quota.
                    while let Some(value) = C::recv(&receiver).await {
                        count += 1;
                        checksum = checksum.wrapping_add(value);
                    }
                    (count, checksum)
                },
                runtime.handle(),
            );
        }
        drop(receiver);
        Self { start, tasks }
    }

    pub async fn run(&mut self) -> (usize, usize) {
        self.start.wait().await;
        let mut count = 0;
        let mut checksum = 0usize;
        while let Some(result) = self.tasks.join_next().await {
            let (received, sum) = result.expect("benchmark task panicked");
            count += received;
            checksum = checksum.wrapping_add(sum);
        }
        assert_eq!(count, BATCH_MESSAGES);
        assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
        black_box((count, checksum))
    }
}

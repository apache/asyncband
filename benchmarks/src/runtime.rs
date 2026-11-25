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

use std::future::Future;
use std::hint::black_box;

use tokio::runtime::Builder;
use tokio::runtime::Runtime;

pub const BATCH_MESSAGES: usize = 16_384;
pub const BROADCAST_MESSAGES: usize = 4_096;
pub const BOUNDED_CAPACITY: usize = 64;
pub const WORKERS: usize = 4;

/// Builds a single-threaded Tokio runtime for task-scheduled benchmarks.
pub fn current_thread() -> Runtime {
    Builder::new_current_thread().build().unwrap()
}

/// Builds a multi-threaded Tokio runtime with `workers` worker threads.
pub fn multi_thread(workers: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap()
}

/// Returns the expected sum of `0..messages`.
pub fn expected_checksum(messages: usize) -> usize {
    if messages == 0 {
        0
    } else {
        messages * (messages - 1) / 2
    }
}

/// Runs one point-to-point channel batch across `senders` and `receivers` on `runtime`.
pub fn run_queue_batch<S, R, SendFut, RecvFut>(
    runtime: &Runtime,
    total_messages: usize,
    senders: Vec<S>,
    receivers: Vec<R>,
    send_range: fn(S, usize, usize) -> SendFut,
    drain_recv: fn(R) -> RecvFut,
) where
    S: Send + 'static,
    R: Send + 'static,
    SendFut: Future<Output = ()> + Send + 'static,
    RecvFut: Future<Output = (usize, usize)> + Send + 'static,
{
    assert!(!senders.is_empty(), "need at least one sender");
    assert!(!receivers.is_empty(), "need at least one receiver");
    assert_eq!(
        total_messages % senders.len(),
        0,
        "total_messages must divide evenly among senders"
    );

    let per_sender = total_messages / senders.len();
    let mut recv_handles = Vec::with_capacity(receivers.len());
    for receiver in receivers {
        recv_handles.push(runtime.spawn(drain_recv(receiver)));
    }
    let mut send_handles = Vec::with_capacity(senders.len());
    for (index, sender) in senders.into_iter().enumerate() {
        let start = index * per_sender;
        let end = start + per_sender;
        send_handles.push(runtime.spawn(send_range(sender, start, end)));
    }

    let (count, checksum) = runtime.block_on(async {
        for handle in send_handles {
            handle.await.expect("benchmark sender task panicked");
        }
        let mut count = 0usize;
        let mut checksum = 0usize;
        for handle in recv_handles {
            let (received, sum) = handle.await.expect("benchmark receiver task panicked");
            count += received;
            checksum = checksum.wrapping_add(sum);
        }
        (count, checksum)
    });

    assert_eq!(count, total_messages);
    assert_eq!(checksum, expected_checksum(total_messages));
    black_box((count, checksum));
}

/// Runs one broadcast batch across `senders` and `receivers` on `runtime`.
pub fn run_broadcast_batch<S, R, SendFut, RecvFut>(
    runtime: &Runtime,
    total_messages: usize,
    senders: Vec<S>,
    receivers: Vec<R>,
    send_range: fn(S, usize, usize) -> SendFut,
    recv_exact: fn(R, usize) -> RecvFut,
) where
    S: Send + 'static,
    R: Send + 'static,
    SendFut: Future<Output = ()> + Send + 'static,
    RecvFut: Future<Output = usize> + Send + 'static,
{
    assert!(!senders.is_empty(), "need at least one sender");
    assert!(!receivers.is_empty(), "need at least one receiver");
    assert_eq!(
        total_messages % senders.len(),
        0,
        "total_messages must divide evenly among senders"
    );

    let per_sender = total_messages / senders.len();
    let mut recv_handles = Vec::with_capacity(receivers.len());
    for receiver in receivers {
        recv_handles.push(runtime.spawn(recv_exact(receiver, total_messages)));
    }
    let mut send_handles = Vec::with_capacity(senders.len());
    for (index, sender) in senders.into_iter().enumerate() {
        let start = index * per_sender;
        let end = start + per_sender;
        send_handles.push(runtime.spawn(send_range(sender, start, end)));
    }

    let expected = expected_checksum(total_messages);
    runtime.block_on(async {
        for handle in send_handles {
            handle.await.expect("benchmark sender task panicked");
        }
        for handle in recv_handles {
            let checksum = handle.await.expect("benchmark receiver task panicked");
            assert_eq!(checksum, expected);
            black_box(checksum);
        }
    });
}

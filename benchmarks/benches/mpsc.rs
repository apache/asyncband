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
use std::pin::pin;
use std::task::Context;

use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use benchmarks::runtime::BATCH_MESSAGES;
use benchmarks::runtime::BOUNDED_CAPACITY;
use benchmarks::runtime::WORKERS;
use benchmarks::runtime::current_thread;
use benchmarks::runtime::expected_checksum;
use benchmarks::runtime::multi_thread;
use benchmarks::runtime::run_queue_batch;

trait MpscPeer {
    type BoundedTx<T: Send + 'static>: Send + Sync + Clone + 'static;
    type BoundedRx<T: Send + 'static>: Send + 'static;
    type UnboundedTx<T: Send + 'static>: Send + Sync + Clone + 'static;
    type UnboundedRx<T: Send + 'static>: Send + 'static;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>);

    fn try_send<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T);

    fn try_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>) -> T;

    fn send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
    ) -> impl Future<Output = ()> + Send;

    fn poll_send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
        cx: &mut Context<'_>,
    );

    fn poll_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>, cx: &mut Context<'_>)
    -> T;

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>);

    fn send_unbounded<T: Send + 'static>(tx: &Self::UnboundedTx<T>, value: T);

    fn try_recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T;

    fn recv_unbounded<T: Send + 'static>(
        rx: &mut Self::UnboundedRx<T>,
    ) -> impl Future<Output = T> + Send;

    fn send_range_bounded(
        tx: Self::BoundedTx<usize>,
        start: usize,
        end: usize,
    ) -> impl Future<Output = ()> + Send + 'static;

    fn drain_recv_bounded(
        rx: Self::BoundedRx<usize>,
    ) -> impl Future<Output = (usize, usize)> + Send + 'static;

    fn send_range_unbounded(
        tx: Self::UnboundedTx<usize>,
        start: usize,
        end: usize,
    ) -> impl Future<Output = ()> + Send + 'static;

    fn drain_recv_unbounded(
        rx: Self::UnboundedRx<usize>,
    ) -> impl Future<Output = (usize, usize)> + Send + 'static;
}

trait ReserveMpscPeer: MpscPeer {
    fn poll_reserve_send(tx: &Self::BoundedTx<usize>, value: usize, cx: &mut Context<'_>);
}

struct Asyncband;

impl MpscPeer for Asyncband {
    type BoundedTx<T: Send + 'static> = asyncband::mpsc::BoundedSender<T>;
    type BoundedRx<T: Send + 'static> = asyncband::mpsc::BoundedReceiver<T>;
    type UnboundedTx<T: Send + 'static> = asyncband::mpsc::UnboundedSender<T>;
    type UnboundedRx<T: Send + 'static> = asyncband::mpsc::UnboundedReceiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        asyncband::mpsc::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn send_bounded<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.send(value).await.unwrap();
    }

    #[inline]
    fn poll_send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
        cx: &mut Context<'_>,
    ) {
        poll_ready(tx.send(value), cx).unwrap();
    }

    #[inline]
    fn poll_recv_bounded<T: Send + 'static>(
        rx: &mut Self::BoundedRx<T>,
        cx: &mut Context<'_>,
    ) -> T {
        poll_ready(rx.recv(), cx).unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        asyncband::mpsc::unbounded()
    }

    #[inline]
    fn send_unbounded<T: Send + 'static>(tx: &Self::UnboundedTx<T>, value: T) {
        tx.send(value).unwrap();
    }

    #[inline]
    fn try_recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.recv().await.unwrap()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(mut rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(mut rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }
}

impl ReserveMpscPeer for Asyncband {
    #[inline]
    fn poll_reserve_send(tx: &Self::BoundedTx<usize>, value: usize, cx: &mut Context<'_>) {
        let permit = poll_ready(tx.reserve(), cx).unwrap();
        permit.send(value).unwrap();
    }
}

struct Tokio;

impl MpscPeer for Tokio {
    type BoundedTx<T: Send + 'static> = tokio::sync::mpsc::Sender<T>;
    type BoundedRx<T: Send + 'static> = tokio::sync::mpsc::Receiver<T>;
    type UnboundedTx<T: Send + 'static> = tokio::sync::mpsc::UnboundedSender<T>;
    type UnboundedRx<T: Send + 'static> = tokio::sync::mpsc::UnboundedReceiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        tokio::sync::mpsc::channel(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn send_bounded<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.send(value).await.unwrap();
    }

    #[inline]
    fn poll_send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
        cx: &mut Context<'_>,
    ) {
        poll_ready(tx.send(value), cx).unwrap();
    }

    #[inline]
    fn poll_recv_bounded<T: Send + 'static>(
        rx: &mut Self::BoundedRx<T>,
        cx: &mut Context<'_>,
    ) -> T {
        poll_ready(rx.recv(), cx).unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        tokio::sync::mpsc::unbounded_channel()
    }

    #[inline]
    fn send_unbounded<T: Send + 'static>(tx: &Self::UnboundedTx<T>, value: T) {
        tx.send(value).unwrap();
    }

    #[inline]
    fn try_recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.recv().await.unwrap()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(mut rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Some(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(mut rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Some(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }
}

impl ReserveMpscPeer for Tokio {
    #[inline]
    fn poll_reserve_send(tx: &Self::BoundedTx<usize>, value: usize, cx: &mut Context<'_>) {
        let permit = poll_ready(tx.reserve(), cx).unwrap();
        permit.send(value);
    }
}

struct AsyncChannel;

impl MpscPeer for AsyncChannel {
    type BoundedTx<T: Send + 'static> = async_channel::Sender<T>;
    type BoundedRx<T: Send + 'static> = async_channel::Receiver<T>;
    type UnboundedTx<T: Send + 'static> = async_channel::Sender<T>;
    type UnboundedRx<T: Send + 'static> = async_channel::Receiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        async_channel::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn send_bounded<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.send(value).await.unwrap();
    }

    #[inline]
    fn poll_send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
        cx: &mut Context<'_>,
    ) {
        poll_ready(tx.send(value), cx).unwrap();
    }

    #[inline]
    fn poll_recv_bounded<T: Send + 'static>(
        rx: &mut Self::BoundedRx<T>,
        cx: &mut Context<'_>,
    ) -> T {
        poll_ready(rx.recv(), cx).unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        async_channel::unbounded()
    }

    #[inline]
    fn send_unbounded<T: Send + 'static>(tx: &Self::UnboundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.recv().await.unwrap()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.try_send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        Self::drain_recv_bounded(rx).await
    }
}

struct Flume;

impl MpscPeer for Flume {
    type BoundedTx<T: Send + 'static> = flume::Sender<T>;
    type BoundedRx<T: Send + 'static> = flume::Receiver<T>;
    type UnboundedTx<T: Send + 'static> = flume::Sender<T>;
    type UnboundedRx<T: Send + 'static> = flume::Receiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        flume::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &mut Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn send_bounded<T: Send + 'static>(tx: &Self::BoundedTx<T>, value: T) {
        tx.send_async(value).await.unwrap();
    }

    #[inline]
    fn poll_send_bounded<T: Send + 'static>(
        tx: &Self::BoundedTx<T>,
        value: T,
        cx: &mut Context<'_>,
    ) {
        poll_ready(tx.send_async(value), cx).unwrap();
    }

    #[inline]
    fn poll_recv_bounded<T: Send + 'static>(
        rx: &mut Self::BoundedRx<T>,
        cx: &mut Context<'_>,
    ) -> T {
        poll_ready(rx.recv_async(), cx).unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        flume::unbounded()
    }

    #[inline]
    fn send_unbounded<T: Send + 'static>(tx: &Self::UnboundedTx<T>, value: T) {
        tx.send(value).unwrap();
    }

    #[inline]
    fn try_recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    #[inline]
    async fn recv_unbounded<T: Send + 'static>(rx: &mut Self::UnboundedRx<T>) -> T {
        rx.recv_async().await.unwrap()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send_async(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv_async().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        Self::drain_recv_bounded(rx).await
    }
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpsc/bounded/try-roundtrip",
        75_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::bounded::<usize>(BOUNDED_CAPACITY);
            bencher.bench_local(|| {
                M::try_send(&tx, black_box(1usize));
                M::try_recv_bounded(&mut rx)
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/ready-roundtrip",
        45_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::bounded::<usize>(BOUNDED_CAPACITY);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                M::poll_send_bounded(&tx, black_box(1usize), &mut cx);
                M::poll_recv_bounded(&mut rx, &mut cx)
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/ready-roundtrip-1k",
        3_200_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::bounded::<[u8; 1024]>(BOUNDED_CAPACITY);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                M::poll_send_bounded(&tx, black_box([7u8; 1024]), &mut cx);
                let value = M::poll_recv_bounded(&mut rx, &mut cx);
                black_box(&value);
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/reserve-roundtrip",
        65_000_000,
        implementations!([Asyncband, Tokio], |M, bencher| {
            let (tx, mut rx) = M::bounded::<usize>(BOUNDED_CAPACITY);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                M::poll_reserve_send(&tx, black_box(1usize), &mut cx);
                M::try_recv_bounded(&mut rx)
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/backpressure-handoff",
        12_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::bounded::<usize>(1);
            M::try_send(&tx, 0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut send = pin!(M::send_bounded(&tx, black_box(1usize)));
                poll_pending(send.as_mut(), &mut cx);
                let drained = M::try_recv_bounded(&mut rx);
                poll_pinned_ready(send.as_mut(), &mut cx);
                drained
            });
        }),
    );

    runner.compare(
        "mpsc/unbounded/wake-receiver",
        15_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::unbounded::<usize>();
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut recv = pin!(M::recv_unbounded(&mut rx));
                poll_pending(recv.as_mut(), &mut cx);
                M::send_unbounded(&tx, black_box(1usize));
                poll_pinned_ready(recv.as_mut(), &mut cx)
            });
        }),
    );

    let expected = expected_checksum(BATCH_MESSAGES);

    runner.compare(
        "mpsc/bounded/seq-batch",
        3_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::bounded::<usize>(BATCH_MESSAGES);
            bencher.bench_local(|| {
                for value in 0..BATCH_MESSAGES {
                    M::try_send(&tx, value);
                }
                let mut sum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    sum = sum.wrapping_add(M::try_recv_bounded(&mut rx));
                }
                assert_eq!(sum, expected);
                sum
            });
        }),
    );

    runner.compare(
        "mpsc/unbounded/seq-batch",
        2_400,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::unbounded::<usize>();
            bencher.bench_local(|| {
                for value in 0..BATCH_MESSAGES {
                    M::send_unbounded(&tx, value);
                }
                let mut sum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    sum = sum.wrapping_add(M::try_recv_unbounded(&mut rx));
                }
                assert_eq!(sum, expected);
                sum
            });
        }),
    );

    runner.compare(
        "mpsc/unbounded/seq-batch-backlog",
        2_400,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let (tx, mut rx) = M::unbounded::<usize>();
            // Pre-fill the upper half so every round drains a full 0..BATCH_MESSAGES checksum
            // while leaving `BATCH_MESSAGES / 2` items buffered for the next round.
            for value in (BATCH_MESSAGES / 2)..BATCH_MESSAGES {
                M::send_unbounded(&tx, value);
            }
            bencher.bench_local(|| {
                for value in 0..BATCH_MESSAGES {
                    M::send_unbounded(&tx, value);
                }
                let mut sum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    sum = sum.wrapping_add(M::try_recv_unbounded(&mut rx));
                }
                assert_eq!(sum, expected);
                sum
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/tasks-1p1c-cap64",
        1_800,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(64);
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    vec![tx],
                    vec![rx],
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/tasks-1p1c-cap64-current",
        3_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = current_thread();
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(64);
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    vec![tx],
                    vec![rx],
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/tasks-4p1c-cap1",
        180,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(1);
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    vec![rx],
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/tasks-4p1c-cap64",
        450,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(64);
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    vec![rx],
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpsc/bounded/tasks-4p1c-cap16384",
        200,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(BATCH_MESSAGES);
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    vec![rx],
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpsc/unbounded/tasks-4p1c",
        750,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::unbounded::<usize>();
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    vec![rx],
                    M::send_range_unbounded,
                    M::drain_recv_unbounded,
                );
            });
        }),
    );
}

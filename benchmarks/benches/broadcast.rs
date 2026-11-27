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
use std::sync::Arc;

use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::runtime::BOUNDED_CAPACITY;
use benchmarks::runtime::BROADCAST_MESSAGES;
use benchmarks::runtime::WORKERS;
use benchmarks::runtime::multi_thread;
use benchmarks::runtime::run_broadcast_batch;

trait BroadcastPeer {
    type Tx: Send + Sync + Clone + 'static;
    type Rx: Send + 'static;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx);

    fn subscribe(tx: &Self::Tx) -> Self::Rx;

    fn try_send(tx: &Self::Tx, value: usize);

    fn try_recv(rx: &mut Self::Rx) -> usize;

    fn send_range(
        tx: Self::Tx,
        start: usize,
        end: usize,
    ) -> impl Future<Output = ()> + Send + 'static;

    fn recv_exact(rx: Self::Rx, count: usize) -> impl Future<Output = usize> + Send + 'static;
}

trait BackpressureBroadcastPeer: BroadcastPeer {
    fn send(tx: &Self::Tx, value: usize) -> impl Future<Output = ()> + Send;
}

struct AsyncbandBounded;

impl BroadcastPeer for AsyncbandBounded {
    type Tx = asyncband::broadcast::mpmc::BoundedSender<usize>;
    type Rx = asyncband::broadcast::mpmc::BoundedReceiver<usize>;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx) {
        asyncband::broadcast::mpmc::bounded(capacity)
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.subscribe()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        *rx.try_recv().unwrap()
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await;
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(*rx.recv().await.unwrap());
        }
        sum
    }
}

impl BackpressureBroadcastPeer for AsyncbandBounded {
    #[inline]
    async fn send(tx: &Self::Tx, value: usize) {
        tx.send(value).await;
    }
}

struct AsyncbandUnbounded;

impl BroadcastPeer for AsyncbandUnbounded {
    type Tx = asyncband::broadcast::mpmc::UnboundedSender<usize>;
    type Rx = asyncband::broadcast::mpmc::UnboundedReceiver<usize>;

    fn channel(_capacity: usize) -> (Self::Tx, Self::Rx) {
        asyncband::broadcast::mpmc::unbounded()
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.subscribe()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.send(value);
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        *rx.try_recv().unwrap()
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.send(value);
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(*rx.recv().await.unwrap());
        }
        sum
    }
}

struct AsyncBroadcast;

impl BroadcastPeer for AsyncBroadcast {
    type Tx = async_broadcast::Sender<usize>;
    type Rx = async_broadcast::Receiver<usize>;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx) {
        async_broadcast::broadcast(capacity)
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.new_receiver()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.try_broadcast(value).unwrap();
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        rx.try_recv().unwrap()
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.broadcast_direct(value).await.unwrap();
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(rx.recv_direct().await.unwrap());
        }
        sum
    }
}

impl BackpressureBroadcastPeer for AsyncBroadcast {
    #[inline]
    async fn send(tx: &Self::Tx, value: usize) {
        tx.broadcast_direct(value).await.unwrap();
    }
}

/// Wraps each message in `Arc<usize>` so `async-broadcast` matches Asyncband's shared `Arc<T>`
/// delivery contract.
struct AsyncBroadcastShared;

impl BroadcastPeer for AsyncBroadcastShared {
    type Tx = async_broadcast::Sender<Arc<usize>>;
    type Rx = async_broadcast::Receiver<Arc<usize>>;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx) {
        async_broadcast::broadcast(capacity)
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.new_receiver()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.try_broadcast(Arc::new(value)).unwrap();
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        *black_box(rx.try_recv().unwrap())
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.broadcast_direct(Arc::new(value)).await.unwrap();
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(*black_box(rx.recv_direct().await.unwrap()));
        }
        sum
    }
}

struct Tokio;

impl BroadcastPeer for Tokio {
    type Tx = tokio::sync::broadcast::Sender<usize>;
    type Rx = tokio::sync::broadcast::Receiver<usize>;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx) {
        tokio::sync::broadcast::channel(capacity)
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.subscribe()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.send(value).unwrap();
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        rx.try_recv().unwrap()
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(rx.recv().await.unwrap());
        }
        sum
    }
}

/// Wraps each message in `Arc<usize>` so `tokio::sync::broadcast` matches Asyncband's shared
/// `Arc<T>` delivery contract.
struct TokioShared;

impl BroadcastPeer for TokioShared {
    type Tx = tokio::sync::broadcast::Sender<Arc<usize>>;
    type Rx = tokio::sync::broadcast::Receiver<Arc<usize>>;

    fn channel(capacity: usize) -> (Self::Tx, Self::Rx) {
        tokio::sync::broadcast::channel(capacity)
    }

    fn subscribe(tx: &Self::Tx) -> Self::Rx {
        tx.subscribe()
    }

    #[inline]
    fn try_send(tx: &Self::Tx, value: usize) {
        tx.send(Arc::new(value)).unwrap();
    }

    #[inline]
    fn try_recv(rx: &mut Self::Rx) -> usize {
        *black_box(rx.try_recv().unwrap())
    }

    async fn send_range(tx: Self::Tx, start: usize, end: usize) {
        for value in start..end {
            tx.send(Arc::new(value)).unwrap();
        }
    }

    async fn recv_exact(mut rx: Self::Rx, count: usize) -> usize {
        let mut sum = 0usize;
        for _ in 0..count {
            sum = sum.wrapping_add(*black_box(rx.recv().await.unwrap()));
        }
        sum
    }
}

fn bench_try_fanout_4<B: BroadcastPeer>(bencher: benchmarks::harness::Bencher<'_>) {
    let (tx, r0) = B::channel(BOUNDED_CAPACITY);
    let mut receivers = [r0, B::subscribe(&tx), B::subscribe(&tx), B::subscribe(&tx)];
    bencher.bench_local(|| {
        B::try_send(&tx, black_box(1usize));
        let mut sum = 0usize;
        for rx in &mut receivers {
            sum = sum.wrapping_add(B::try_recv(rx));
        }
        sum
    });
}

fn bench_no_backpressure_tasks_1p4r<B: BroadcastPeer>(bencher: benchmarks::harness::Bencher<'_>) {
    let runtime = multi_thread(WORKERS);
    bencher.bench_local(|| {
        let (tx, r0) = B::channel(BROADCAST_MESSAGES);
        let receivers = vec![r0, B::subscribe(&tx), B::subscribe(&tx), B::subscribe(&tx)];
        run_broadcast_batch(
            &runtime,
            BROADCAST_MESSAGES,
            vec![tx],
            receivers,
            B::send_range,
            B::recv_exact,
        );
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "broadcast/bounded/try-fanout-4",
        6_500_000,
        implementations!(
            [AsyncbandBounded, AsyncbandUnbounded, AsyncBroadcast, Tokio],
            |B, bencher| bench_try_fanout_4::<B>(bencher)
        ),
    );

    runner.compare(
        "broadcast/bounded/try-fanout-4-shared",
        6_500_000,
        implementations!(
            [
                AsyncbandBounded,
                AsyncbandUnbounded,
                AsyncBroadcastShared,
                TokioShared,
            ],
            |B, bencher| bench_try_fanout_4::<B>(bencher)
        ),
    );

    runner.compare(
        "broadcast/bounded/backpressure-handoff",
        6_000_000,
        implementations!([AsyncbandBounded, AsyncBroadcast], |B, bencher| {
            let (tx, mut rx) = B::channel(1);
            B::try_send(&tx, 0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut send = pin!(B::send(&tx, black_box(1usize)));
                poll_pending(send.as_mut(), &mut cx);
                let drained = B::try_recv(&mut rx);
                poll_pinned_ready(send.as_mut(), &mut cx);
                drained
            });
        }),
    );

    runner.local("broadcast/unbounded/wake-fanout-4", 4_000_000, |bencher| {
        let (tx, r0) = asyncband::broadcast::mpmc::unbounded::<usize>();
        let mut receivers = [r0, tx.subscribe(), tx.subscribe(), tx.subscribe()];
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let [r0, r1, r2, r3] = &mut receivers;
            let mut f0 = pin!(r0.recv());
            let mut f1 = pin!(r1.recv());
            let mut f2 = pin!(r2.recv());
            let mut f3 = pin!(r3.recv());
            poll_pending(f0.as_mut(), &mut cx);
            poll_pending(f1.as_mut(), &mut cx);
            poll_pending(f2.as_mut(), &mut cx);
            poll_pending(f3.as_mut(), &mut cx);
            tx.send(black_box(1usize));
            black_box((
                *poll_pinned_ready(f0.as_mut(), &mut cx).unwrap(),
                *poll_pinned_ready(f1.as_mut(), &mut cx).unwrap(),
                *poll_pinned_ready(f2.as_mut(), &mut cx).unwrap(),
                *poll_pinned_ready(f3.as_mut(), &mut cx).unwrap(),
            ))
        });
    });

    runner.local("broadcast/unbounded/try-roundtrip", 19_000_000, |bencher| {
        let (tx, mut rx) = asyncband::broadcast::mpmc::unbounded::<usize>();
        bencher.bench_local(|| {
            tx.send(black_box(1usize));
            *rx.try_recv().unwrap()
        });
    });

    runner.compare(
        "broadcast/bounded/tasks-1p4r-cap64",
        500,
        implementations!([AsyncbandBounded, AsyncBroadcast], |B, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, r0) = B::channel(64);
                let receivers = vec![r0, B::subscribe(&tx), B::subscribe(&tx), B::subscribe(&tx)];
                run_broadcast_batch(
                    &runtime,
                    BROADCAST_MESSAGES,
                    vec![tx],
                    receivers,
                    B::send_range,
                    B::recv_exact,
                );
            });
        }),
    );

    runner.compare(
        "broadcast/bounded/tasks-4p4r-cap1",
        160,
        implementations!([AsyncbandBounded, AsyncBroadcast], |B, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, r0) = B::channel(1);
                let receivers = vec![r0, B::subscribe(&tx), B::subscribe(&tx), B::subscribe(&tx)];
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                run_broadcast_batch(
                    &runtime,
                    BROADCAST_MESSAGES,
                    senders,
                    receivers,
                    B::send_range,
                    B::recv_exact,
                );
            });
        }),
    );

    runner.compare(
        "broadcast/no-backpressure/tasks-1p4r",
        350,
        implementations!(
            [AsyncbandBounded, AsyncbandUnbounded, Tokio, AsyncBroadcast],
            |B, bencher| bench_no_backpressure_tasks_1p4r::<B>(bencher)
        ),
    );

    runner.compare(
        "broadcast/no-backpressure/tasks-1p4r-shared",
        350,
        implementations!(
            [
                AsyncbandBounded,
                AsyncbandUnbounded,
                TokioShared,
                AsyncBroadcastShared,
            ],
            |B, bencher| bench_no_backpressure_tasks_1p4r::<B>(bencher)
        ),
    );
}

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
use std::future::IntoFuture;
use std::hint::black_box;
use std::pin::pin;

use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use benchmarks::runtime::WORKERS;
use benchmarks::runtime::current_thread;
use benchmarks::runtime::multi_thread;

const REQUESTS_PER_ROUND: usize = 1_024;

trait OneshotPeer {
    type OneshotTx<T: Send + 'static>: Send + 'static;
    type OneshotRx<T: Send + 'static>: IntoFuture<Output = Self::RecvOut<T>, IntoFuture: Send>
        + Send
        + 'static;
    type RecvOut<T: Send + 'static>;
    type MpscTx: Send + Sync + 'static;
    type MpscRx: Send + 'static;

    fn oneshot<T: Send + 'static>() -> (Self::OneshotTx<T>, Self::OneshotRx<T>);

    fn send_oneshot<T: Send + 'static>(tx: Self::OneshotTx<T>, value: T);

    fn unwrap_oneshot<T: Send + 'static>(out: Self::RecvOut<T>) -> T;

    fn mpsc_bounded(capacity: usize) -> (Self::MpscTx, Self::MpscRx);

    fn send_req(
        tx: &Self::MpscTx,
        reply: Self::OneshotTx<usize>,
    ) -> impl Future<Output = ()> + Send;

    fn recv_req(
        rx: &mut Self::MpscRx,
    ) -> impl Future<Output = Option<Self::OneshotTx<usize>>> + Send;
}

struct Asyncband;

impl OneshotPeer for Asyncband {
    type OneshotTx<T: Send + 'static> = asyncband::oneshot::Sender<T>;
    type OneshotRx<T: Send + 'static> = asyncband::oneshot::Receiver<T>;
    type RecvOut<T: Send + 'static> = Result<T, asyncband::oneshot::RecvError>;
    type MpscTx = asyncband::mpsc::BoundedSender<Self::OneshotTx<usize>>;
    type MpscRx = asyncband::mpsc::BoundedReceiver<Self::OneshotTx<usize>>;

    #[inline]
    fn oneshot<T: Send + 'static>() -> (Self::OneshotTx<T>, Self::OneshotRx<T>) {
        asyncband::oneshot::channel()
    }

    #[inline]
    fn send_oneshot<T: Send + 'static>(tx: Self::OneshotTx<T>, value: T) {
        tx.send(value).unwrap();
    }

    #[inline]
    fn unwrap_oneshot<T: Send + 'static>(out: Self::RecvOut<T>) -> T {
        out.unwrap()
    }

    fn mpsc_bounded(capacity: usize) -> (Self::MpscTx, Self::MpscRx) {
        asyncband::mpsc::bounded(capacity)
    }

    #[inline]
    async fn send_req(tx: &Self::MpscTx, reply: Self::OneshotTx<usize>) {
        tx.send(reply).await.unwrap();
    }

    #[inline]
    async fn recv_req(rx: &mut Self::MpscRx) -> Option<Self::OneshotTx<usize>> {
        rx.recv().await.ok()
    }
}

struct Tokio;

impl OneshotPeer for Tokio {
    type OneshotTx<T: Send + 'static> = tokio::sync::oneshot::Sender<T>;
    type OneshotRx<T: Send + 'static> = tokio::sync::oneshot::Receiver<T>;
    type RecvOut<T: Send + 'static> = Result<T, tokio::sync::oneshot::error::RecvError>;
    type MpscTx = tokio::sync::mpsc::Sender<Self::OneshotTx<usize>>;
    type MpscRx = tokio::sync::mpsc::Receiver<Self::OneshotTx<usize>>;

    #[inline]
    fn oneshot<T: Send + 'static>() -> (Self::OneshotTx<T>, Self::OneshotRx<T>) {
        tokio::sync::oneshot::channel()
    }

    #[inline]
    fn send_oneshot<T: Send + 'static>(tx: Self::OneshotTx<T>, value: T) {
        tx.send(value).ok().unwrap();
    }

    #[inline]
    fn unwrap_oneshot<T: Send + 'static>(out: Self::RecvOut<T>) -> T {
        out.unwrap()
    }

    fn mpsc_bounded(capacity: usize) -> (Self::MpscTx, Self::MpscRx) {
        tokio::sync::mpsc::channel(capacity)
    }

    #[inline]
    async fn send_req(tx: &Self::MpscTx, reply: Self::OneshotTx<usize>) {
        tx.send(reply).await.unwrap();
    }

    #[inline]
    async fn recv_req(rx: &mut Self::MpscRx) -> Option<Self::OneshotTx<usize>> {
        rx.recv().await
    }
}

fn bench_request_reply<O: OneshotPeer>(
    runtime: &tokio::runtime::Runtime,
    bencher: benchmarks::harness::Bencher<'_>,
) {
    let (req_tx, mut req_rx) = O::mpsc_bounded(16);
    let responder = runtime.spawn(async move {
        while let Some(reply) = O::recv_req(&mut req_rx).await {
            O::send_oneshot(reply, black_box(1usize));
        }
    });
    bencher.bench_local(|| {
        runtime.block_on(async {
            let mut sum = 0usize;
            for i in 0..REQUESTS_PER_ROUND {
                let (reply_tx, reply_rx) = O::oneshot::<usize>();
                O::send_req(&req_tx, reply_tx).await;
                let value = O::unwrap_oneshot(reply_rx.await);
                sum = sum.wrapping_add(value ^ i);
            }
            black_box(sum)
        })
    });
    drop(req_tx);
    runtime.block_on(responder).unwrap();
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "oneshot/send-before-poll",
        20_000_000,
        implementations!([Asyncband, Tokio], |O, bencher| {
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let (tx, rx) = O::oneshot::<usize>();
                O::send_oneshot(tx, black_box(1usize));
                O::unwrap_oneshot(poll_ready(rx.into_future(), &mut cx))
            });
        }),
    );

    runner.compare(
        "oneshot/poll-before-send",
        18_000_000,
        implementations!([Asyncband, Tokio], |O, bencher| {
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let (tx, rx) = O::oneshot::<usize>();
                let mut rx = pin!(rx.into_future());
                poll_pending(rx.as_mut(), &mut cx);
                O::send_oneshot(tx, black_box(1usize));
                O::unwrap_oneshot(poll_pinned_ready(rx.as_mut(), &mut cx))
            });
        }),
    );

    runner.compare(
        "oneshot/request-reply-current",
        3_200,
        implementations!([Asyncband, Tokio], |O, bencher| {
            let runtime = current_thread();
            bench_request_reply::<O>(&runtime, bencher);
        }),
    );

    runner.compare(
        "oneshot/request-reply-4w",
        38,
        implementations!([Asyncband, Tokio], |O, bencher| {
            let runtime = multi_thread(WORKERS);
            bench_request_reply::<O>(&runtime, bencher);
        }),
    );
}

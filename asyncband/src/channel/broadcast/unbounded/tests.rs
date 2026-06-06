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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::*;

struct TrackWake(Arc<AtomicUsize>);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn count_waker() -> (Waker, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(TrackWake(count.clone())));
    (waker, count)
}

#[tokio::test]
async fn test_broadcast_basic() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    tx.send(10);
    tx.send(20);

    assert_eq!(rx1.recv().await, Ok(10));
    assert_eq!(rx1.recv().await, Ok(20));
    assert_eq!(rx2.recv().await, Ok(10));
    assert_eq!(rx2.recv().await, Ok(20));
}

#[tokio::test]
async fn test_broadcast_slow_receiver() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);
    tx.send(3);
    tx.send(4);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(rx.recv().await, Ok(4));
}

#[tokio::test]
async fn test_broadcast_closed() {
    let (tx, mut rx) = channel::<()>();
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn test_broadcast_closed_after_buffered_messages() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn test_wait_mechanism() {
    let (tx, mut rx) = channel();
    let (waker, wake_count) = count_waker();
    let mut cx = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut cx).is_pending());

    tx.send(42);

    assert_eq!(wake_count.load(Ordering::Relaxed), 1);
    assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(Ok(42)));
}

#[tokio::test]
async fn test_recv_cancellation_removes_waiter() {
    let (tx, mut rx) = channel::<i32>();
    let (waker, wake_count) = count_waker();
    let mut cx = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut cx).is_pending());

    drop(recv);
    tx.send(1);

    assert_eq!(wake_count.load(Ordering::Relaxed), 0);
    assert_eq!(rx.try_recv(), Ok(1));
}

#[tokio::test]
async fn test_dropped_woken_recv_does_not_remove_reused_waiter() {
    let (tx, mut rx1) = channel::<i32>();
    let mut rx2 = tx.subscribe();
    let (waker1, wake_count1) = count_waker();
    let mut cx1 = Context::from_waker(&waker1);
    let mut recv1 = Box::pin(rx1.recv());

    assert!(recv1.as_mut().poll(&mut cx1).is_pending());

    tx.send(1);
    assert_eq!(wake_count1.load(Ordering::Relaxed), 1);
    assert_eq!(rx2.try_recv(), Ok(1));

    let (waker2, wake_count2) = count_waker();
    let mut cx2 = Context::from_waker(&waker2);
    let mut recv2 = Box::pin(rx2.recv());
    assert!(recv2.as_mut().poll(&mut cx2).is_pending());

    drop(recv1);
    tx.send(2);

    assert_eq!(wake_count2.load(Ordering::Relaxed), 1);
    drop(recv2);
}

#[tokio::test]
async fn test_subscribe() {
    let (tx, _rx) = channel();
    let mut rx = tx.subscribe();

    tx.send(100);
    assert_eq!(rx.recv().await, Ok(100));
}

#[tokio::test]
async fn test_receiver_count_and_len() {
    let (tx, mut rx1) = channel();
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(rx1.len(), 0);
    assert!(rx1.is_empty());

    tx.send(1);
    tx.send(2);
    assert_eq!(rx1.len(), 2);
    assert!(!rx1.is_empty());

    let mut rx2 = tx.subscribe();
    assert_eq!(tx.receiver_count(), 2);
    assert_eq!(rx2.len(), 0);
    assert!(rx2.is_empty());

    tx.send(3);
    assert_eq!(rx1.len(), 3);
    assert_eq!(rx2.len(), 1);

    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx2.len(), 0);
    drop(rx2);
    assert_eq!(tx.receiver_count(), 1);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.len(), 2);
}

#[tokio::test]
async fn test_resubscribe() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();

    // rx sees 1, 2
    // rx2 sees nothing yet (starts at tail=2)

    tx.send(3);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx2.recv().await, Ok(3));
}

#[tokio::test]
async fn test_try_recv() {
    let (tx, mut rx) = channel();

    // Empty
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Success
    tx.send(10);
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Closed
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn test_consumed_messages_are_reclaimed() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.buffer_len(), 2);

    assert_eq!(rx1.recv().await, Ok(1));
    assert_eq!(tx.buffer_len(), 2);

    assert_eq!(rx2.recv().await, Ok(1));
    assert_eq!(tx.buffer_len(), 1);

    assert_eq!(rx1.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 1);

    assert_eq!(rx2.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn test_drop_receiver_reclaims_messages() {
    let (tx, mut rx1) = channel();
    let rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    tx.send(3);

    assert_eq!(rx1.recv().await, Ok(1));
    assert_eq!(rx1.recv().await, Ok(2));
    assert_eq!(rx1.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 3);

    drop(rx2);
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn test_buffer_len_tracks_shared_backlog() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.buffer_len(), 2);

    assert_eq!(rx1.recv().await, Ok(1));
    assert_eq!(rx1.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 2);

    assert_eq!(rx2.recv().await, Ok(1));
    assert_eq!(tx.buffer_len(), 1);
    assert_eq!(rx2.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn test_resubscribe_keeps_original_receiver_backlog() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();
    assert_eq!(tx.buffer_len(), 2);

    tx.send(3);

    assert_eq!(rx2.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 3);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
async fn test_send_panics_on_version_overflow() {
    let (tx, _) = channel();
    tx.shared.inner.lock().tail = u64::MAX;
    tx.send(());
}

#[tokio::test]
async fn test_send_without_receivers_does_not_buffer() {
    let (tx, rx) = channel();
    drop(rx);

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.buffer_len(), 0);

    let mut rx = tx.subscribe();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(3);
    assert_eq!(rx.recv().await, Ok(3));
}

#[tokio::test]
async fn test_multi_senders_concurrent() {
    let (tx, mut rx) = channel();
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    let handle1 = tokio::spawn(async move {
        for i in 0..10 {
            tx1.send(i);
        }
    });

    let handle2 = tokio::spawn(async move {
        for i in 10..20 {
            tx2.send(i);
        }
    });

    // Main tx can also send
    for i in 20..30 {
        tx.send(i);
    }

    handle1.await.unwrap();
    handle2.await.unwrap();
    drop(tx);

    let mut received = Vec::new();
    while let Ok(n) = rx.recv().await {
        received.push(n);
    }
    received.sort();

    let expected = (0..30).collect::<Vec<_>>();
    assert_eq!(received, expected);
}

#[tokio::test]
async fn test_multi_senders_multiple_receivers_receive_all() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    let handle1 = tokio::spawn(async move {
        for i in 0..10 {
            tx1.send(i);
        }
    });
    let handle2 = tokio::spawn(async move {
        for i in 10..20 {
            tx2.send(i);
        }
    });

    for i in 20..30 {
        tx.send(i);
    }

    handle1.await.unwrap();
    handle2.await.unwrap();
    drop(tx);

    let received1 = drain(&mut rx1).await;
    let received2 = drain(&mut rx2).await;
    let received3 = drain(&mut rx3).await;

    assert_eq!(received1, received2);
    assert_eq!(received1, received3);

    let mut sorted = received1;
    sorted.sort();
    let expected = (0..30).collect::<Vec<_>>();
    assert_eq!(sorted, expected);
}

async fn drain(rx: &mut Receiver<i32>) -> Vec<i32> {
    let mut received = Vec::new();
    while let Ok(n) = rx.recv().await {
        received.push(n);
    }
    received
}

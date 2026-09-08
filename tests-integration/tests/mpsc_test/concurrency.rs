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
use std::pin::Pin;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread;

use asyncband::mpsc;
use asyncband::mpsc::RecvError;
use asyncband::mpsc::TryRecvError;
use asyncband::mpsc::TrySendError;
use tests_integration::poll_once;
use tests_integration::test_runtime;
use tokio_test::assert_ok;

use super::support::WakeCounter;
use super::support::poll_with;

#[test]
fn bounded_receive_racing_with_send_registration_cannot_lose_wakeup() {
    for _ in 0..128 {
        let (tx, mut rx) = mpsc::bounded(1);
        tx.try_send(1).unwrap();
        let start = Barrier::new(2);
        let (waker, notified) = WakeCounter::new();
        let mut send = Box::pin(tx.send(2));
        let poll = thread::scope(|scope| {
            let receive = scope.spawn(|| {
                start.wait();
                assert_eq!(rx.try_recv(), Ok(1));
            });
            start.wait();
            let poll = send.as_mut().poll(&mut Context::from_waker(&waker));
            receive.join().unwrap();
            poll
        });
        if poll.is_pending() {
            assert!(notified.count() > 0);
            assert_eq!(poll_once(send.as_mut()), Poll::Ready(Ok(())));
        } else {
            assert_eq!(poll, Poll::Ready(Ok(())));
        }
        assert_eq!(rx.try_recv(), Ok(2));
    }
}

#[test]
fn bounded_try_recv_does_not_report_empty_after_completed_sends() {
    const PRODUCERS: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = if cfg!(miri) { 64 } else { 16_384 };
    let (tx, mut rx) = mpsc::bounded(64);
    let completed = AtomicUsize::new(0);
    let mut premature_empty = 0;
    let mut next = [0; PRODUCERS];
    let mut out_of_order = 0;

    thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let tx = tx.clone();
            let completed = &completed;
            scope.spawn(move || {
                for sequence in 0..MESSAGES_PER_PRODUCER {
                    loop {
                        match tx.try_send((producer, sequence)) {
                            Ok(()) => break,
                            Err(TrySendError::Full(_)) => thread::yield_now(),
                            Err(TrySendError::Disconnected(_)) => panic!("receiver is still alive"),
                        }
                    }
                    completed.fetch_add(1, Ordering::Release);
                }
            });
        }

        let mut received = 0;
        while received < PRODUCERS * MESSAGES_PER_PRODUCER {
            // Once more sends have completed than messages received, Empty cannot be correct.
            let has_completed_send = completed.load(Ordering::Acquire) > received;
            match rx.try_recv() {
                Ok((producer, sequence)) => {
                    out_of_order += usize::from(sequence != next[producer]);
                    next[producer] += 1;
                    received += 1;
                }
                Err(TryRecvError::Empty) => {
                    premature_empty += usize::from(has_completed_send);
                    thread::yield_now();
                }
                Err(TryRecvError::Disconnected) => panic!("original sender is still alive"),
            }
        }
    });

    // Drain and join before asserting so a failure cannot strand a producer on a full channel.
    assert_eq!(premature_empty, 0);
    assert_eq!(out_of_order, 0);
    assert_eq!(next, [MESSAGES_PER_PRODUCER; PRODUCERS]);
}

fn receive_racing_with(
    mut receive: Pin<&mut impl Future<Output = Result<usize, RecvError>>>,
    publish: impl FnOnce() + Send,
    expected: Result<usize, RecvError>,
) {
    let (waker, counter) = WakeCounter::new();
    let start = Barrier::new(2);
    let result = thread::scope(|scope| {
        let producer = scope.spawn(|| {
            start.wait();
            publish();
        });
        start.wait();
        let result = poll_with(receive.as_mut(), &waker);
        producer.join().unwrap();
        result
    });
    if result.is_pending() {
        assert!(counter.count() > 0, "a pending receiver must be notified");
        assert_eq!(poll_with(receive, &waker), Poll::Ready(expected));
    } else {
        assert_eq!(result, Poll::Ready(expected));
    }
}

#[test]
fn publication_racing_with_receiver_registration_cannot_lose_wakeup() {
    for capacity in [1, 3] {
        let (tx, mut rx) = mpsc::bounded(capacity);
        for value in 0..128 {
            receive_racing_with(
                Box::pin(rx.recv()).as_mut(),
                || tx.try_send(value).unwrap(),
                Ok(value),
            );
        }
    }
    let (tx, mut rx) = mpsc::unbounded();
    for value in 0..128 {
        receive_racing_with(
            Box::pin(rx.recv()).as_mut(),
            || tx.send(value).unwrap(),
            Ok(value),
        );
    }
}

#[test]
fn last_sender_drop_racing_with_receiver_registration_cannot_lose_wakeup() {
    for _ in 0..128 {
        let (tx, mut rx) = mpsc::bounded::<usize>(1);
        receive_racing_with(
            Box::pin(rx.recv()).as_mut(),
            move || drop(tx),
            Err(RecvError::Disconnected),
        );
        let (tx, mut rx) = mpsc::unbounded::<usize>();
        receive_racing_with(
            Box::pin(rx.recv()).as_mut(),
            move || drop(tx),
            Err(RecvError::Disconnected),
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
fn unbounded_collects_from_multiple_producers() {
    let (tx, mut rx) = mpsc::unbounded();

    test_runtime().block_on(async move {
        for i in 0..8 {
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(i).unwrap();
            });
        }
        drop(tx);

        let mut sum = 0;
        while let Ok(i) = rx.recv().await {
            sum += i;
        }
        assert_eq!(sum, 28);
    });
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_backpressure_progresses_on_an_executor() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    // This will block until the receiver is ready to receive.
    tokio::spawn(async move {
        tx.send(2).await.unwrap();
    });

    assert_eq!(Ok(1), rx.recv().await);
    assert_eq!(Ok(2), rx.recv().await);
    assert_eq!(Err(RecvError::Disconnected), rx.recv().await);
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn selection_preserves_messages_across_cancelled_receives() {
    let (tx1, mut rx1) = mpsc::unbounded::<i32>();
    let (tx2, mut rx2) = mpsc::unbounded::<i32>();
    let (tx3, mut rx3) = mpsc::bounded(1);
    let (tx4, mut rx4) = mpsc::bounded(1);

    tokio::spawn(async move {
        assert_ok!(tx2.send(1));
        tokio::task::yield_now().await;

        assert_ok!(tx1.send(2));
        tokio::task::yield_now().await;

        assert_ok!(tx2.send(3));
        tokio::task::yield_now().await;

        assert_ok!(tx3.send(4).await);
        tokio::task::yield_now().await;

        assert_ok!(tx4.send(5).await);
        tokio::task::yield_now().await;

        assert_ok!(tx3.send(6).await);
        tokio::task::yield_now().await;

        drop((tx1, tx2));
    });

    let mut msgs = vec![];
    let mut rx1_disconnected = false;
    let mut rx2_disconnected = false;
    let mut rx3_disconnected = false;
    let mut rx4_disconnected = false;

    while !(rx1_disconnected && rx2_disconnected && rx3_disconnected && rx4_disconnected) {
        tokio::select! {
            result = rx1.recv(), if !rx1_disconnected => {
                match result {
                    Ok(x) => msgs.push(x),
                    Err(RecvError::Disconnected) => rx1_disconnected = true,
                }
            }
            result = rx2.recv(), if !rx2_disconnected => {
                match result {
                    Ok(y) => msgs.push(y),
                    Err(RecvError::Disconnected) => rx2_disconnected = true,
                }
            }
            result = rx3.recv(), if !rx3_disconnected => {
                match result {
                    Ok(z) => msgs.push(z),
                    Err(RecvError::Disconnected) => rx3_disconnected = true,
                }
            }
            result = rx4.recv(), if !rx4_disconnected => {
                match result {
                    Ok(w) => msgs.push(w),
                    Err(RecvError::Disconnected) => rx4_disconnected = true,
                }
            }
        }
    }

    msgs.sort_unstable();
    assert_eq!(&msgs[..], &[1, 2, 3, 4, 5, 6]);
}

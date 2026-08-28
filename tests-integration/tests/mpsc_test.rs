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

use std::task::Poll;
use std::time::Instant;

use asyncband::mpsc;
use asyncband::mpsc::RecvError;
use asyncband::mpsc::TryRecvError;
use asyncband::mpsc::TrySendError;
use tests_integration::poll_once;
use tests_integration::test_runtime;
use tokio_test::assert_ok;

fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

#[test]
fn test_unbounded_pressure() {
    let n = 1024 * 1024;
    let (tx, mut rx) = mpsc::unbounded();

    test_runtime().block_on(async move {
        let start = Instant::now();
        tokio::spawn(async move {
            for i in 0..n {
                tx.send(i).unwrap();
            }
        });

        for i in 0..n {
            assert_eq!(rx.recv().await, Ok(i));
        }
        println!("Elapsed: {:?}", start.elapsed());
    });
}

#[test]
fn test_unbounded_sum() {
    let (tx, mut rx) = mpsc::unbounded();

    test_runtime().block_on(async move {
        for i in 0..100 {
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
        assert_eq!(sum, 4950);
    });
}

#[tokio::test]
async fn select_streams() {
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

    let mut rem = true;
    let mut msgs = vec![];
    let mut rx1_closed = false;
    let mut rx2_closed = false;
    let mut rx3_closed = false;
    let mut rx4_closed = false;

    while rem {
        rem = !(rx1_closed && rx2_closed && rx3_closed && rx4_closed);

        tokio::select! {
            result = rx1.recv(), if !rx1_closed => {
                match result {
                    Ok(x) => msgs.push(x),
                    Err(RecvError::Disconnected) => rx1_closed = true,
                }
            }
            result = rx2.recv(), if !rx2_closed => {
                match result {
                    Ok(y) => msgs.push(y),
                    Err(RecvError::Disconnected) => rx2_closed = true,
                }
            }
            result = rx3.recv(), if !rx3_closed => {
                match result {
                    Ok(z) => msgs.push(z),
                    Err(RecvError::Disconnected) => rx3_closed = true,
                }
            }
            result = rx4.recv(), if !rx4_closed => {
                match result {
                    Ok(w) => msgs.push(w),
                    Err(RecvError::Disconnected) => rx4_closed = true,
                }
            }
            else => {
                rx1_closed = true;
                rx2_closed = true;
                rx3_closed = true;
                rx4_closed = true;
            }
        }
    }

    msgs.sort_unstable();
    assert_eq!(&msgs[..], &[1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn send_recv_unbounded() {
    let (tx, mut rx) = mpsc::unbounded::<i32>();

    // Using `try_send`
    assert_ok!(tx.send(1));
    assert_ok!(tx.send(2));

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));

    drop(tx);

    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn async_send_recv_unbounded() {
    let (tx, mut rx) = mpsc::unbounded();

    tokio::spawn(async move {
        assert_ok!(tx.send(1));
        assert_ok!(tx.send(2));
    });

    assert_eq!(Ok(1), rx.recv().await);
    assert_eq!(Ok(2), rx.recv().await);
    assert_eq!(Err(RecvError::Disconnected), rx.recv().await);
}

#[test]
fn try_recv_unbounded() {
    for num in 0..100 {
        let (tx, mut rx) = mpsc::unbounded();

        for i in 0..num {
            tx.send(i).unwrap();
        }

        for i in 0..num {
            assert_eq!(rx.try_recv(), Ok(i));
        }

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn try_recv_close_while_empty_unbounded() {
    let (tx, mut rx) = mpsc::unbounded::<()>();

    assert_eq!(Err(TryRecvError::Empty), rx.try_recv());
    drop(tx);
    assert_eq!(Err(TryRecvError::Disconnected), rx.try_recv());
}

#[tokio::test]
async fn send_recv_bounded() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    assert_eq!(rx.recv().await, Ok(1));

    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn async_send_recv_bounded() {
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

#[test]
fn try_send_recv_bounded() {
    for num in 1..101 {
        let (tx, mut rx) = mpsc::bounded(num);

        for i in 0..num {
            tx.try_send(i).unwrap();
        }

        assert_eq!(tx.try_send(num), Err(TrySendError::Full(num)));

        for i in 0..num {
            assert_eq!(rx.try_recv(), Ok(i));
        }

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[tokio::test]
async fn try_send_after_close_bounded() {
    let (tx, rx) = mpsc::bounded(1);

    tx.try_send(1).unwrap();
    drop(rx);

    assert_eq!(tx.try_send(3), Err(TrySendError::Disconnected(3)));
}

#[tokio::test]
async fn send_after_close_bounded() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    assert_eq!(rx.recv().await, Ok(1));

    drop(rx);
    let error = tx.send(2).await.unwrap_err();
    assert_eq!(error.into_inner(), 2);
}

#[test]
fn bounded_wakes_blocked_senders_one_at_a_time() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(expect_ready(poll_once(first.as_mut())), Ok(()));
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(expect_ready(poll_once(second.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn bounded_cancelled_notified_sender_passes_slot_to_next_sender() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    drop(first);

    assert_eq!(expect_ready(poll_once(second.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn bounded_receiver_drop_returns_values_to_all_blocked_senders() {
    let (tx, rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    drop(rx);

    let first_error = expect_ready(poll_once(first.as_mut())).unwrap_err();
    let second_error = expect_ready(poll_once(second.as_mut())).unwrap_err();
    assert_eq!(first_error.into_inner(), 1);
    assert_eq!(second_error.into_inner(), 2);
}

#[test]
fn test_bounded_pressure() {
    let n = 1024 * 1024;
    let (tx, mut rx) = mpsc::bounded(1024);

    test_runtime().block_on(async move {
        let start = Instant::now();
        tokio::spawn(async move {
            for i in 0..n {
                tx.send(i).await.unwrap();
            }
        });

        for i in 0..n {
            assert_eq!(rx.recv().await, Ok(i));
        }
        println!("Elapsed: {:?}", start.elapsed());
    });
}

// This file contains a polling loop adapted from Pollster 1.0.1 and a thread-local Parker/Waker
// reuse strategy adapted from futures-lite 2.6.1.
// Copyright (c) 2020-2021 Joshua Barretto
// Asyncband uses the Apache-2.0 license option for code incorporated from both projects.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream sources:
// https://github.com/zesterer/pollster/blob/6a1a148208326e9c5b231b16f199f5227c550774/src/lib.rs
// https://github.com/smol-rs/futures-lite/blob/226ce18976d8714d6bd9700b61dcc81d7200bc9a/src/future.rs#L62-L91

use std::cell::RefCell;
use std::future::IntoFuture;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use super::parker::Parker;

pub(super) fn block_on<F: IntoFuture>(future: F) -> F::Output {
    let mut future = pin!(future.into_future());

    with_parker(|parker, waker| {
        let mut context = Context::from_waker(waker);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Pending => parker.park(),
                Poll::Ready(output) => return output,
            }
        }
    })
}

pub(super) fn wait_timeout<F: IntoFuture>(future: F, timeout: Duration) -> Option<F::Output> {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        // A duration beyond Instant's range cannot expire during the process lifetime.
        return Some(block_on(future));
    };
    let mut future = pin!(future.into_future());

    with_parker(|parker, waker| {
        let mut context = Context::from_waker(waker);

        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return Some(output);
            }

            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            parker.park_timeout(deadline.saturating_duration_since(now));
        }
    })
}

fn parker_and_waker() -> (Parker, Waker) {
    let parker = Parker::new();
    let waker = parker.waker();
    (parker, waker)
}

thread_local! {
    // Holding the mutable borrow while polling makes a recursive call take the fresh-parker path
    // instead of sharing a notification token.
    static CACHE: RefCell<(Parker, Waker)> = RefCell::new(parker_and_waker());
}

fn with_parker<T>(wait: impl FnOnce(&Parker, &Waker) -> T) -> T {
    CACHE.with(|cache| {
        let cached;
        let fresh;
        let (parker, waker) = match cache.try_borrow_mut() {
            Ok(pair) => {
                cached = pair;
                &*cached
            }
            Err(_) => {
                fresh = parker_and_waker();
                &fresh
            }
        };

        wait(parker, waker)
    })
}

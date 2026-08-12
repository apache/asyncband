// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::prelude::rust_2015::Vec;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use super::*;

macro_rules! assert_time {
    ($time:expr, $mills:literal $(,)?) => {
        assert!(
            (Duration::from_millis($mills - 1)
                ..Duration::from_millis($mills + std::cmp::max($mills >> 1, 50)))
                .contains(&$time)
        )
    };
}

#[test]
fn test_count_down() {
    let latch = Latch::new(3);
    latch.count_down();
    latch.count_down();
    latch.count_down();
    assert_eq!(latch.count(), 0);
}

#[test]
fn test_try_wait() {
    let latch = Latch::new(0);
    assert_eq!(latch.try_wait(), Ok(()));
}

#[test]
fn test_try_wait_err() {
    let latch = Latch::new(3);
    assert_eq!(latch.try_wait(), Err(3));
}

#[test]
fn test_arrive_zero() {
    let latch = Latch::new(2);
    latch.arrive(0);
    assert_eq!(latch.count(), 2);
}

#[test]
fn test_more_arrive() {
    let latch = Latch::new(10);
    for _ in 0..4 {
        latch.arrive(3);
    }
    assert_eq!(latch.count(), 0);
}

#[tokio::test]
async fn test_arrive() {
    let latch = Latch::new(3);
    latch.arrive(3);
    latch.wait().await;
    assert_eq!(latch.count(), 0);
}

#[tokio::test]
async fn test_last_one_signal() {
    let latch = Arc::new(Latch::new(3));
    let l1 = latch.clone();

    latch.count_down();
    latch.count_down();

    let start = Instant::now();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(32)).await;
        l1.count_down()
    });

    latch.wait().await;
    assert_time!(start.elapsed(), 32);
    assert_eq!(latch.count(), 0);
}

#[tokio::test]
async fn test_gate_wait() {
    let latch = Arc::new(Latch::new(1));
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let latch = latch.clone();
            let start = Instant::now();

            tokio::spawn(async move {
                latch.wait().await;
                start.elapsed()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(20)).await;
    latch.count_down();

    for t in tasks {
        assert_time!(t.await.unwrap(), 20);
    }
}

#[tokio::test]
async fn test_multi_tasks() {
    const SIZE: u32 = 16;
    let latch = Arc::new(Latch::new(SIZE));
    let counter = Arc::new(AtomicU32::new(0));

    for _ in 0..SIZE {
        let latch = latch.clone();
        let counter = counter.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            counter.fetch_add(1, Ordering::Relaxed);
            latch.count_down()
        });
    }

    let start = Instant::now();

    latch.wait().await;
    assert_time!(start.elapsed(), 20);
    assert_eq!(counter.load(Ordering::Relaxed), SIZE);
    assert_eq!(latch.count(), 0);
}

#[tokio::test]
async fn test_more_count_down() {
    const SIZE: u32 = 16;
    let latch = Arc::new(Latch::new(SIZE));
    let counter = Arc::new(AtomicU32::new(0));

    for _ in 0..(SIZE + (SIZE >> 1)) {
        let latch = latch.clone();
        let counter = counter.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            counter.fetch_add(1, Ordering::Relaxed);
            latch.count_down()
        });
    }

    latch.wait().await;
    assert!(counter.load(Ordering::Relaxed) >= SIZE);
    assert_eq!(latch.count(), 0);
    latch.count_down();
    assert_eq!(latch.count(), 0);
}

#[tokio::test]
async fn test_select_two_wait() {
    let latch1 = Arc::new(Latch::new(1));
    let latch2 = Arc::new(Latch::new(1));
    let l1 = latch1.clone();
    let l2 = latch2.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        l1.count_down();
    });

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        l2.count_down();
    });

    assert!(tokio::select! {
        _ = latch1.wait() => false,
        _ = latch2.wait() => true,
    });

    latch1.wait().await;
}

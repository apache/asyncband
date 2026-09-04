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

// Portions of this test support are adapted from the oneshot crate at commit
// 83fd0864be7289067ce96cc79cd96c0928742979:
// The incorporated upstream portions are licensed under Apache-2.0. See the project LICENSE file.
// https://github.com/faern/oneshot/tree/83fd0864be7289067ce96cc79cd96c0928742979/tests/helpers

use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

pub(super) struct DropProbe<T> {
    drop_count: Arc<AtomicUsize>,
    value: T,
}

impl<T> DropProbe<T> {
    pub(super) fn new(value: T) -> (Self, Arc<AtomicUsize>) {
        let drop_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                drop_count: drop_count.clone(),
                value,
            },
            drop_count,
        )
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }
}

impl<T> Drop for DropProbe<T> {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(super) struct WakerProbe {
    wake_count: AtomicU32,
}

impl WakerProbe {
    pub(super) fn new() -> (Waker, Arc<Self>) {
        let probe = Arc::new(Self::default());
        (Waker::from(probe.clone()), probe)
    }

    pub(super) fn live_waker_count(this: &Arc<Self>) -> usize {
        // The returned probe owns one strong reference; every other reference belongs to a live
        // Waker created from it.
        Arc::strong_count(this) - 1
    }

    pub(super) fn wake_count(&self) -> u32 {
        self.wake_count.load(Ordering::Relaxed)
    }
}

impl Wake for WakerProbe {
    fn wake(self: Arc<Self>) {
        self.wake_count.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wake_count.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) fn spawn_named<F, T>(name: &str, f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(f)
        .unwrap()
}

pub(super) fn spin_until<F>(label: &str, mut f: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut spins = 0usize;

    loop {
        if f() {
            break;
        }

        assert!(Instant::now() < deadline, "timed out waiting for {label}");

        if spins % 64 == 0 {
            std::thread::yield_now();
        } else {
            spin_loop();
        }

        spins += 1;
    }
}

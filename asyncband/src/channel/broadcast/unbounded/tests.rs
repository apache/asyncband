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

use super::*;

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    // The receiver is dropped right away: the doctored counter would make its own drop overflow.
    let (tx, _) = channel();
    tx.shared.inner.lock().tail = u64::MAX;
    tx.send(());
}

#[test]
fn one_off_burst_allocation_is_returned_once_it_is_behind_us() {
    let (tx, mut rx) = channel();

    let burst = MIN_RETAINED_CAPACITY * 16;
    for i in 0..burst {
        tx.send(i);
    }
    assert!(tx.shared.inner.lock().buffer.capacity() >= burst);

    for i in 0..burst {
        assert_eq!(rx.try_recv(), Ok(i));
    }

    // Draining evaluates the cycle that just peaked, so the burst allocation is still held.
    assert_eq!(tx.buffer_len(), 0);
    assert!(tx.shared.inner.lock().buffer.capacity() >= burst);

    // The next cycle stays small, which is what releases the memory.
    tx.send(0);
    assert_eq!(rx.try_recv(), Ok(0));
    assert!(tx.shared.inner.lock().buffer.capacity() < burst);
}

#[test]
fn repeated_bursts_keep_their_allocation() {
    let (tx, mut rx) = channel();
    let burst = MIN_RETAINED_CAPACITY * 4;

    for _ in 0..4 {
        for i in 0..burst {
            tx.send(i);
        }
        for i in 0..burst {
            assert_eq!(rx.try_recv(), Ok(i));
        }
    }

    // Every cycle peaks at the same size, so the buffer must not rebuild its allocation each time.
    assert!(tx.shared.inner.lock().buffer.capacity() >= burst);
}

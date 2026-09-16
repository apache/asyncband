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

use super::TryRecvError;
use super::*;
use crate::broadcast::spmc::common::CHUNK_LEN;

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    // Keep a live subscription at the doctored tail so drop does not walk the whole log, and so
    // this send is a publication rather than a no-subscriber discard.
    let (mut tx, mut rx) = unbounded();
    tx.shared.set_tail(u64::MAX);
    rx.cursor = u64::MAX;
    tx.send(());
}

#[test]
fn chunk_storage_grows_and_drains_with_the_live_window() {
    let (mut tx, mut rx) = unbounded();

    let burst = CHUNK_LEN * 4;
    for i in 0..burst {
        tx.send(i);
    }
    assert!(tx.shared.buffer.allocated_slots() >= burst);

    for i in 0..burst {
        assert_eq!(rx.try_recv(), Ok(i));
    }

    // The footprint tracks the live window, not the lifetime message count: only the chunk
    // holding the current window is left.
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.buffer.allocated_slots(), CHUNK_LEN);

    // Released chunks keep their index entries, so lookups still reach live versions.
    for i in 0..CHUNK_LEN {
        tx.send(i);
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(tx.shared.buffer.allocated_slots(), CHUNK_LEN);
}

#[test]
fn discarded_sends_do_not_grow_chunk_storage() {
    let (mut tx, rx) = unbounded();
    drop(rx);

    for i in 0..CHUNK_LEN * 4 {
        tx.send(i);
    }

    // Discarded sends are not publications. The log must not allocate empty chunks for versions
    // that were never retained, matching MPMC's empty-buffer discard path.
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.buffer.allocated_slots(), CHUNK_LEN);

    let mut rx = tx.subscribe();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(0);
    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.buffer.allocated_slots(), CHUNK_LEN);
}

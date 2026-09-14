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
use crate::broadcast::spmc::common::CHUNK_LEN;

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    // The receiver is dropped right away: the doctored counter would make its own drop overflow.
    let (mut tx, _) = unbounded();
    tx.shared.set_tail(u64::MAX);
    tx.send(());
}

#[test]
fn chunks_grow_with_the_committed_log() {
    let (mut tx, mut rx) = unbounded();

    let burst = CHUNK_LEN * 4;
    for i in 0..burst {
        tx.send(i);
    }
    assert!(tx.shared.buffer.allocated_slots() >= burst);

    for i in 0..burst {
        assert_eq!(rx.try_recv(), Ok(i));
    }

    // Chunks stay allocated until the channel is dropped so receivers can walk them without a
    // reclamation lock.
    assert_eq!(tx.retained_message_count(), 0);
    assert!(tx.shared.buffer.allocated_slots() >= burst);
}

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

use std::collections::VecDeque;
use std::task::Waker;

#[derive(Debug, Default)]
pub(super) struct WaitQueue {
    next_id: u64,
    entries: VecDeque<(u64, Waker)>,
}

impl WaitQueue {
    pub(super) fn register(&mut self, id: &mut Option<u64>, waker: &Waker) -> Option<Waker> {
        if let Some(id) = id {
            if let Some((_, registered)) = self.entries.iter_mut().find(|(entry, _)| entry == id) {
                if !registered.will_wake(waker) {
                    return Some(std::mem::replace(registered, waker.clone()));
                }
                return None;
            }
        }

        let new_id = self.allocate_id();
        self.entries.push_back((new_id, waker.clone()));
        *id = Some(new_id);
        None
    }

    pub(super) fn remove(&mut self, id: &mut Option<u64>) -> Option<Waker> {
        let id = id.take()?;
        if let Some(index) = self.entries.iter().position(|(entry, _)| *entry == id) {
            return self.entries.remove(index).map(|(_, waker)| waker);
        }
        None
    }

    pub(super) fn take_all(&mut self) -> Vec<Waker> {
        self.entries.drain(..).map(|(_, waker)| waker).collect()
    }

    fn allocate_id(&mut self) -> u64 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.entries.iter().all(|(entry, _)| *entry != id) {
                return id;
            }
        }
    }
}

pub(super) fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

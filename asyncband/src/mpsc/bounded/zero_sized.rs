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

use crate::internal::mutex::Mutex;

// VecDeque stores only a count for ZSTs, while handling alignment and destructors safely.
// Keeping this separate avoids allocating a standard-channel stamp for every zero-sized value.
pub struct Channel<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    values: VecDeque<T>,
    closed: bool,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        debug_assert_eq!(size_of::<T>(), 0);
        Self {
            state: Mutex::new(State {
                values: VecDeque::new(),
                closed: false,
            }),
        }
    }

    pub fn send(&self, value: T) -> Result<(), T> {
        let mut state = self.state.lock();
        if state.closed {
            return Err(value);
        }
        state.values.push_back(value);
        Ok(())
    }

    pub fn recv(&self) -> Option<T> {
        self.state.lock().values.pop_front()
    }

    pub fn close(&self) {
        self.state.lock().closed = true;
    }
}

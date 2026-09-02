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

use std::task::Waker;

/// An owning waker collection that stores the first entry without allocating.
#[derive(Debug)]
pub struct WakerBatch {
    first: Option<Waker>,
    rest: Vec<Waker>,
}

impl WakerBatch {
    pub const fn new() -> Self {
        Self {
            first: None,
            rest: vec![],
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            first: None,
            rest: Vec::with_capacity(capacity.saturating_sub(1)),
        }
    }

    pub fn push(&mut self, waker: Waker) {
        if self.first.is_none() {
            self.first = Some(waker);
        } else {
            self.rest.push(waker);
        }
    }
}

impl Extend<Waker> for WakerBatch {
    fn extend<I: IntoIterator<Item = Waker>>(&mut self, iter: I) {
        for waker in iter {
            self.push(waker);
        }
    }
}

impl IntoIterator for WakerBatch {
    type Item = Waker;
    type IntoIter = std::iter::Chain<std::option::IntoIter<Waker>, std::vec::IntoIter<Waker>>;

    fn into_iter(self) -> Self::IntoIter {
        self.first.into_iter().chain(self.rest)
    }
}

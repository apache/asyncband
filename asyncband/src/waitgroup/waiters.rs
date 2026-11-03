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

use std::mem;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::task::Waker;

use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

pub enum Registration {
    First,
    Set(Option<WakerToken>),
}

#[derive(Default)]
struct Slots {
    first: Option<Waker>,
    extra: Option<Box<WakerSet>>,
}

// Registry mutations need exclusive access. The standard write lock keeps the uncontended
// lock state inline on Darwin, where a standard mutex allocates a pthread mutex on first use.
#[derive(Default)]
pub struct Waiters(RwLock<Slots>);

impl Waiters {
    fn slots(&self) -> RwLockWriteGuard<'_, Slots> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn register(
        &self,
        token: &mut Registration,
        waker: &Waker,
        handles: &AtomicUsize,
    ) -> Poll<()> {
        let mut slots = self.slots();
        // The last participant detaches registrations under this same lock. Either it takes our
        // waker, or we observe zero and need no registration.
        if handles.load(Ordering::Acquire) == 0 {
            *token = Registration::Set(None);
            return Poll::Ready(());
        }
        let retired = match token {
            Registration::First => {
                let current = slots.first.as_mut().expect("registered first waker");
                if current.will_wake(waker) {
                    None
                } else {
                    Some(mem::replace(current, waker.clone()))
                }
            }
            Registration::Set(None) if slots.first.is_none() => {
                slots.first = Some(waker.clone());
                *token = Registration::First;
                None
            }
            Registration::Set(id) => slots
                .extra
                .get_or_insert_with(Default::default)
                .register(id, waker),
        };
        drop(slots);
        drop(retired);
        Poll::Pending
    }

    pub fn unregister(&self, token: &mut Registration, handles: &AtomicUsize) {
        let mut slots = self.slots();
        if handles.load(Ordering::Acquire) != 0 {
            let retired = match token {
                Registration::First => slots.first.take(),
                Registration::Set(id) => slots
                    .extra
                    .as_mut()
                    .expect("registered overflow waker")
                    .unregister(id),
            };
            drop(slots);
            drop(retired);
        }
    }

    pub fn wake_all(&self) {
        let slots = mem::take(&mut *self.slots());
        if let Some(first) = slots.first {
            first.wake();
        }
        if let Some(extra) = slots.extra {
            extra.wake_all();
        }
    }
}

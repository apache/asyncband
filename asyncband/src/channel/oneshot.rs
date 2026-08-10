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

//! A channel that transfers at most one value.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

pub use crate::channel::RecvError;
pub use crate::channel::SendError;
pub use crate::channel::TryRecvError;
use crate::internal::mutex::Mutex;

/// Creates a one-shot channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: None,
            sender_alive: true,
            receiver_alive: true,
            receiver_waker: None,
        }),
    });
    (
        Sender {
            shared: Some(shared.clone()),
        },
        Receiver {
            shared: Some(shared),
        },
    )
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    value: Option<T>,
    sender_alive: bool,
    receiver_alive: bool,
    receiver_waker: Option<Waker>,
}

/// The sending half of a one-shot channel.
pub struct Sender<T> {
    shared: Option<Arc<Shared<T>>>,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("completed", &self.shared.is_none())
            .finish()
    }
}

impl<T> Sender<T> {
    /// Sends the channel's value.
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let shared = self
            .shared
            .take()
            .expect("a one-shot sender can only complete once");
        let waker = {
            let mut state = shared.state.lock();
            if !state.receiver_alive {
                return Err(SendError::new(value));
            }
            state.value = Some(value);
            state.sender_alive = false;
            state.receiver_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Returns true if the receiver has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared
            .as_ref()
            .is_none_or(|shared| !shared.state.lock().receiver_alive)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let waker = {
            let mut state = shared.state.lock();
            state.sender_alive = false;
            state.receiver_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// The receiving half of a one-shot channel.
pub struct Receiver<T> {
    shared: Option<Arc<Shared<T>>>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("completed", &self.shared.is_none())
            .finish()
    }
}

impl<T> Receiver<T> {
    /// Receives the value, waiting for the sender when necessary.
    pub async fn recv(self) -> Result<T, RecvError> {
        self.await
    }

    /// Attempts to receive the value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let Some(shared) = self.shared.as_ref() else {
            return Err(TryRecvError::Disconnected);
        };
        let result = {
            let mut state = shared.state.lock();
            if let Some(value) = state.value.take() {
                state.receiver_alive = false;
                Ok(value)
            } else if !state.sender_alive {
                state.receiver_alive = false;
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        };
        if !matches!(result, Err(TryRecvError::Empty)) {
            self.shared = None;
        }
        result
    }

    /// Returns true if no value can still be received.
    pub fn is_disconnected(&self) -> bool {
        self.shared.as_ref().is_none_or(|shared| {
            let state = shared.state.lock();
            !state.sender_alive && state.value.is_none()
        })
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(shared) = self.shared.as_ref() else {
            return Poll::Ready(Err(RecvError::Disconnected));
        };
        let mut retired_waker = None;
        let result = {
            let mut state = shared.state.lock();
            if let Some(value) = state.value.take() {
                state.receiver_alive = false;
                Some(Ok(value))
            } else if !state.sender_alive {
                state.receiver_alive = false;
                Some(Err(RecvError::Disconnected))
            } else {
                if state
                    .receiver_waker
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(cx.waker()))
                {
                    retired_waker = state.receiver_waker.replace(cx.waker().clone());
                }
                None
            }
        };
        drop(retired_waker);
        match result {
            Some(result) => {
                self.shared = None;
                Poll::Ready(result)
            }
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let retired_waker = {
            let mut state = shared.state.lock();
            state.receiver_alive = false;
            state.receiver_waker.take()
        };
        drop(retired_waker);
    }
}

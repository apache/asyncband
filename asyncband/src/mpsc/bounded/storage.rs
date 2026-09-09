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

use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::mpsc;

use super::zero_sized;
use crate::internal::mutex::Mutex;

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    if size_of::<T>() == 0 {
        let channel = Arc::new(zero_sized::Channel::new());
        (
            Sender::ZeroSized(channel.clone()),
            Receiver::ZeroSized(channel),
        )
    } else {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        (
            Sender::Standard(RwLock::new(Some(sender))),
            Receiver::Standard(Mutex::new(receiver)),
        )
    }
}

pub enum Sender<T> {
    Standard(RwLock<Option<mpsc::SyncSender<T>>>),
    ZeroSized(Arc<zero_sized::Channel<T>>),
}

impl<T> Sender<T> {
    // The caller owns capacity until the receiver removes this message.
    pub fn send(&self, value: T) -> Result<(), T> {
        match self {
            Self::Standard(sender) => {
                let sender = sender.read().unwrap_or_else(PoisonError::into_inner);
                let Some(sender) = sender.as_ref() else {
                    return Err(value);
                };
                match sender.try_send(value) {
                    Ok(()) => Ok(()),
                    Err(mpsc::TrySendError::Disconnected(value)) => Err(value),
                    Err(mpsc::TrySendError::Full(_)) => {
                        unreachable!("a reserved capacity unit must fit in the backing channel")
                    }
                }
            }
            Self::ZeroSized(channel) => channel.send(value),
        }
    }

    pub fn close(&self) {
        match self {
            Self::Standard(sender) => {
                // Taking the sole STD sender waits for synchronous publications to finish and
                // prevents new ones. The receiver can then drain without racing a late message.
                let sender = sender
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                drop(sender);
            }
            Self::ZeroSized(channel) => channel.close(),
        }
    }
}

pub enum Receiver<T> {
    // The mutex preserves Sync for Send-only payloads. Receiving uses exclusive get_mut access.
    Standard(Mutex<mpsc::Receiver<T>>),
    ZeroSized(Arc<zero_sized::Channel<T>>),
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Option<T> {
        match self {
            // The sender lives in shared state; endpoint disconnection is tracked by its owner.
            Self::Standard(receiver) => receiver.get_mut().try_recv().ok(),
            Self::ZeroSized(channel) => channel.recv(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // The owner closes the sender first. STD's receiver destructor may leak remaining
        // messages if a payload destructor panics, so remove each value before dropping it.
        // This guard finishes the drain during unwinding from the first destructor panic.
        struct Drain<'a, T>(&'a mut Receiver<T>);
        impl<T> Drop for Drain<'_, T> {
            fn drop(&mut self) {
                while let Some(value) = self.0.recv() {
                    drop(value);
                }
            }
        }
        let remaining = Drain(self);
        while let Some(value) = remaining.0.recv() {
            drop(value);
        }
    }
}

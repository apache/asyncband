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

use std::future::poll_fn;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::retry_waiter::RetryWaiter;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::spmc::RecvError;
use crate::spmc::SendError;
use crate::spmc::TryRecvError;
use crate::spmc::TrySendError;
use crate::spmc::atomic_ring::Ring;
use crate::spmc::queue;

pub enum Shared<T> {
    Ring(Box<Channel<T>>),
    General(queue::Shared<T>),
}

impl<T> Shared<T> {
    pub fn bounded(capacity: usize) -> Self {
        if Ring::<T>::supports(capacity) {
            Self::Ring(Box::new(Channel::new(capacity)))
        } else {
            // Preserve the single-slot handoff and lazy storage for oversized queues.
            Self::General(queue::Shared::bounded(capacity))
        }
    }

    pub fn drop_sender(&self) {
        match self {
            Self::Ring(channel) => channel.drop_sender(),
            Self::General(channel) => channel.drop_sender(),
        }
    }

    pub fn clone_receiver(&self) {
        match self {
            Self::Ring(channel) => channel.clone_receiver(),
            Self::General(channel) => channel.clone_receiver(),
        }
    }

    pub fn drop_receiver(&self) {
        match self {
            Self::Ring(channel) => channel.drop_receiver(),
            Self::General(channel) => channel.drop_receiver(),
        }
    }

    /// # Safety
    ///
    /// The caller must be this queue's only producer, and its calls must not overlap.
    pub unsafe fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self {
            // SAFETY: The caller is the only producer.
            Self::Ring(channel) => unsafe { channel.try_send(value) },
            Self::General(channel) => channel.try_send(value),
        }
    }

    /// # Safety
    ///
    /// The caller must be this queue's only producer, and no other producer call may overlap the
    /// returned future until it completes or is dropped.
    pub async unsafe fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self {
            // SAFETY: The caller remains the only producer across the await.
            Self::Ring(channel) => unsafe { channel.send(value).await },
            Self::General(channel) => channel.send(value).await,
        }
    }

    /// # Safety
    ///
    /// The caller must be this queue's only producer, and its calls must not overlap.
    pub unsafe fn try_reserve(&self) -> Result<(), TrySendError<()>> {
        match self {
            // SAFETY: The caller is the only producer.
            Self::Ring(channel) => unsafe { channel.ring.try_reserve() },
            Self::General(channel) => channel.try_reserve(),
        }
    }

    /// # Safety
    ///
    /// The caller must be this queue's only producer, and no other producer call may overlap the
    /// returned future until it completes or is dropped.
    pub async unsafe fn reserve(&self) -> Result<(), SendError<()>> {
        match self {
            // SAFETY: The caller remains the only producer across the await.
            Self::Ring(channel) => unsafe { channel.reserve().await },
            Self::General(channel) => channel.reserve().await,
        }
    }

    /// # Safety
    ///
    /// The caller must be this queue's only producer, and must have reserved capacity with no
    /// producer call since.
    pub unsafe fn send_reserved(&self, value: T) -> Result<(), SendError<T>> {
        match self {
            // SAFETY: The caller is the only producer and holds a reservation.
            Self::Ring(channel) => unsafe { channel.send_reserved(value) },
            Self::General(channel) => channel.send_reserved(value),
        }
    }

    pub fn has_sender(&self) -> bool {
        match self {
            Self::Ring(channel) => !channel.sender_closed.load(Ordering::SeqCst),
            Self::General(channel) => channel.has_sender(),
        }
    }

    pub fn has_receivers(&self) -> bool {
        match self {
            // The last receiver closes the ring.
            Self::Ring(channel) => !channel.ring.is_closed(),
            Self::General(channel) => channel.has_receivers(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Ring(channel) => channel.ring.is_empty(),
            Self::General(channel) => channel.is_empty(),
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self {
            Self::Ring(channel) => channel.try_recv(),
            Self::General(channel) => channel.try_recv(),
        }
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        match self {
            Self::Ring(channel) => channel.recv().await,
            Self::General(channel) => channel.recv().await,
        }
    }
}

pub struct Channel<T> {
    ring: Ring<T>,
    sender_closed: AtomicBool,
    recv_waiting: AtomicBool,
    send_waiting: AtomicBool,
    control: Mutex<Control>,
}

struct Control {
    receivers: usize,
    recv_waiters: WaitList<RetryWaiter>,
    send_waker: Option<Waker>,
}

impl<T> Channel<T> {
    fn new(capacity: usize) -> Self {
        Self {
            ring: Ring::new(capacity),
            sender_closed: AtomicBool::new(false),
            recv_waiting: AtomicBool::new(false),
            send_waiting: AtomicBool::new(false),
            control: Mutex::new(Control {
                receivers: 1,
                recv_waiters: WaitList::new(),
                send_waker: None,
            }),
        }
    }

    fn update_recv_waiting(&self, control: &Control) {
        self.recv_waiting
            .store(!control.recv_waiters.is_empty(), Ordering::SeqCst);
    }

    fn notify_receiver(&self) {
        if self.recv_waiting.load(Ordering::SeqCst) {
            let wake = {
                let mut control = self.control.lock();
                let wake = control.recv_waiters.notify_one();
                self.update_recv_waiting(&control);
                wake
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }
    }

    fn take_send_waker(&self, control: &mut Control) -> Option<Waker> {
        let wake = control.send_waker.take();
        self.send_waiting.store(false, Ordering::SeqCst);
        wake
    }

    fn notify_sender(&self) {
        if self.send_waiting.load(Ordering::SeqCst) {
            let wake = self.take_send_waker(&mut self.control.lock());
            if let Some(waker) = wake {
                waker.wake();
            }
        }
    }

    fn drop_sender(&self) {
        let mut waiters = {
            let mut control = self.control.lock();
            self.sender_closed.store(true, Ordering::SeqCst);
            self.recv_waiting.store(false, Ordering::SeqCst);
            mem::replace(&mut control.recv_waiters, WaitList::new())
        };
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
    }

    fn clone_receiver(&self) {
        self.control.lock().receivers += 1;
    }

    fn drop_receiver(&self) {
        let wake = {
            let mut control = self.control.lock();
            control.receivers -= 1;
            if control.receivers != 0 {
                return;
            }
            self.ring.close();
            self.take_send_waker(&mut control)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        self.ring.discard();
    }

    unsafe fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // SAFETY: Shared's producer contract makes this the ring's only producer.
        unsafe { self.ring.try_send(value)? };
        self.notify_receiver();
        Ok(())
    }

    async unsafe fn send(&self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: The caller remains the only producer throughout this operation.
        let value = match unsafe { self.try_send(value) } {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };
        let mut send = Send {
            waiter: SendWaiter {
                channel: self,
                registered: false,
            },
            value: Some(value),
        };
        poll_fn(|cx| send.poll(cx)).await
    }

    async unsafe fn reserve(&self) -> Result<(), SendError<()>> {
        // SAFETY: The caller remains the only producer throughout this operation.
        match unsafe { self.ring.try_reserve() } {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(())) => return Err(SendError::new(())),
            Err(TrySendError::Full(())) => {}
        }
        let mut reserve = Reserve(SendWaiter {
            channel: self,
            registered: false,
        });
        poll_fn(|cx| reserve.poll(cx)).await
    }

    unsafe fn send_reserved(&self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: The caller is the only producer.
        match unsafe { self.try_send(value) } {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(value)) => Err(SendError::new(value)),
            Err(TrySendError::Full(_)) => {
                unreachable!("only the reserving producer consumes capacity")
            }
        }
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        let value = match self.ring.try_recv() {
            Some(value) => value,
            None if self.sender_closed.load(Ordering::SeqCst) => {
                self.ring.try_recv().ok_or(TryRecvError::Disconnected)?
            }
            None => return Err(TryRecvError::Empty),
        };
        self.notify_sender();
        Ok(value)
    }

    async fn recv(&self) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(value) => return Ok(value),
            Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
            Err(TryRecvError::Empty) => {}
        }
        let mut recv = Recv {
            channel: self,
            waiter: None,
        };
        poll_fn(|cx| recv.poll(cx)).await
    }
}

struct SendWaiter<'a, T> {
    channel: &'a Channel<T>,
    registered: bool,
}

impl<T> SendWaiter<'_, T> {
    #[must_use = "drop the replaced waker after releasing the control lock"]
    fn register(&mut self, control: &mut Control, waker: &Waker) -> Option<Waker> {
        let retired = register_waker(&mut control.send_waker, waker);
        self.registered = true;
        // Reclamation publication precedes the consumer's notification check. Registering before
        // the SeqCst capacity recheck makes it impossible for both sides to miss the other.
        self.channel.send_waiting.store(true, Ordering::SeqCst);
        retired
    }

    #[must_use = "drop the registered waker after releasing the control lock"]
    fn take_registration(&mut self, control: &mut Control) -> Option<Waker> {
        self.registered = false;
        self.channel.take_send_waker(control)
    }
}

struct Send<'a, T> {
    waiter: SendWaiter<'a, T>,
    value: Option<T>,
}

impl<T> Send<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T>>> {
        let channel = self.waiter.channel;
        let mut control = channel.control.lock();
        let value = self.value.take().expect("pending send owns its value");
        if control.receivers == 0 {
            self.waiter.registered = false;
            return Poll::Ready(Err(SendError::new(value)));
        }
        let retired = self.waiter.register(&mut control, cx.waker());
        // SAFETY: Only Channel::send constructs this state, whose caller is the only producer.
        match unsafe { channel.ring.try_send(value) } {
            Ok(()) => {
                let registration = self.waiter.take_registration(&mut control);
                // Publishing under the lock notifies a waiting receiver without reacquiring it.
                let wake = control.recv_waiters.notify_one();
                channel.update_recv_waiting(&control);
                drop(control);
                if let Some(waker) = wake {
                    waker.wake();
                }
                drop(registration);
                drop(retired);
                Poll::Ready(Ok(()))
            }
            Err(TrySendError::Full(value)) => {
                self.value = Some(value);
                drop(control);
                drop(retired);
                Poll::Pending
            }
            Err(TrySendError::Disconnected(_)) => {
                unreachable!("receiver close requires the control lock");
            }
        }
    }
}

struct Reserve<'a, T>(SendWaiter<'a, T>);

impl<T> Reserve<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<()>>> {
        let channel = self.0.channel;
        let mut control = channel.control.lock();
        if control.receivers == 0 {
            self.0.registered = false;
            return Poll::Ready(Err(SendError::new(())));
        }
        let retired = self.0.register(&mut control, cx.waker());
        // SAFETY: Only Channel::reserve constructs this state, whose caller is the only producer.
        match unsafe { channel.ring.try_reserve() } {
            Ok(()) => {
                let registration = self.0.take_registration(&mut control);
                drop(control);
                drop(registration);
                drop(retired);
                Poll::Ready(Ok(()))
            }
            Err(TrySendError::Full(())) => {
                drop(control);
                drop(retired);
                Poll::Pending
            }
            Err(TrySendError::Disconnected(())) => {
                unreachable!("receiver close requires the control lock");
            }
        }
    }
}

impl<T> Drop for SendWaiter<'_, T> {
    fn drop(&mut self) {
        if self.registered {
            let retired = self
                .channel
                .take_send_waker(&mut self.channel.control.lock());
            drop(retired);
        }
    }
}

struct Recv<'a, T> {
    channel: &'a Channel<T>,
    waiter: Option<WaiterId>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut control = self.channel.control.lock();
        // The sender closes under the control lock.
        let sender_closed = self.channel.sender_closed.load(Ordering::Relaxed);
        let retired = if sender_closed {
            self.waiter = None;
            None
        } else {
            let retired = control.recv_waiters.register(&mut self.waiter, cx.waker());
            self.channel.recv_waiting.store(true, Ordering::SeqCst);
            retired
        };
        let value = self.channel.ring.try_recv();
        if value.is_none() && !sender_closed {
            drop(control);
            drop(retired);
            return Poll::Pending;
        }
        let registration = self.waiter.take().map(|id| control.recv_waiters.remove(id));
        self.channel.update_recv_waiting(&control);
        let wake = if value.is_some() {
            self.channel.take_send_waker(&mut control)
        } else {
            None
        };
        drop(control);
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(registration);
        drop(retired);
        Poll::Ready(value.ok_or(RecvError::Disconnected))
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let (retired, wake) = {
            let mut control = self.channel.control.lock();
            if self.channel.sender_closed.load(Ordering::Relaxed) {
                return;
            }
            let retired = control.recv_waiters.remove(id);
            let wake = if matches!(retired, RetryWaiter::Notified) && !self.channel.ring.is_empty()
            {
                control.recv_waiters.notify_one()
            } else {
                None
            };
            self.channel.update_recv_waiting(&control);
            (retired, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(retired);
    }
}

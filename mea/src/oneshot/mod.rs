// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This implementation is derived from the `oneshot` crate [1], with significant simplifications
// since mea needs not support synchronized receiving functions.
//
// [1] https://github.com/faern/oneshot/blob/83fd0864/src/lib.rs

//! A one-shot channel is used for sending a single message between
//! asynchronous tasks. The [`channel`] function is used to create a
//! [`Sender`] and [`Receiver`] handle pair that form the channel.
//!
//! The `Sender` handle is used by the producer to send the value.
//! The `Receiver` handle is used by the consumer to receive the value.
//!
//! Each handle can be used on separate tasks.
//!
//! Since the `send` method is not async, it can be used anywhere. This includes
//! sending between two runtimes, and using it from non-async code.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel();
//!
//! tokio::spawn(async move {
//!     if let Err(_) = tx.send(3) {
//!         println!("the receiver dropped");
//!     }
//! });
//!
//! match rx.await {
//!     Ok(v) => println!("got = {:?}", v),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```
//!
//! If the sender is dropped without sending, the receiver will fail with
//! [`RecvError`]:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! tokio::spawn(async move {
//!     drop(tx);
//! });
//!
//! match rx.await {
//!     Ok(_) => panic!("This doesn't happen"),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```

use std::any::type_name;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

#[cfg(test)]
mod tests;

/// Creates a new oneshot channel and returns the two endpoints, [`Sender`] and [`Receiver`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = NonNull::from(Box::leak(Box::new(Channel::new())));
    (
        Sender {
            channel: Some(ChannelRef(channel)),
        },
        Receiver {
            channel: Some(ChannelRef(channel)),
        },
    )
}

/// Sends a value to the associated [`Receiver`].
pub struct Sender<T> {
    channel: Option<ChannelRef<T>>,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Sync> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning an error contains the message if it
    /// could not be sent.
    pub fn send(mut self, message: T) -> Result<(), SendError<T>> {
        let channel_ref = self.channel.take().unwrap();
        let channel = channel_ref.get();

        // Write the message into the channel on the heap.
        //
        // SAFETY: The receiver only ever accesses this memory location if we are in the MESSAGE
        // state, and since we are responsible for setting that state, we can guarantee that we have
        // exclusive access to this memory location to perform this write.
        unsafe { channel.write_message(message) };

        // Publishing MESSAGE is the linearization point for a successful send. The receiver can
        // drop its handle immediately after observing it; this sender's allocation reference
        // keeps the channel alive while we take and wake a registered waker.
        match channel.state.swap(MESSAGE, Ordering::AcqRel) {
            EMPTY => Ok(()),
            WAITING => {
                // SAFETY: Replacing WAITING transfers ownership of the initialized waker to this
                // sender. The acquire half of the swap synchronizes with its publication.
                unsafe { channel.take_waker() }.wake();
                Ok(())
            }
            // The receiver was already dropped. No receiver can access the initialized message,
            // so restore the terminal state and return ownership to the caller.
            DISCONNECTED => {
                channel.state.store(DISCONNECTED, Ordering::Relaxed);
                Err(SendError::new(unsafe { channel.take_message() }))
            }
            state => unreachable!("unexpected channel state: {state}"),
        }
    }

    /// Returns true if the associated [`Receiver`] has been dropped.
    ///
    /// If true is returned, a future call to send is guaranteed to return an error.
    pub fn is_closed(&self) -> bool {
        let channel = self.channel.as_ref().unwrap().get();

        // ORDERING: We *chose* a Relaxed ordering here as it sufficient to enforce the method's
        // contract: "if true is returned, a future call to send is guaranteed to return an error."
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the receiver might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(channel_ref) = self.channel.take() else {
            return;
        };
        let channel = channel_ref.get();

        match channel.state.swap(DISCONNECTED, Ordering::AcqRel) {
            EMPTY => {}
            WAITING => {
                // SAFETY: Replacing WAITING transfers ownership of the initialized waker to this
                // sender. The acquire half of the swap synchronizes with its publication.
                unsafe { channel.take_waker() }.wake();
            }
            DISCONNECTED => {}
            state => unreachable!("unexpected channel state: {state}"),
        }
    }
}

/// Receives a value from the associated [`Sender`].
pub struct Receiver<T> {
    channel: Option<ChannelRef<T>>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

// The Receiver can NOT be `Sync`: receive operations take `&self` and assume that no other
// receive operation runs concurrently.

impl<T> Unpin for Receiver<T> {}

impl<T> IntoFuture for Receiver<T> {
    type Output = Result<T, RecvError>;

    type IntoFuture = Recv<T>;

    fn into_future(mut self) -> Self::IntoFuture {
        Recv {
            channel: self.channel.take(),
        }
    }
}

impl<T> Receiver<T> {
    /// Returns true if the associated [`Sender`] was dropped before sending a message. Or if
    /// the message has already been received.
    ///
    /// If `true` is returned, all future calls to receive the message are guaranteed to return
    /// [`RecvError`]. And future calls to this method is guaranteed to also return `true`.
    pub fn is_closed(&self) -> bool {
        let channel = self.channel.as_ref().unwrap().get();

        // ORDERING: We *chose* a Relaxed ordering here as it is sufficient to
        // enforce the method's contract.
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the sender might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    /// Returns true if there is a message in the channel, ready to be received.
    ///
    /// If `true` is returned, the next call to receive the message is guaranteed to return
    /// the message immediately.
    pub fn has_message(&self) -> bool {
        let channel = self.channel.as_ref().unwrap().get();

        // ORDERING: An acquire ordering is used to guarantee no subsequent loads is reordered
        // before this one. This upholds the contract that if true is returned, the next call to
        // receive the message is guaranteed to also observe the `MESSAGE` state and return the
        // message immediately.
        matches!(channel.state.load(Ordering::Acquire), MESSAGE)
    }

    /// Checks if there is a message in the channel without blocking. Returns:
    ///
    /// * `Ok(message)` if there was a message in the channel.
    /// * `Err(TryRecvError::Empty)` if the [`Sender`] is alive, but has not yet sent a message.
    /// * `Err(TryRecvError::Disconnected)` if the [`Sender`] was dropped before sending anything or
    ///   if the message has already been extracted by a previous `try_recv` call.
    ///
    /// If a message is returned, the channel is disconnected and any subsequent receive operation
    /// using this receiver will return an error: [`TryRecvError::Disconnected`] for `try_recv`,
    /// or [`RecvError::Disconnected`] for [`recv`](Receiver::into_future).
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let channel = self.channel.as_ref().unwrap().get();

        match channel.state.load(Ordering::Acquire) {
            MESSAGE => {
                channel.state.store(DISCONNECTED, Ordering::Relaxed);
                Ok(unsafe { channel.take_message() })
            }
            EMPTY => Err(TryRecvError::Empty),
            DISCONNECTED => Err(TryRecvError::Disconnected),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(channel_ref) = self.channel.take() else {
            return;
        };
        let channel = channel_ref.get();

        channel.disconnect_receiver();
    }
}

/// A future that completes when the message is sent from the associated [`Sender`], or the
/// [`Sender`] is dropped before sending a message.
pub struct Recv<T> {
    channel: Option<ChannelRef<T>>,
}

impl<T> fmt::Debug for Recv<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recv").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Recv<T> {}

impl<T> Future for Recv<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(channel_ref) = self.channel.as_ref() else {
            return Poll::Ready(Err(RecvError::Disconnected));
        };

        let result = channel_ref.get().poll_receiver(cx.waker());

        if result.is_ready() {
            self.channel.take();
        }
        result
    }
}

impl<T> Drop for Recv<T> {
    fn drop(&mut self) {
        let Some(channel_ref) = self.channel.take() else {
            return;
        };

        channel_ref.get().disconnect_receiver();
    }
}

/// One of the two allocation references owned by the channel endpoints.
struct ChannelRef<T>(NonNull<Channel<T>>);

impl<T> ChannelRef<T> {
    fn get(&self) -> &Channel<T> {
        // SAFETY: Construction assigns exactly one reference to each endpoint, and Drop releases
        // it only after the endpoint has finished accessing the channel.
        unsafe { self.0.as_ref() }
    }
}

impl<T> Drop for ChannelRef<T> {
    fn drop(&mut self) {
        // SAFETY: This value owns exactly one allocation reference.
        unsafe { release(self.0) };
    }
}

/// Internal channel data structure.
///
/// The [`channel`] method allocates and puts one instance of this struct on the heap for each
/// oneshot channel instance. The struct holds:
///
/// * One allocation reference for each endpoint.
/// * The current state of the channel.
/// * The message in the channel. This memory is uninitialized until the message is sent.
/// * An atomically owned waker for the task currently receiving on this channel.
///
/// The state only describes stable, externally observable ownership. Allocation lifetime is kept
/// separate so neither endpoint has to wait for the other endpoint to finish a state transition.
struct Channel<T> {
    refs: AtomicUsize,
    // Native-width RMWs avoid the fallback sequences required for sub-word atomics on some
    // targets.
    state: AtomicUsize,
    message: UnsafeCell<MaybeUninit<T>>,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

// SAFETY: The message and waker slots are only accessed after an atomic state transition transfers
// exclusive ownership to one endpoint.
unsafe impl<T: Send> Sync for Channel<T> {}

impl<T> Channel<T> {
    const fn new() -> Self {
        Self {
            refs: AtomicUsize::new(2),
            state: AtomicUsize::new(EMPTY),
            message: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    fn poll_receiver(&self, waker: &Waker) -> Poll<Result<T, RecvError>> {
        match self.state.load(Ordering::Acquire) {
            EMPTY => {
                // SAFETY: EMPTY means no published waker exists. A concurrent sender can publish a
                // terminal state but will not access a waker that has not reached WAITING.
                unsafe { self.register_waker(waker.clone()) }
            }
            WAITING => {
                match self.state.compare_exchange(
                    WAITING,
                    EMPTY,
                    Ordering::Acquire,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // SAFETY: The successful transition gives this receiver ownership of the
                        // previously registered waker.
                        unsafe { self.drop_waker() };

                        // SAFETY: The state is EMPTY and this is the only receiver.
                        unsafe { self.register_waker(waker.clone()) }
                    }
                    Err(MESSAGE) => {
                        // The sender replaced WAITING and therefore owns the registered waker.
                        self.take_sent_message()
                    }
                    Err(DISCONNECTED) => Poll::Ready(Err(RecvError::Disconnected)),
                    Err(state) => unreachable!("unexpected channel state: {state}"),
                }
            }
            MESSAGE => self.take_sent_message(),
            DISCONNECTED => Poll::Ready(Err(RecvError::Disconnected)),
            state => unreachable!("unexpected channel state: {state}"),
        }
    }

    fn disconnect_receiver(&self) {
        match self.state.swap(DISCONNECTED, Ordering::AcqRel) {
            EMPTY => {}
            WAITING => {
                // SAFETY: Replacing WAITING transfers ownership of the initialized waker to this
                // receiver. The acquire half of the swap synchronizes with publication.
                unsafe { self.drop_waker() };
            }
            MESSAGE => {
                // SAFETY: The acquire half of the swap synchronizes with the sender's publication
                // of MESSAGE, so this receiver exclusively owns the initialized message.
                unsafe { self.drop_message() };
            }
            DISCONNECTED => {}
            state => unreachable!("unexpected channel state: {state}"),
        }
    }

    unsafe fn register_waker(&self, waker: Waker) -> Poll<Result<T, RecvError>> {
        // SAFETY: The caller owns the unpublished waker slot while the state is EMPTY.
        unsafe { (*self.waker.get()).write(waker) };

        match self
            .state
            .compare_exchange(EMPTY, WAITING, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => Poll::Pending,
            Err(MESSAGE) => {
                // SAFETY: The sender observed EMPTY and did not access this unpublished waker.
                unsafe { self.drop_waker() };
                self.take_sent_message()
            }
            Err(DISCONNECTED) => {
                // SAFETY: The sender observed EMPTY and did not access this unpublished waker.
                unsafe { self.drop_waker() };
                Poll::Ready(Err(RecvError::Disconnected))
            }
            Err(state) => unreachable!("unexpected channel state: {state}"),
        }
    }

    fn take_sent_message(&self) -> Poll<Result<T, RecvError>> {
        // A sender never changes the state after publishing MESSAGE, and there is only one
        // receiver, so this store exclusively claims the initialized message.
        self.state.store(DISCONNECTED, Ordering::Relaxed);

        // SAFETY: The caller has acquired the sender's publication of MESSAGE, and the state
        // transition above gives this receiver exclusive ownership.
        Poll::Ready(Ok(unsafe { self.take_message() }))
    }

    #[inline(always)]
    unsafe fn write_message(&self, message: T) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.as_mut_ptr().write(message);
        }
    }

    #[inline(always)]
    unsafe fn drop_message(&self) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.assume_init_drop();
        }
    }

    #[inline(always)]
    unsafe fn take_message(&self) -> T {
        unsafe { ptr::read(self.message.get()).assume_init() }
    }

    #[inline(always)]
    unsafe fn drop_waker(&self) {
        unsafe { (*self.waker.get()).assume_init_drop() };
    }

    #[inline(always)]
    unsafe fn take_waker(&self) -> Waker {
        unsafe { ptr::read(self.waker.get()).assume_init() }
    }
}

impl<T> Drop for Channel<T> {
    fn drop(&mut self) {
        match *self.state.get_mut() {
            MESSAGE => {
                // SAFETY: Exclusive access to Channel proves that no endpoint can access the slot.
                unsafe { self.message.get_mut().assume_init_drop() };
            }
            WAITING => {
                // SAFETY: Exclusive access to Channel proves that no endpoint can access the slot.
                unsafe { self.waker.get_mut().assume_init_drop() };
            }
            EMPTY | DISCONNECTED => {}
            state => unreachable!("unexpected channel state: {state}"),
        }
    }
}

unsafe fn release<T>(channel_ptr: NonNull<Channel<T>>) {
    // SAFETY: The caller owns one reference and keeps the allocation alive through this RMW.
    let channel = unsafe { channel_ptr.as_ref() };
    if channel.refs.fetch_sub(1, Ordering::Release) == 1 {
        fence(Ordering::Acquire);

        // SAFETY: The transition from one reference to zero gives this thread exclusive ownership
        // of the allocation, and every endpoint performs release as its final channel operation.
        unsafe { drop(Box::from_raw(channel_ptr.as_ptr())) };
    }
}

/// An error returned when trying to send on a closed channel. Returned from
/// [`Sender::send`] if the corresponding [`Receiver`] has already been dropped.
///
/// The message that could not be sent can be retrieved again with [`SendError::into_inner`].
pub struct SendError<T> {
    message: T,
}

impl<T> SendError<T> {
    fn new(message: T) -> Self {
        Self { message }
    }

    /// Get a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        &self.message
    }

    /// Consumes the error and returns the message that failed to be sent.
    pub fn into_inner(self) -> T {
        self.message
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed channel")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned by [`Receiver::try_recv`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TryRecvError {
    /// This channel is currently empty, but the sender has not yet disconnected, so data may yet
    /// become available.
    Empty,
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a closed channel",
        })
    }
}

impl std::error::Error for TryRecvError {}

/// An error returned when awaiting the message via [`Receiver`].
///
/// This error indicates that the corresponding [`Sender`] was dropped before sending any message.
/// Note that if a message was already received (e.g., via [`Receiver::try_recv`]), subsequent
/// `try_recv` calls will return [`TryRecvError::Disconnected`] instead.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RecvError {
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a closed channel")
    }
}

impl std::error::Error for RecvError {}

/// The initial channel state. Active while both endpoints are still alive, no message has been
/// sent, and the receiver is not receiving.
const EMPTY: usize = 0;
/// A message has been sent to the channel, but the receiver has not yet read it.
const MESSAGE: usize = 1;
/// The channel has been closed. This means that either the sender or receiver has been dropped,
/// or the message sent to the channel has already been received.
const DISCONNECTED: usize = 2;
/// The receiver is pending and has published a waker for the sender to take.
const WAITING: usize = 3;

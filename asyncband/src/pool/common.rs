// Copyright 2025 FastLabs Developers
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
//
// This file contains code ported from Fastpool 1.1.1.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/fast/fastpool/blob/e4c65f1ed38395abc58d68eda8bd09925c13028e/fastpool/src/common.rs

use std::future::Future;
use std::time::Instant;

/// Lifecycle metadata for a pooled object.
#[derive(Debug, Clone, Copy)]
pub struct ObjectStatus {
    created: Instant,
    last_returned: Option<Instant>,
    recycle_count: usize,
}

impl Default for ObjectStatus {
    fn default() -> Self {
        Self {
            created: Instant::now(),
            last_returned: None,
            recycle_count: 0,
        }
    }
}

impl ObjectStatus {
    /// Returns the instant when this object was created.
    pub fn created(&self) -> Instant {
        self.created
    }

    /// Returns the instant when this object was last returned to the pool.
    ///
    /// If the object has not been returned yet, this returns its creation time. While an object is
    /// checked out, the value therefore describes the end of its previous use, if any.
    pub fn last_used(&self) -> Instant {
        self.last_returned.unwrap_or(self.created)
    }

    /// Returns the number of successful checkouts from the idle queue.
    pub fn recycle_count(&self) -> usize {
        self.recycle_count
    }

    pub(super) fn mark_recycled(&mut self) {
        self.recycle_count += 1;
    }

    pub(super) fn mark_returned(&mut self) {
        self.last_returned = Some(Instant::now());
    }
}

/// The result of a pool's `retain` call.
#[derive(Debug)]
#[non_exhaustive]
pub struct RetainResult<T> {
    /// The number of objects retained.
    pub retained: usize,
    /// The removed objects, after their detachment hooks have run.
    pub removed: Vec<T>,
}

/// A trait whose instance creates new objects and recycles existing ones.
pub trait ManageObject: Send + Sync {
    /// The type of objects that this instance creates and recycles.
    type Object: Send;

    /// The type of errors that this instance can return.
    type Error: Send;

    /// Creates a new object.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when an object cannot be created. Pool checkout
    /// and replenishment propagate this error to their caller.
    ///
    /// # Cancellation
    ///
    /// The pool may drop this future when checkout or replenishment is cancelled. Implementations
    /// must arrange cleanup of resources owned by an unfinished creation attempt.
    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send;

    /// Checks whether `o` can be reused for another checkout, refreshing it if needed.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the object should not be reused. The pool
    /// detaches the object and retries checkout; it does not return this error to the caller.
    /// Record any required diagnostics in this implementation.
    ///
    /// # Cancellation
    ///
    /// The pool may drop this future when checkout is cancelled. Mutations already made to `o`
    /// are not rolled back. [`RecycleCancelledStrategy`] determines whether the object is detached
    /// or returned to the idle queue; returning it requires the object to remain valid after an
    /// interrupted check.
    fn is_recyclable(
        &self,
        o: &mut Self::Object,
        status: &ObjectStatus,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// A callback invoked when an object is detached from a live pool.
    ///
    /// This includes explicit detachment, failed or cancelled recycling, and removal through
    /// `retain`. The callback runs without the pool's internal lock held. It is not invoked when
    /// the pool itself is dropped or when an object can no longer reach its pool.
    ///
    /// If this instance does not hold any references to the object, the default implementation can
    /// be used, which does nothing.
    fn on_detached(&self, _o: &mut Self::Object) {}
}

/// Strategy for dequeuing objects from the object pool.
#[derive(Debug, Default, Clone, Copy)]
pub enum QueueStrategy {
    /// First in first out.
    ///
    /// This strategy behaves like a queue.
    #[default]
    Fifo,
    /// Last in first out.
    ///
    /// This strategy behaves like a stack.
    Lifo,
}

/// Strategy to apply when object recycling is cancelled.
///
/// This enum controls the behavior when the recycling process (specifically the
/// [`ManageObject::is_recyclable`] check) is cancelled; for example, when the
/// `get()` future is dropped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RecycleCancelledStrategy {
    /// Detach the object from the pool.
    ///
    /// This is the safest option. If the recycling check is cancelled, we assume the object might
    /// be in an unknown state or that the check was taking too long for a reason. The object will
    /// detach from the pool.
    #[default]
    Detach,

    /// Return the object to the pool for potential reuse.
    ///
    /// This assumes that interrupting the check does not invalidate the object. The object is put
    /// back into the pool.
    ReturnToPool,
}

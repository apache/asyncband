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

use std::future::Future;
use std::time::Instant;

/// Statistics regarding an object returned by the pool.
#[derive(Debug, Clone, Copy)]
pub struct ObjectStatus {
    created: Instant,
    pub(crate) recycled: Option<Instant>,
    pub(crate) recycle_count: usize,
}

impl Default for ObjectStatus {
    fn default() -> Self {
        Self {
            created: Instant::now(),
            recycled: None,
            recycle_count: 0,
        }
    }
}

impl ObjectStatus {
    /// Returns the instant when this object was created.
    pub fn created(&self) -> Instant {
        self.created
    }

    /// Returns the instant when this object was last used.
    pub fn last_used(&self) -> Instant {
        self.recycled.unwrap_or(self.created)
    }

    /// Returns the number of times the object was recycled.
    pub fn recycle_count(&self) -> usize {
        self.recycle_count
    }
}

/// A trait whose instance creates new objects and recycles existing ones.
pub trait ManageObject: Send + Sync {
    /// The type of objects that this instance creates and recycles.
    type Object: Send;

    /// The type of errors that this instance can return.
    type Error: Send;

    /// Creates a new object.
    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send;

    /// Whether the object `o` is recyclable.
    ///
    /// Returns `Ok(())` if the object is recyclable; otherwise, returns an error.
    fn is_recyclable(
        &self,
        o: &mut Self::Object,
        status: &ObjectStatus,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// A callback invoked when an object is detached from the pool.
    ///
    /// If this instance does not hold any references to the object, then the default
    /// implementation can be used which does nothing.
    fn on_detached(&self, _o: &mut Self::Object) {}
}

/// Queue strategy when dequeuing objects from the object pool.
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

/// Strategy when recycling object has been cancelled.
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

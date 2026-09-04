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
// See the project LICENSE file for the exact upstream revision and source path.

//! Bounded object pools.
//!
//! A bounded pool uses a [`ManageObject`] implementation to create, validate, and detach objects.
//! Objects cannot be inserted manually.
//!
//! The pool is bounded by the `max_size` config option of [`PoolConfig`]. If the pool reaches the
//! maximum size, additional [`Pool::get`] calls wait until an object is returned to or detached
//! from the pool.
//!
//! [`Pool::new`] returns an [`Arc`], allowing background maintenance code to hold a [`Weak`] and
//! terminate naturally when application owners drop the pool. Scheduling that maintenance remains
//! the caller's responsibility.
//!
//! Bounded pools are useful for pooling database connections.
//!
//! ## Examples
//!
//! ```
//! use asyncband::pool::ManageObject;
//! use asyncband::pool::ObjectStatus;
//! use asyncband::pool::bounded::Pool;
//! use asyncband::pool::bounded::PoolConfig;
//!
//! struct Compute;
//! impl Compute {
//!     async fn do_work(&self) -> i32 {
//!         42
//!     }
//! }
//!
//! struct Manager;
//! impl ManageObject for Manager {
//!     type Object = Compute;
//!     type Error = ();
//!
//!     async fn create(&self) -> Result<Self::Object, Self::Error> {
//!         Ok(Compute)
//!     }
//!
//!     async fn is_recyclable(
//!         &self,
//!         _object: &mut Self::Object,
//!         _status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::new(PoolConfig::new(16), Manager);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.do_work().await, 42);
//! # }
//! ```

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

use crate::internal::mutex::Mutex;
use crate::pool::ManageObject;
use crate::pool::ObjectStatus;
use crate::pool::QueueStrategy;
use crate::pool::RecycleCancelledStrategy;
use crate::pool::RetainResult;
use crate::pool::state::ObjectState;
use crate::pool::state::PoolState;
use crate::semaphore::OwnedSemaphorePermit;
use crate::semaphore::Semaphore;

/// The configuration of [`Pool`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Maximum size of the [`Pool`]. Must be greater than zero.
    pub max_size: usize,

    /// Queue strategy of the [`Pool`].
    ///
    /// Determines the order of objects being queued and dequeued.
    pub queue_strategy: QueueStrategy,

    /// Strategy to apply when object recycling is cancelled.
    pub recycle_cancelled_strategy: RecycleCancelledStrategy,
}

impl PoolConfig {
    /// Creates a new [`PoolConfig`] for a pool with the given maximum size.
    ///
    /// [`Pool::new`] panics if `max_size` is zero.
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            queue_strategy: QueueStrategy::default(),
            recycle_cancelled_strategy: RecycleCancelledStrategy::default(),
        }
    }

    /// Returns a new [`PoolConfig`] with the specified queue strategy.
    #[must_use = "this method returns the updated pool configuration"]
    pub fn with_queue_strategy(mut self, queue_strategy: QueueStrategy) -> Self {
        self.queue_strategy = queue_strategy;
        self
    }

    /// Returns a new [`PoolConfig`] with the specified recycle cancelled strategy.
    #[must_use = "this method returns the updated pool configuration"]
    pub fn with_recycle_cancelled_strategy(
        mut self,
        recycle_cancelled_strategy: RecycleCancelledStrategy,
    ) -> Self {
        self.recycle_cancelled_strategy = recycle_cancelled_strategy;
        self
    }
}

/// The current pool status.
///
/// See [`Pool::status`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolStatus {
    /// The maximum size of the pool.
    pub max_size: usize,

    /// The number of successfully created objects that have not been detached.
    pub current_size: usize,

    /// The number of objects currently available for checkout.
    pub idle_count: usize,
}

/// Generic runtime-agnostic object pool with a maximum size.
///
/// See the [module level documentation](self) for more.
pub struct Pool<M: ManageObject> {
    config: PoolConfig,
    manager: M,

    /// A semaphore that reserves capacity for checkouts and object creation.
    permits: Arc<Semaphore>,
    /// The objects tracked by the pool.
    slots: Mutex<PoolState<M::Object>>,
}

impl<M> std::fmt::Debug for Pool<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.slots)
            .field("config", &self.config)
            .field("permits", &self.permits)
            .finish()
    }
}

impl<M: ManageObject> Pool<M> {
    /// Creates a new [`Pool`].
    ///
    /// # Panics
    ///
    /// Panics if `config.max_size` is zero.
    pub fn new(config: PoolConfig, manager: M) -> Arc<Self> {
        assert!(
            config.max_size > 0,
            "bounded pool max_size must be greater than zero"
        );

        let permits = Arc::new(Semaphore::new(config.max_size));
        let slots = Mutex::new(PoolState::new());

        Arc::new(Self {
            config,
            manager,
            permits,
            slots,
        })
    }

    /// Creates objects until the pool approaches `target_idle` idle objects.
    ///
    /// The pool reserves only capacity that is immediately available and never waits for
    /// checked-out objects. Existing idle objects count toward the target, and the pool's
    /// maximum size is never exceeded. Targets above the maximum size are treated as the maximum.
    /// Concurrent calls and checkouts can change the observed idle count while this method is
    /// running, so the target is best effort rather than a postcondition.
    ///
    /// Returns the number of objects created. If [`ManageObject::create`] fails, objects created by
    /// this call before the failure remain in the pool and the error is returned.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this operation releases all reserved capacity. Objects created before
    /// cancellation remain idle in the pool; the in-progress [`ManageObject::create`] future is
    /// dropped.
    pub async fn replenish_to(&self, target_idle: usize) -> Result<usize, M::Error> {
        let target_idle = target_idle.min(self.config.max_size);
        let Some(mut reservation) = ReplenishReservation::reserve_up_to(&self.permits, target_idle)
        else {
            return Ok(0);
        };

        let (idle_count, available_slots) = {
            let slots = self.slots.lock();
            let idle_count = slots.idle_count();

            // Idle objects occupy pool slots without holding permits. Available permits plus this
            // reservation represent capacity not committed to other checkouts, creations, or
            // replenishments; subtracting idle objects leaves the slots this call may create.
            let uncommitted_capacity = self
                .permits
                .available_permits()
                .checked_add(reservation.permits())
                .expect("invariant broken: semaphore capacity must not overflow");
            let available_slots = uncommitted_capacity.saturating_sub(idle_count);
            (idle_count, available_slots)
        };
        let to_create = target_idle
            .saturating_sub(idle_count)
            .min(reservation.permits())
            .min(available_slots);
        reservation.release(reservation.permits() - to_create);

        let mut replenished = 0;
        for _ in 0..to_create {
            let object = self.manager.create().await?;
            {
                let mut slots = self.slots.lock();
                slots.add_idle(ObjectState::new(object));
            }
            replenished += 1;
            reservation.release(1);
        }

        Ok(replenished)
    }

    /// Retrieves an [`Object`] from this [`Pool`].
    ///
    /// If the pool has reached its maximum size and has no idle object, this method waits until an
    /// object is returned to or detached from the pool.
    ///
    /// # Cancel safety
    ///
    /// Cancelling while waiting for capacity or creating a new object restores the reserved pool
    /// capacity. Cancelling while [`ManageObject::is_recyclable`] is checking an idle object
    /// follows [`PoolConfig::recycle_cancelled_strategy`].
    pub async fn get(self: &Arc<Self>) -> Result<Object<M>, M::Error> {
        let permit = self.permits.clone().acquire_owned(1).await;

        let object = loop {
            let existing = self.slots.lock().pop(self.config.queue_strategy);

            match existing {
                None => {
                    let object = self.manager.create().await?;
                    let state = ObjectState::new(object);
                    self.slots.lock().add_active();
                    break Object {
                        state: Some(state),
                        permit,
                        pool: Arc::downgrade(self),
                    };
                }
                Some(object) => {
                    let mut unready_object = UnreadyObject {
                        state: Some(object),
                        pool: Arc::downgrade(self),
                        recycle_cancelled_strategy: self.config.recycle_cancelled_strategy,
                    };

                    let state = unready_object.state();
                    let status = state.status;
                    if self
                        .manager
                        .is_recyclable(&mut state.o, &status)
                        .await
                        .is_ok()
                    {
                        state.status.mark_recycled();
                        break unready_object.ready(permit);
                    } else {
                        // We need to manually detach here as the drop implementation
                        // depends on the recycle cancelled strategy.
                        unready_object.detach();
                    }
                }
            };
        };

        Ok(object)
    }

    /// Retains idle objects for which `f` returns `true`.
    ///
    /// Checked-out objects are skipped and may return to the pool later. The predicate runs while
    /// the pool is locked and must not call back into it; detachment hooks run after the lock is
    /// released.
    ///
    /// The following example starts a background task that runs every 30 seconds and removes
    /// objects from the pool that have not been used for more than one minute. The task will
    /// terminate if the pool is dropped.
    ///
    /// ```rust,no_run
    /// # use std::convert::Infallible;
    /// # use std::sync::Arc;
    /// # use std::time::Duration;
    /// # use asyncband::pool::ManageObject;
    /// # use asyncband::pool::ObjectStatus;
    /// # use asyncband::pool::bounded::Pool;
    /// # use asyncband::pool::bounded::PoolConfig;
    /// let interval = Duration::from_secs(30);
    /// let max_age = Duration::from_secs(60);
    ///
    /// # struct Manager;
    /// # impl ManageObject for Manager {
    /// #     type Object = u32;
    /// #     type Error = Infallible;
    /// #
    /// #     async fn create(&self) -> Result<Self::Object, Self::Error> {
    /// #         Ok(0)
    /// #     }
    /// #
    /// #     async fn is_recyclable(
    /// #         &self,
    /// #         _object: &mut Self::Object,
    /// #         _status: &ObjectStatus,
    /// #     ) -> Result<(), Self::Error> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # let pool = Pool::new(PoolConfig::new(16), Manager);
    /// let weak_pool = Arc::downgrade(&pool);
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::time::sleep(interval).await;
    ///         if let Some(pool) = weak_pool.upgrade() {
    ///             pool.retain(|_, status| status.last_used().elapsed() < max_age);
    ///         } else {
    ///             break;
    ///         }
    ///     }
    /// });
    /// ```
    pub fn retain(
        &self,
        f: impl FnMut(&mut M::Object, ObjectStatus) -> bool,
    ) -> RetainResult<M::Object> {
        let mut result = {
            let mut slots = self.slots.lock();
            slots.retain(f)
        };
        for object in &mut result.removed {
            self.manager.on_detached(object);
        }
        result
    }

    /// Returns a consistent snapshot of the objects currently tracked by the pool.
    pub fn status(&self) -> PoolStatus {
        let slots = self.slots.lock();

        PoolStatus {
            max_size: self.config.max_size,
            current_size: slots.current_size(),
            idle_count: slots.idle_count(),
        }
    }

    fn return_object(&self, mut state: ObjectState<M::Object>) {
        state.status.mark_returned();
        self.restore_idle(state);
    }

    fn restore_idle(&self, state: ObjectState<M::Object>) {
        let mut slots = self.slots.lock();

        assert!(
            slots.current_size() <= self.config.max_size,
            "invariant broken: current_size <= max_size (actual: {} <= {})",
            slots.current_size(),
            self.config.max_size,
        );

        slots.return_idle(state);
    }

    fn detach_object(&self, o: &mut M::Object) {
        let mut slots = self.slots.lock();

        assert!(
            slots.current_size() <= self.config.max_size,
            "invariant broken: current_size <= max_size (actual: {} <= {})",
            slots.current_size(),
            self.config.max_size,
        );

        slots.detach();
        drop(slots);

        self.manager.on_detached(o);
    }
}

// Temporarily removes capacity while `replenish_to` creates objects. Idle objects do not consume
// semaphore permits, so successful insertions release their reservation. Dropping the guard
// restores any unfinished capacity after an error or cancellation.
struct ReplenishReservation<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

impl<'a> ReplenishReservation<'a> {
    fn reserve_up_to(semaphore: &'a Semaphore, up_to: usize) -> Option<Self> {
        let permits = semaphore.drain_permits(up_to);
        (permits != 0).then_some(Self { semaphore, permits })
    }

    fn permits(&self) -> usize {
        self.permits
    }

    fn release(&mut self, permits: usize) {
        assert!(
            permits <= self.permits,
            "cannot release more permits than this reservation holds"
        );
        self.permits -= permits;
        self.semaphore.release(permits);
    }
}

impl Drop for ReplenishReservation<'_> {
    fn drop(&mut self) {
        self.semaphore.release(self.permits);
    }
}

/// A wrapper of the actual pooled object.
///
/// This object implements [`Deref`] and [`DerefMut`]. You can use it as if it was of type
/// `M::Object`.
///
/// This object implements [`Drop`] that returns the underlying object to the pool on drop. You may
/// call [`Object::detach`] to detach the object from the pool before dropping it.
pub struct Object<M: ManageObject> {
    state: Option<ObjectState<M::Object>>,
    permit: OwnedSemaphorePermit,
    pool: Weak<Pool<M>>,
}

impl<M> std::fmt::Debug for Object<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("state", &self.state)
            .field("permit", &self.permit)
            .finish()
    }
}

impl<M: ManageObject> Drop for Object<M> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.return_object(state);
            }
        }
    }
}

impl<M: ManageObject> Deref for Object<M> {
    type Target = M::Object;
    fn deref(&self) -> &M::Object {
        // INVARIANT: `state` is `Some` until this object is detached or dropped.
        &self.state.as_ref().unwrap().o
    }
}

impl<M: ManageObject> DerefMut for Object<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // INVARIANT: `state` is `Some` until this object is detached or dropped.
        &mut self.state.as_mut().unwrap().o
    }
}

impl<M: ManageObject> AsRef<M::Object> for Object<M> {
    fn as_ref(&self) -> &M::Object {
        self
    }
}

impl<M: ManageObject> AsMut<M::Object> for Object<M> {
    fn as_mut(&mut self) -> &mut M::Object {
        self
    }
}

impl<M: ManageObject> Object<M> {
    /// Detaches the object from the [`Pool`].
    ///
    /// This reduces the size of the pool by one.
    ///
    /// If the pool still exists, its manager may modify the detached object in
    /// [`ManageObject::on_detached`].
    pub fn detach(mut self) -> M::Object {
        // INVARIANT: `state` is `Some` until this object is detached or dropped.
        let mut o = self.state.take().unwrap().o;
        if let Some(pool) = self.pool.upgrade() {
            pool.detach_object(&mut o);
        }
        o
    }

    /// Returns the status of the object.
    pub fn status(&self) -> ObjectStatus {
        // INVARIANT: `state` is `Some` until this object is detached or dropped.
        self.state.as_ref().unwrap().status
    }
}

/// A wrapper of ObjectState used during the `is_recyclable` check in `Pool::get`.
///
/// If the check passes, the object is converted to a ready `Object` via `ready()`.
/// If the check fails, `detach()` should be called to permanently remove the object
/// from the pool. If dropped without calling either method (due to being cancelled),
/// the behavior depends on the pool's [`RecycleCancelledStrategy`] configuration.
struct UnreadyObject<M: ManageObject> {
    state: Option<ObjectState<M::Object>>,
    pool: Weak<Pool<M>>,
    recycle_cancelled_strategy: RecycleCancelledStrategy,
}

impl<M: ManageObject> Drop for UnreadyObject<M> {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                match self.recycle_cancelled_strategy {
                    RecycleCancelledStrategy::Detach => {
                        pool.detach_object(&mut state.o);
                    }
                    RecycleCancelledStrategy::ReturnToPool => {
                        pool.restore_idle(state);
                    }
                }
            }
        }
    }
}

impl<M: ManageObject> UnreadyObject<M> {
    fn ready(mut self, permit: OwnedSemaphorePermit) -> Object<M> {
        // INVARIANT: `state` is `Some` until this object becomes ready, detaches, or is dropped.
        let state = Some(self.state.take().unwrap());
        let pool = self.pool.clone();
        Object {
            state,
            permit,
            pool,
        }
    }

    fn detach(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.detach_object(&mut state.o);
            }
        }
    }

    fn state(&mut self) -> &mut ObjectState<M::Object> {
        // INVARIANT: `state` is `Some` until this object becomes ready, detaches, or is dropped.
        self.state.as_mut().unwrap()
    }
}

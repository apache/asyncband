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

//! Unbounded object pools.
//!
//! An unbounded pool, on the other hand, allows you to put objects to the pool manually. You can
//! use it like Go's [`sync.Pool`](https://pkg.go.dev/sync#Pool).
//!
//! To configure a factory for creating objects when the pool is empty, like `sync.Pool`'s `New`,
//! you can create the unbounded pool via [`Pool::new`](Pool::new) with an
//! implementation of [`ManageObject`].
//!
//! ## Examples
//!
//! Read the following simple demos or more complex examples in the examples directory.
//!
//! 1. Create an unbounded pool with [`NeverManageObject`]:
//!
//! ```
//! use asyncband::pool::unbounded::Pool;
//! use asyncband::pool::unbounded::PoolConfig;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::<Vec<u8>>::never_manage(PoolConfig::default());
//!
//! let result = pool.get().await;
//! assert_eq!(result.unwrap_err().to_string(), "unbounded pool is empty");
//!
//! pool.extend_one(Vec::with_capacity(1024));
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.capacity(), 1024);
//! drop(o);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.capacity(), 1024);
//! let result = pool.get().await;
//! assert_eq!(result.unwrap_err().to_string(), "unbounded pool is empty");
//! # }
//! ```
//!
//! 2. Create an unbounded pool with a custom [`ManageObject`] (object factory):
//!
//! ```
//! use asyncband::pool::ManageObject;
//! use asyncband::pool::ObjectStatus;
//! use asyncband::pool::unbounded::Pool;
//! use asyncband::pool::unbounded::PoolConfig;
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
//!         o: &mut Self::Object,
//!         status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::new(PoolConfig::default(), Manager);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.do_work().await, 42);
//! # }
//! ```

use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

use crate::pool::ManageObject;
use crate::pool::ObjectStatus;
use crate::pool::QueueStrategy;
use crate::pool::RecycleCancelledStrategy;
use crate::pool::RetainResult;
use crate::pool::mutex::Mutex;
use crate::pool::retain_spec;

/// The configuration of [`Pool`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Queue strategy of the [`Pool`].
    ///
    /// Determines the order of objects being queued and dequeued.
    pub queue_strategy: QueueStrategy,

    /// Strategy when recycling object has been cancelled.
    pub recycle_cancelled_strategy: RecycleCancelledStrategy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolConfig {
    /// Creates a new [`PoolConfig`].
    pub fn new() -> Self {
        Self {
            queue_strategy: QueueStrategy::default(),
            recycle_cancelled_strategy: RecycleCancelledStrategy::default(),
        }
    }

    /// Returns a new [`PoolConfig`] with the specified queue strategy.
    pub fn with_queue_strategy(mut self, queue_strategy: QueueStrategy) -> Self {
        self.queue_strategy = queue_strategy;
        self
    }

    /// Returns a new [`PoolConfig`] with the specified recycle cancelled strategy.
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
    /// The current size of the pool.
    pub current_size: usize,

    /// The number of idle objects in the pool.
    pub idle_count: usize,
}

/// The default [`ManageObject`] implementation for unbounded pool.
///
/// * [`NeverManageObject::create`] always returns [`PoolIsEmpty`] so that [`Pool::get`] would get
///   the error if no object is in the pool.
/// * [`NeverManageObject::is_recyclable`] always returns `Ok(())` so that any object is always
///   recyclable.
#[derive(Debug, Copy, Clone)]
pub struct NeverManageObject<T: Send + Sync> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: Send + Sync> Default for NeverManageObject<T> {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

/// The error returned by [`NeverManageObject::create`].
pub struct PoolIsEmpty(());

impl std::fmt::Debug for PoolIsEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unbounded pool is empty")
    }
}

impl std::fmt::Display for PoolIsEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for PoolIsEmpty {}

impl<T: Send + Sync> ManageObject for NeverManageObject<T> {
    type Object = T;
    type Error = PoolIsEmpty;

    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send {
        std::future::ready(Err(PoolIsEmpty(())))
    }

    fn is_recyclable(
        &self,
        _: &mut Self::Object,
        _: &ObjectStatus,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

/// Generic runtime-agnostic unbounded object pool.
///
/// See the [module level documentation](self) for more.
pub struct Pool<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    config: PoolConfig,
    manager: M,

    /// A deque that holds the objects.
    slots: Mutex<PoolDeque<ObjectState<T>>>,
}

#[derive(Debug)]
struct PoolDeque<T> {
    deque: VecDeque<T>,
    current_size: usize,
}

impl<T, M> std::fmt::Debug for Pool<T, M>
where
    T: std::fmt::Debug,
    M: ManageObject<Object = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.slots)
            .field("config", &self.config)
            .finish()
    }
}

// Methods for `Pool` with `NeverManageObject`.
impl<T: Send + Sync> Pool<T> {
    /// Creates a new [`Pool`] from config and the [`NeverManageObject`].
    pub fn never_manage(config: PoolConfig) -> Arc<Self> {
        Self::new(config, NeverManageObject::<T>::default())
    }

    /// Retrieves an [`Object`] from this [`Pool`], or creates a new one with the passed-in async
    /// closure, if the pool is empty.
    ///
    /// This method should be called with a pool wrapped in an [`Arc`].
    ///
    /// This method only exists for [`NeverManageObject`] pools. If you provide a custom
    /// [`ManageObject`] implementation, you should use [`Pool::get`] instead, and it will call
    /// [`ManageObject::create`] to create a new object if the pool is empty.
    pub async fn get_or_create<E, F>(self: &Arc<Self>, f: F) -> Result<Object<T>, E>
    where
        F: AsyncFnOnce() -> Result<T, E> + Send,
    {
        let existing = match self.config.queue_strategy {
            QueueStrategy::Fifo => self.slots.lock().deque.pop_front(),
            QueueStrategy::Lifo => self.slots.lock().deque.pop_back(),
        };

        match existing {
            None => {
                let object = f().await?;
                let state = ObjectState {
                    o: object,
                    status: ObjectStatus::default(),
                };
                self.slots.lock().current_size += 1;
                Ok(Object {
                    state: Some(state),
                    pool: Arc::downgrade(self),
                })
            }
            Some(mut state) => {
                state.status.recycle_count += 1;
                state.status.recycled = Some(std::time::Instant::now());
                Ok(Object {
                    state: Some(state),
                    pool: Arc::downgrade(self),
                })
            }
        }
    }
}

impl<T, M: ManageObject<Object = T>> Pool<T, M> {
    /// Creates a new [`Pool`] with config and the specified [`ManageObject`].
    pub fn new(config: PoolConfig, manager: M) -> Arc<Self> {
        let slots = Mutex::new(PoolDeque {
            deque: VecDeque::new(),
            current_size: 0,
        });

        Arc::new(Self {
            config,
            manager,
            slots,
        })
    }

    /// Retrieves an [`Object`] from this [`Pool`].
    ///
    /// This method should be called with a pool wrapped in an [`Arc`].
    pub async fn get(self: &Arc<Self>) -> Result<Object<T, M>, M::Error> {
        let object = loop {
            let existing = match self.config.queue_strategy {
                QueueStrategy::Fifo => self.slots.lock().deque.pop_front(),
                QueueStrategy::Lifo => self.slots.lock().deque.pop_back(),
            };

            match existing {
                None => {
                    let object = self.manager.create().await?;
                    let state = ObjectState {
                        o: object,
                        status: ObjectStatus::default(),
                    };
                    self.slots.lock().current_size += 1;
                    break Object {
                        state: Some(state),
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
                        state.status.recycle_count += 1;
                        state.status.recycled = Some(std::time::Instant::now());
                        break unready_object.ready();
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

    /// Extends the pool with exactly one object.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::pool::unbounded::Pool;
    /// use asyncband::pool::unbounded::PoolConfig;
    ///
    /// let config = PoolConfig::default();
    /// let pool = Pool::never_manage(config);
    ///
    /// pool.extend_one(Vec::<i64>::with_capacity(1024));
    /// ```
    pub fn extend_one(&self, o: T) {
        self.extend(Some(o));
    }

    /// Extends the pool with the objects of an iterator.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::pool::unbounded::Pool;
    /// use asyncband::pool::unbounded::PoolConfig;
    ///
    /// let config = PoolConfig::default();
    /// let pool = Pool::never_manage(config);
    ///
    /// pool.extend([
    ///     Vec::<i64>::with_capacity(1024),
    ///     Vec::<i64>::with_capacity(512),
    ///     Vec::<i64>::with_capacity(256),
    /// ]);
    /// ```
    pub fn extend(&self, iter: impl IntoIterator<Item = T>) {
        let mut slots = self.slots.lock();
        for o in iter {
            slots.current_size += 1;
            slots.deque.push_back(ObjectState {
                o,
                status: ObjectStatus::default(),
            });
        }
    }

    /// Retains only the objects that pass the given predicate.
    ///
    /// This function blocks the entire pool. Therefore, the given function should not block.
    ///
    /// The following example starts a background task that runs every 30 seconds and removes
    /// objects from the pool that have not been used for more than one minute. The task will
    /// terminate if the pool is dropped.
    ///
    /// ```rust,ignore
    /// let interval = Duration::from_secs(30);
    /// let max_age = Duration::from_secs(60);
    ///
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
        let mut slots = self.slots.lock();
        let result = retain_spec::do_vec_deque_retain(&mut slots.deque, f);
        slots.current_size -= result.removed.len();
        result
    }

    /// Returns the current status of the pool.
    ///
    /// The status returned by the pool is not guaranteed to be consistent.
    ///
    /// Although this status provides [eventual consistency], the numbers can be
    /// temporarily inaccurate under heavy load. They are intended as an overall insight.
    ///
    /// [eventual consistency]: (https://en.wikipedia.org/wiki/Eventual_consistency)
    pub fn status(&self) -> PoolStatus {
        let slots = self.slots.lock();
        let (current_size, idle_count) = (slots.current_size, slots.deque.len());
        drop(slots);

        PoolStatus {
            current_size,
            idle_count,
        }
    }

    fn push_back(&self, o: ObjectState<T>) {
        let mut slots = self.slots.lock();
        slots.deque.push_back(o);
        drop(slots);
    }

    fn detach_object(&self, o: &mut T) {
        let mut slots = self.slots.lock();
        slots.current_size -= 1;
        drop(slots);
        self.manager.on_detached(o);
    }
}

/// A wrapper of the actual pooled object.
///
/// This object implements [`Deref`] and [`DerefMut`]. You can use it as if it was of type `T`.
///
/// This object implements [`Drop`] that returns the underlying object to the pool on drop. You may
/// call [`Object::detach`] to detach the object from the pool before dropping it.
pub struct Object<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    state: Option<ObjectState<T>>,
    pool: Weak<Pool<T, M>>,
}

impl<T, M> std::fmt::Debug for Object<T, M>
where
    T: std::fmt::Debug,
    M: ManageObject<Object = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("state", &self.state)
            .finish()
    }
}

impl<T, M: ManageObject<Object = T>> Drop for Object<T, M> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.push_back(state);
            }
        }
    }
}

impl<T, M: ManageObject<Object = T>> Deref for Object<T, M> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &self.state.as_ref().unwrap().o
    }
}

impl<T, M: ManageObject<Object = T>> DerefMut for Object<T, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &mut self.state.as_mut().unwrap().o
    }
}

impl<T, M: ManageObject<Object = T>> AsRef<M::Object> for Object<T, M> {
    fn as_ref(&self) -> &M::Object {
        self
    }
}

impl<T, M: ManageObject<Object = T>> AsMut<M::Object> for Object<T, M> {
    fn as_mut(&mut self) -> &mut M::Object {
        self
    }
}

impl<T, M: ManageObject<Object = T>> Object<T, M> {
    /// Detaches the object from the [`Pool`].
    ///
    /// This reduces the size of the pool by one.
    pub fn detach(mut self) -> M::Object {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        let mut o = self.state.take().unwrap().o;
        if let Some(pool) = self.pool.upgrade() {
            pool.detach_object(&mut o);
        }
        o
    }

    /// Returns the status of the object.
    pub fn status(&self) -> ObjectStatus {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        self.state.as_ref().unwrap().status
    }
}

/// A wrapper of ObjectState used during the `is_recyclable` check in `Pool::get`.
///
/// If the check passes, the object is converted to a ready `Object` via `ready()`.
/// If the check fails, `detach()` should be called to permanently remove the object
/// from the pool. If dropped without calling either method (due to being cancelled),
/// the behavior depends on the pool's [`RecycleCancelledStrategy`] configuration.
struct UnreadyObject<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    state: Option<ObjectState<T>>,
    pool: Weak<Pool<T, M>>,
    recycle_cancelled_strategy: RecycleCancelledStrategy,
}

impl<T, M: ManageObject<Object = T>> Drop for UnreadyObject<T, M> {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                match self.recycle_cancelled_strategy {
                    RecycleCancelledStrategy::Detach => {
                        pool.detach_object(&mut state.o);
                    }
                    RecycleCancelledStrategy::ReturnToPool => {
                        pool.push_back(state);
                    }
                }
            }
        }
    }
}

impl<T, M: ManageObject<Object = T>> UnreadyObject<T, M> {
    fn ready(mut self) -> Object<T, M> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        let state = Some(self.state.take().unwrap());
        let pool = self.pool.clone();
        Object { state, pool }
    }

    fn detach(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.detach_object(&mut state.o);
            }
        }
    }

    fn state(&mut self) -> &mut ObjectState<T> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        self.state.as_mut().unwrap()
    }
}

#[derive(Debug)]
struct ObjectState<T> {
    o: T,
    status: ObjectStatus,
}

impl<T> retain_spec::SealedState for ObjectState<T> {
    type Object = T;

    fn status(&self) -> ObjectStatus {
        self.status
    }

    fn mut_object(&mut self) -> &mut Self::Object {
        &mut self.o
    }

    fn take_object(self) -> Self::Object {
        self.o
    }
}

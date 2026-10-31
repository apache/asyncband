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

//! A service observes connection churn even when acquisition succeeds, and resets session state
//! before return.
//!
//! `WithRecycleDiagnostics` adapts an existing manager without changing its error type or retry
//! policy. Failed pings and intentional retirement have separate counters; the same adapter works
//! with either managed pool variant. Its callback runs on the acquiring task, so it should be quick
//! and nonblocking. Cancelled checks have no result to report. The connections simulate I/O
//! outcomes.
//!
//! `ReturnAfterReset` detaches by default so a failed or cancelled reset cannot return an unclean
//! connection. Install it before borrower work begins; only successful cleanup permits ordinary
//! return. This guard is application policy, independent of the pool's checkout cancellation
//! strategy. A successful reset cannot prevent a later disconnect while idle, so this service also
//! checks at checkout. Workloads that only need local-state checks can implement those in
//! `is_recyclable`.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Waker;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded::Object;
use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;

struct WithRecycleDiagnostics<M, F> {
    manager: M,
    report: F,
}

impl<M, F> ManageObject for WithRecycleDiagnostics<M, F>
where
    M: ManageObject,
    F: Fn(&M::Error, &ObjectStatus) + Send + Sync,
{
    type Object = M::Object;
    type Error = M::Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        self.manager.create().await
    }

    async fn is_recyclable(
        &self,
        object: &mut Self::Object,
        status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        let result = self.manager.is_recyclable(object, status).await;
        if let Err(error) = &result {
            (self.report)(error, status);
        }
        result
    }

    fn on_detached(&self, object: &mut Self::Object) {
        self.manager.on_detached(object);
    }
}

#[derive(Debug)]
enum RecycleError {
    PingFailed,
    Retired,
}

#[derive(Debug)]
struct ResetFailed;

struct Connection {
    connected: Arc<AtomicBool>,
    dirty: bool,
}

impl Connection {
    async fn ping(&self) -> Result<(), RecycleError> {
        tokio::task::yield_now().await;
        if self.connected.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(RecycleError::PingFailed)
        }
    }

    async fn reset(&mut self) -> Result<(), ResetFailed> {
        tokio::task::yield_now().await;
        if !self.connected.load(Ordering::Relaxed) {
            return Err(ResetFailed);
        }
        self.dirty = false;
        Ok(())
    }
}

struct Manager {
    detached: Arc<AtomicUsize>,
}

impl ManageObject for Manager {
    type Object = Connection;
    type Error = RecycleError;

    async fn create(&self) -> Result<Connection, RecycleError> {
        Ok(Connection {
            connected: Arc::new(AtomicBool::new(true)),
            dirty: false,
        })
    }

    async fn is_recyclable(
        &self,
        connection: &mut Connection,
        status: &ObjectStatus,
    ) -> Result<(), RecycleError> {
        // A low usage limit makes intentional retirement easy to observe in this example.
        if status.recycle_count() >= 1 {
            return Err(RecycleError::Retired);
        }
        connection.ping().await
    }

    fn on_detached(&self, _: &mut Connection) {
        self.detached.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReturnAfterReset<M: ManageObject>(Option<Object<M>>);

impl<M: ManageObject<Object = Connection>> ReturnAfterReset<M> {
    fn connection(&mut self) -> &mut Connection {
        self.0.as_mut().unwrap()
    }

    async fn reset_and_return(mut self) -> Result<(), ResetFailed> {
        self.connection().reset().await?;
        // Taking the wrapper disarms the guard only after the awaited reset succeeds.
        drop(self.0.take());
        Ok(())
    }
}

impl<M: ManageObject> Drop for ReturnAfterReset<M> {
    fn drop(&mut self) {
        if let Some(object) = self.0.take() {
            drop(object.detach());
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let failed_pings = AtomicUsize::new(0);
    let retirements = AtomicUsize::new(0);
    let detached = Arc::new(AtomicUsize::new(0));
    let manager = WithRecycleDiagnostics {
        manager: Manager {
            detached: detached.clone(),
        },
        report: |error: &RecycleError, _status: &ObjectStatus| match error {
            RecycleError::PingFailed => {
                failed_pings.fetch_add(1, Ordering::Relaxed);
            }
            RecycleError::Retired => {
                retirements.fetch_add(1, Ordering::Relaxed);
            }
        },
    };
    let pool = Pool::new(PoolConfig::new(1), manager);

    let first = pool.get().await.unwrap();
    let remote_connection = first.connected.clone();
    drop(first);
    // The remote end disconnects while the connection is idle.
    remote_connection.store(false, Ordering::Relaxed);
    let replacement = pool.get().await.unwrap();
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    assert_eq!(detached.load(Ordering::Relaxed), 1);
    assert!(!Arc::ptr_eq(&remote_connection, &replacement.connected));
    println!("Acquisition succeeded after reporting one failed ping");

    drop(replacement);
    let reused = pool.get().await.unwrap();
    assert_eq!(reused.status().recycle_count(), 1);
    drop(reused);
    let clean = pool.get().await.unwrap();
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    println!("Usage-limit retirement counted separately from failed pings");

    let mut session = ReturnAfterReset(Some(clean));
    session.connection().dirty = true;
    let connection = session.connection().connected.clone();
    session.reset_and_return().await.unwrap();
    let reused = pool.get().await.unwrap();
    assert!(!reused.dirty);
    assert!(Arc::ptr_eq(&connection, &reused.connected));

    let mut session = ReturnAfterReset(Some(reused));
    session.connection().dirty = true;
    session
        .connection()
        .connected
        .store(false, Ordering::Relaxed);
    let result = session.reset_and_return().await;
    assert!(matches!(result, Err(ResetFailed)));
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(detached.load(Ordering::Relaxed), 3);

    {
        let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
        session.connection().dirty = true;
        let mut reset = pin!(session.reset_and_return());
        // Poll through yield_now to simulate cancellation during asynchronous cleanup.
        assert!(
            reset
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(detached.load(Ordering::Relaxed), 4);
    println!("Failed and cancelled resets both detached their connections");

    let broken = pool.get().await.unwrap();
    broken.connected.store(false, Ordering::Relaxed);
    // A borrower that has already observed a failure can skip reset and another ping.
    drop(broken.detach());
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    assert_eq!(detached.load(Ordering::Relaxed), 5);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_and_cleanup() {
        main();
    }

    #[tokio::test]
    async fn cancellation_before_reset_also_detaches() {
        let detached = Arc::new(AtomicUsize::new(0));
        let pool = Pool::new(
            PoolConfig::new(1),
            Manager {
                detached: detached.clone(),
            },
        );

        // Cancellation can occur in borrower work, before reset is even requested.
        {
            let mut borrower = pin!(async {
                let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
                session.connection().dirty = true;
                std::future::pending::<()>().await;
                session.reset_and_return().await.unwrap();
            });
            assert!(
                borrower
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(detached.load(Ordering::Relaxed), 1);

        let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
        session.connection().dirty = true;
        // The future owns the guard even if it is dropped without being polled.
        drop(session.reset_and_return());
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(detached.load(Ordering::Relaxed), 2);
    }
}

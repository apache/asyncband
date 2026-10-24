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

//! Simulate a server closing an idle connection. The manager logs the failed check, and the pool
//! supplies a new connection.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;

struct Connection {
    connected: Arc<AtomicBool>,
}

struct Manager;

impl ManageObject for Manager {
    type Object = Connection;
    type Error = io::Error;

    async fn create(&self) -> io::Result<Connection> {
        Ok(Connection {
            connected: Arc::new(AtomicBool::new(true)),
        })
    }

    async fn is_recyclable(
        &self,
        connection: &mut Connection,
        _status: &ObjectStatus,
    ) -> io::Result<()> {
        if connection.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let error = io::Error::new(io::ErrorKind::ConnectionReset, "server closed connection");
        eprintln!("Connection check failed: {error}");
        Err(error)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let pool = Pool::new(PoolConfig::new(1), Manager);
    let connection = pool.get().await.unwrap();
    let server_connected = connection.connected.clone();
    drop(connection);

    // The connection closes while it is idle in the pool.
    server_connected.store(false, Ordering::Relaxed);

    let replacement = pool.get().await.unwrap();
    assert!(replacement.connected.load(Ordering::Relaxed));
    assert!(!Arc::ptr_eq(&server_connected, &replacement.connected));
    println!("get() succeeded with a new connection");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnected_idle_connection_is_replaced() {
        main();
    }
}

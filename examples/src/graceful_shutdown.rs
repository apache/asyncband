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

//! Stop a service on Ctrl+C or an administrative request, wait for worker cleanup, and abort the
//! worker if the shutdown deadline expires.

use std::error::Error;
use std::time::Duration;

use asyncband::shutdown;
use asyncband::shutdown::ShutdownGuard;
use tokio::sync::oneshot;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let (shutdown, guard) = shutdown::new();
    let worker = tokio::spawn(run_worker(guard));

    // A real service can give this sender to an administrative endpoint.
    let (_admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel::<()>();
    let reason = tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            "Ctrl+C"
        }
        result = admin_shutdown_rx => {
            result?;
            "an administrative request"
        }
    };
    println!("received {reason}; starting graceful shutdown");

    // Request explicitly before the cancellable timeout. Even if the timeout branch wins before
    // `shutdown` is polled, workers are guaranteed to observe the request.
    shutdown.request_shutdown();
    let completed = tokio::select! {
        _ = shutdown => true,
        _ = tokio::time::sleep(Duration::from_secs(30)) => false,
    };

    if !completed {
        eprintln!("graceful shutdown timed out; aborting the worker");
        worker.abort();
    }

    match worker.await {
        Ok(()) => {}
        Err(error) if !completed && error.is_cancelled() => {}
        Err(error) => return Err(error.into()),
    }

    Ok(())
}

async fn run_worker(guard: ShutdownGuard) {
    loop {
        tokio::select! {
            _ = guard.shutdown_requested() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                println!("worker completed another unit of work");
            }
        }
    }

    println!("worker cleanup complete");
}

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

//! Receive messages while retaining a stop notification and one deadline across selections.
//!
//! Asyncband supplies the selection and channels. The application supplies Tokio's timer and task
//! execution. Once the message channel closes, its branch is disabled so its ready error cannot
//! turn the loop into a busy loop.

use std::future::IntoFuture;
use std::pin::pin;
use std::time::Duration;

use asyncband::mpsc;
use asyncband::oneshot;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (sender, mut receiver) = mpsc::bounded(1);
    let (stop_sender, stop_receiver) = oneshot::channel();
    let producer = tokio::spawn(async move {
        for index in 1..=3 {
            if sender.send(format!("message {index}")).await.is_err() {
                return;
            }
        }
        drop(sender);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = stop_sender.send(());
    });

    // Recreating these operations inside the loop would discard the oneshot receiver or restart
    // the timeout each time a message wins. Passing pinned borrows preserves both operations.
    let mut stop = pin!(stop_receiver.into_future());
    let mut deadline = pin!(tokio::time::sleep(Duration::from_secs(1)));
    let mut messages_open = true;
    loop {
        asyncband::select! {
            biased;
            result = stop.as_mut() => {
                println!("producer finished: {result:?}");
                break;
            },
            _ = deadline.as_mut() => {
                println!("deadline reached");
                break;
            },
            result = receiver.recv(), if messages_open => match result {
                Ok(message) => println!("received {message}"),
                Err(_) => {
                    messages_open = false;
                    println!("message channel drained; waiting for stop");
                }
            },
        }
    }
    drop(receiver);
    producer.await.unwrap();
}

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

use std::thread;
use std::time::Duration;

use asyncband::blocking::FutureExt as _;
use asyncband::mutex::Mutex;
use asyncband::oneshot;

#[test]
fn blocking_bridge_composes_with_public_primitives() {
    let mutex = Mutex::new(1);
    *mutex.lock().block_on() += 1;
    assert_eq!(*mutex.lock().block_on(), 2);

    let (sender, receiver) = oneshot::channel();
    let producer = thread::spawn(move || sender.send(7).unwrap());

    assert_eq!(receiver.block_on(), Ok(7));
    producer.join().unwrap();
}

#[test]
fn timed_out_wait_cancels_an_asyncband_future() {
    let (sender, receiver) = oneshot::channel();

    assert_eq!(receiver.wait_timeout(Duration::ZERO), None);
    assert_eq!(sender.send(7).unwrap_err().into_inner(), 7);
}

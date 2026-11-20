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

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Eq)]
pub struct ManagerError;

type Step = oneshot::Receiver<Result<(), ManagerError>>;

#[derive(Default)]
struct State {
    created: AtomicUsize,
    creating: Mutex<VecDeque<Step>>,
    recycling: Mutex<VecDeque<Step>>,
    detached: Mutex<Vec<usize>>,
}

#[derive(Clone, Default)]
pub struct Manager(Arc<State>);

impl Manager {
    pub fn created(&self) -> usize {
        self.0.created.load(Ordering::Relaxed)
    }

    pub fn detached(&self) -> Vec<usize> {
        self.0.detached.lock().unwrap().clone()
    }

    pub fn pause_create(&self) -> oneshot::Sender<Result<(), ManagerError>> {
        let (sender, receiver) = oneshot::channel();
        self.0.creating.lock().unwrap().push_back(receiver);
        sender
    }

    pub fn pause_recycle(&self) -> oneshot::Sender<Result<(), ManagerError>> {
        let (sender, receiver) = oneshot::channel();
        self.0.recycling.lock().unwrap().push_back(receiver);
        sender
    }
}

impl ManageObject for Manager {
    type Object = usize;
    type Error = ManagerError;

    async fn create(&self) -> Result<usize, ManagerError> {
        let id = self.0.created.fetch_add(1, Ordering::Relaxed);
        let step = self.0.creating.lock().unwrap().pop_front();
        if let Some(step) = step {
            step.await.expect("creation controller dropped")?;
        }
        Ok(id)
    }

    async fn is_recyclable(&self, _: &mut usize, _: &ObjectStatus) -> Result<(), ManagerError> {
        let step = self.0.recycling.lock().unwrap().pop_front();
        if let Some(step) = step {
            step.await.expect("recycle controller dropped")?;
        }
        Ok(())
    }

    fn on_detached(&self, object: &mut usize) {
        self.0.detached.lock().unwrap().push(*object);
        // Make hook execution visible in the values returned by retain/detach as well.
        *object += 1000;
    }
}

pub fn ready<F: Future>(future: F) -> F::Output {
    expect_ready(poll_once(pin!(future)))
}

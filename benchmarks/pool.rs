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

use std::convert::Infallible;
use std::pin::pin;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded;
use asyncband::pool::unbounded;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const CAPACITIES: &[usize] = &[1, 32, 1024, usize::MAX];

struct Manager;

impl ManageObject for Manager {
    type Object = usize;
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Ok(0)
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[divan::bench(args = CAPACITIES)]
fn construct_bounded(bencher: Bencher, capacity: usize) {
    bencher.bench_local(|| {
        black_box(bounded::Pool::new(
            bounded::PoolConfig::new(black_box(capacity)),
            Manager,
        ))
    });
}

#[divan::bench]
fn bounded_warm_get_and_return(bencher: Bencher) {
    let pool = bounded::Pool::new(bounded::PoolConfig::new(1), Manager);
    let mut context = bench_context();
    drop(poll_ready(pool.get(), &mut context).unwrap());

    bencher.bench_local(|| {
        let object = poll_ready(pool.get(), &mut context).unwrap();
        black_box(*object);
        drop(object);
    });
}

#[divan::bench]
fn unbounded_warm_try_get_and_return(bencher: Bencher) {
    let pool = unbounded::Pool::<usize>::never_manage(unbounded::PoolConfig::default());
    pool.extend_one(0);

    bencher.bench_local(|| {
        let object = pool.try_get().unwrap();
        black_box(*object);
        drop(object);
    });
}

#[divan::bench]
fn bounded_contended_handoff(bencher: Bencher) {
    let pool = bounded::Pool::new(bounded::PoolConfig::new(1), Manager);
    let mut context = bench_context();
    drop(poll_ready(pool.get(), &mut context).unwrap());

    bencher.bench_local(|| {
        let held = poll_ready(pool.get(), &mut context).unwrap();
        let mut waiter = pin!(pool.get());
        poll_pending(waiter.as_mut(), &mut context);

        drop(held);
        let object = poll_pinned_ready(waiter.as_mut(), &mut context).unwrap();
        black_box(*object);
        drop(object);
    });
}

#[divan::bench]
fn cancel_bounded_waiter(bencher: Bencher) {
    let pool = bounded::Pool::new(bounded::PoolConfig::new(1), Manager);
    let mut context = bench_context();
    drop(poll_ready(pool.get(), &mut context).unwrap());

    bencher.bench_local(|| {
        let held = poll_ready(pool.get(), &mut context).unwrap();
        {
            let mut waiter = pin!(pool.get());
            poll_pending(waiter.as_mut(), &mut context);
        }
        drop(held);
    });
}

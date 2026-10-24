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

use std::hint::black_box;

use asyncband::pool::unbounded;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;

fn unbounded_warm_try_get_and_return(bencher: Bencher) {
    let pool = unbounded::Pool::<usize>::never_manage(unbounded::PoolConfig::default());
    pool.extend_one(0);

    bencher.bench_local(|| {
        let object = pool.try_get().unwrap();
        black_box(*object);
        drop(object);
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::unbounded_warm_try_get_and_return"),
        9_000_000,
        1,
        unbounded_warm_try_get_and_return,
    );
}

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

use benchmarks::harness::Runner;

mod broadcast;
mod coalesce;
mod coordination;
mod locks;
mod mpmc;
mod mpsc;
mod oneshot;
mod watch;

fn main() {
    let mut runner = Runner::from_args();
    mpsc::register(&mut runner);
    mpmc::register(&mut runner);
    broadcast::register(&mut runner);
    oneshot::register(&mut runner);
    watch::register(&mut runner);
    locks::register(&mut runner);
    coordination::register(&mut runner);
    coalesce::register(&mut runner);
    runner.finish();
}

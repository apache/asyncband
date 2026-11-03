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
use std::time::Duration;
use std::time::Instant;

use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use benchmarks::support::bench_context;
use benchmarks::support::poll_ready;
use clap::Parser;

#[derive(Parser)]
#[command(about = "Time upgradable-lock acquisition and promotion separately.")]
struct Options {
    #[arg(long, default_value_t = 2_000_000)]
    rounds: usize,
    #[arg(long, default_value_t = 10)]
    repeat: usize,
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

struct Results {
    acquisition: Vec<Duration>,
    promotion: Vec<Duration>,
}

fn measure(rounds: usize, repeat: usize) -> Results {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();
    let mut results = Results {
        acquisition: Vec::with_capacity(repeat),
        promotion: Vec::with_capacity(repeat),
    };

    for _ in 0..repeat {
        let mut acquisition = Duration::ZERO;
        let mut promotion = Duration::ZERO;

        for _ in 0..rounds {
            let start = Instant::now();
            let guard = poll_ready(lock.upgradable_read(), &mut context);
            acquisition += start.elapsed();

            let start = Instant::now();
            let writer = poll_ready(RwLockUpgradableReadGuard::upgrade(guard), &mut context);
            promotion += start.elapsed();

            black_box(*writer);
            drop(writer);
        }

        results.acquisition.push(acquisition);
        results.promotion.push(promotion);
    }

    results
}

fn report(name: &str, rounds: usize, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let middle = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        let lower = samples[middle - 1];
        lower + (samples[middle] - lower) / 2
    } else {
        samples[middle]
    };
    let minimum = samples.first().unwrap();
    let maximum = samples.last().unwrap();
    let ns_per_round = median.as_nanos() as f64 / rounds as f64;

    println!(
        "{name:<24} median {:>8.2} ns/round  ({:.2}..{:.2} ms; runs: {:?})",
        ns_per_round,
        minimum.as_secs_f64() * 1_000.0,
        maximum.as_secs_f64() * 1_000.0,
        samples
            .iter()
            .map(|sample| sample.as_millis())
            .collect::<Vec<_>>(),
    );
}

fn main() {
    let options = Options::parse();
    assert!(options.rounds > 0, "rounds must be positive");
    assert!(options.repeat > 0, "repeat must be positive");

    let results = measure(options.rounds, options.repeat);
    println!(
        "UpgradableRwLock, {} rounds per run, {} runs",
        options.rounds, options.repeat
    );
    println!("Each operation is timed separately; the clock reads add measurable overhead.");
    report("acquisition", options.rounds, results.acquisition);
    report("promotion", options.rounds, results.promotion);
}

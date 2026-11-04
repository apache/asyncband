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
use std::pin::pin;
use std::time::Duration;
use std::time::Instant;

use asyncband::rwlock::RwLockUpgradableReadGuard;
use asyncband::rwlock::UpgradableRwLock;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use clap::Parser;

#[derive(Parser)]
#[command(about = "Time upgradable-lock acquisition and promotion separately.")]
struct Options {
    #[arg(long, default_value_t = 2_000_000)]
    rounds: usize,
    #[arg(long, default_value_t = 10)]
    repeat: usize,
    #[arg(long)]
    test: bool,
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

const READER_COUNTS: &[usize] = &[1, 4, 16];

struct Results {
    acquisition: Vec<Duration>,
    promotion: Vec<Duration>,
    acquisition_with_reader: Vec<Duration>,
    promotion_with_readers: Vec<Vec<Duration>>,
    promotion_with_waiting_writer: Vec<Duration>,
    acquisition_after_waiting_writer: Vec<Duration>,
}

fn measure(rounds: usize, repeat: usize) -> Results {
    let lock = UpgradableRwLock::new(0usize);
    let mut context = bench_context();
    let mut results = Results {
        acquisition: Vec::with_capacity(repeat),
        promotion: Vec::with_capacity(repeat),
        acquisition_with_reader: Vec::with_capacity(repeat),
        promotion_with_readers: READER_COUNTS
            .iter()
            .map(|_| Vec::with_capacity(repeat))
            .collect(),
        promotion_with_waiting_writer: Vec::with_capacity(repeat),
        acquisition_after_waiting_writer: Vec::with_capacity(repeat),
    };

    for _ in 0..repeat {
        let mut acquisition = Duration::ZERO;
        let mut promotion = Duration::ZERO;
        let mut acquisition_with_reader = Duration::ZERO;
        let mut promotion_with_readers = vec![Duration::ZERO; READER_COUNTS.len()];
        let mut promotion_with_waiting_writer = Duration::ZERO;
        let mut acquisition_after_waiting_writer = Duration::ZERO;

        for _ in 0..rounds {
            let start = Instant::now();
            let guard = poll_ready(lock.upgradable_read(), &mut context);
            acquisition += start.elapsed();

            let start = Instant::now();
            let writer = poll_ready(RwLockUpgradableReadGuard::upgrade(guard), &mut context);
            promotion += start.elapsed();

            black_box(*writer);
            drop(writer);

            let reader = poll_ready(lock.read(), &mut context);
            let start = Instant::now();
            let guard = poll_ready(lock.upgradable_read(), &mut context);
            acquisition_with_reader += start.elapsed();
            black_box(*guard);
            drop(guard);
            drop(reader);

            for (index, &reader_count) in READER_COUNTS.iter().enumerate() {
                let guard = poll_ready(lock.upgradable_read(), &mut context);
                let readers = (0..reader_count)
                    .map(|_| poll_ready(lock.read(), &mut context))
                    .collect::<Vec<_>>();
                let mut upgrade = pin!(RwLockUpgradableReadGuard::upgrade(guard));
                let start = Instant::now();
                poll_pending(upgrade.as_mut(), &mut context);
                drop(readers);
                let writer = poll_pinned_ready(upgrade.as_mut(), &mut context);
                promotion_with_readers[index] += start.elapsed();
                black_box(*writer);
                drop(writer);
            }

            let guard = poll_ready(lock.upgradable_read(), &mut context);
            let mut queued_writer = pin!(lock.write());
            poll_pending(queued_writer.as_mut(), &mut context);
            let start = Instant::now();
            let writer = poll_ready(RwLockUpgradableReadGuard::upgrade(guard), &mut context);
            promotion_with_waiting_writer += start.elapsed();
            black_box(*writer);
            drop(writer);
            drop(poll_pinned_ready(queued_writer.as_mut(), &mut context));

            let first_guard = poll_ready(lock.upgradable_read(), &mut context);
            let mut queued_writer = pin!(lock.write());
            poll_pending(queued_writer.as_mut(), &mut context);
            let mut queued_upgradable = pin!(lock.upgradable_read());
            poll_pending(queued_upgradable.as_mut(), &mut context);
            let start = Instant::now();
            drop(first_guard);
            drop(poll_pinned_ready(queued_writer.as_mut(), &mut context));
            let guard = poll_pinned_ready(queued_upgradable.as_mut(), &mut context);
            acquisition_after_waiting_writer += start.elapsed();
            black_box(*guard);
            drop(guard);
        }

        results.acquisition.push(acquisition);
        results.promotion.push(promotion);
        results
            .acquisition_with_reader
            .push(acquisition_with_reader);
        for (samples, elapsed) in results
            .promotion_with_readers
            .iter_mut()
            .zip(promotion_with_readers)
        {
            samples.push(elapsed);
        }
        results
            .promotion_with_waiting_writer
            .push(promotion_with_waiting_writer);
        results
            .acquisition_after_waiting_writer
            .push(acquisition_after_waiting_writer);
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
    if options.test {
        let _ = measure(1, 1);
        println!("ok  upgradable-lock stage benchmark smoke checks");
        return;
    }
    assert!(options.rounds > 0, "rounds must be positive");
    assert!(options.repeat > 0, "repeat must be positive");

    let results = measure(options.rounds, options.repeat);
    println!(
        "UpgradableRwLock, {} rounds per run, {} runs",
        options.rounds, options.repeat
    );
    println!("The clock reads add measurable overhead; treat these numbers as diagnostic.");
    report("acquisition", options.rounds, results.acquisition);
    report("promotion", options.rounds, results.promotion);
    report(
        "acquisition + reader held",
        options.rounds,
        results.acquisition_with_reader,
    );
    for (&reader_count, samples) in READER_COUNTS.iter().zip(results.promotion_with_readers) {
        report(
            &format!("promotion + {reader_count} readers"),
            options.rounds,
            samples,
        );
    }
    report(
        "promotion + writer queued",
        options.rounds,
        results.promotion_with_waiting_writer,
    );
    report(
        "acquire behind queued writer",
        options.rounds,
        results.acquisition_after_waiting_writer,
    );
}

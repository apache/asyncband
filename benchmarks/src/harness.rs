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
use std::num::NonZeroUsize;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Time fixed workloads in milliseconds.")]
struct Options {
    /// Case name substrings; a case must contain all of them.
    filters: Vec<String>,

    /// List matching cases and their work sizes without running them.
    #[arg(long, conflicts_with = "test")]
    list: bool,

    /// Exercise each matching case once per worker, without measuring performance.
    #[arg(long)]
    test: bool,

    /// Run each workload this many times, printing every elapsed time.
    #[arg(long, default_value = "1")]
    repeat: NonZeroUsize,

    /// Multiply the checked-in work sizes by this factor.
    #[arg(long, default_value = "1", conflicts_with = "rounds")]
    scale: NonZeroUsize,

    /// Override the total rounds, for sizing a filtered workload.
    #[arg(long)]
    rounds: Option<NonZeroUsize>,

    // Cargo passes this flag to harness-free benchmark executables.
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

pub struct Runner {
    options: Options,
    matched: usize,
}

impl Runner {
    pub fn from_args() -> Self {
        Self {
            options: Options::parse(),
            matched: 0,
        }
    }

    pub fn case(
        &mut self,
        name: impl AsRef<str>,
        rounds: usize,
        threads: usize,
        workload: impl Fn(Bencher),
    ) {
        let name = name.as_ref();
        if !self
            .options
            .filters
            .iter()
            .all(|filter| name.contains(filter))
        {
            return;
        }
        self.matched += 1;
        assert!(rounds > 0 && threads > 0);
        let rounds = if self.options.test {
            threads
        } else if let Some(rounds) = self.options.rounds {
            rounds.get()
        } else {
            rounds
                .checked_mul(self.options.scale.get())
                .expect("scaled workload is too large")
        };
        assert!(rounds >= threads, "use at least one round per worker");
        if self.options.list {
            println!("{rounds:>10} rounds  {name}");
            return;
        }
        if self.options.test {
            workload(Bencher { rounds, threads });
            println!("ok  {name}");
            return;
        }
        for repetition in 1..=self.options.repeat.get() {
            // Time the whole workload, including fixture setup, worker startup and cleanup.
            // No per-operation clocks, overhead subtraction, or adaptive iteration counts.
            let start = Instant::now();
            workload(Bencher { rounds, threads });
            let elapsed = start.elapsed();
            println!(
                "{:>6} ms  {rounds:>10} rounds  {name}  [{repetition}/{}]",
                elapsed.as_millis(),
                self.options.repeat,
            );
        }
    }

    pub fn finish(self) {
        if self.matched == 0 {
            eprintln!(
                "No matching cases. Use --list and select a target with cargo x bench --bench."
            );
            std::process::exit(1);
        }
    }
}

/// Executes a fixed total number of rounds; the runner times the enclosing workload.
pub struct Bencher {
    rounds: usize,
    threads: usize,
}

impl Bencher {
    pub fn bench_local<O>(self, mut operation: impl FnMut() -> O) {
        assert_eq!(self.threads, 1);
        for _ in 0..self.rounds {
            black_box(operation());
        }
    }

    pub fn bench<O>(self, operation: impl Fn() -> O + Sync) {
        if self.threads == 1 {
            self.bench_local(operation);
            return;
        }
        let start = Barrier::new(self.threads);
        thread::scope(|scope| {
            for worker in 0..self.threads {
                let operation = &operation;
                let start = &start;
                // Keep total work constant across thread counts, including uneven divisions.
                let rounds =
                    self.rounds / self.threads + usize::from(worker < self.rounds % self.threads);
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..rounds {
                        black_box(operation());
                    }
                });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn repeated_workloads_recreate_state_and_finish_cleanup() {
        let options = Options::try_parse_from(["bench", "--repeat", "3", "--rounds", "7"]).unwrap();
        let live = Cell::new(0);
        let completed = Cell::new(0);
        struct Output<'a>(&'a Cell<usize>);
        impl Drop for Output<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get() - 1);
            }
        }
        let mut runner = Runner {
            options,
            matched: 0,
        };
        runner.case("lifecycle", 100, 1, |bencher| {
            assert_eq!(live.get(), 0);
            let mut rounds = 0;
            bencher.bench_local(|| {
                assert_eq!(live.get(), 0);
                live.set(1);
                rounds += 1;
                Output(&live)
            });
            assert_eq!(rounds, 7);
            assert_eq!(live.get(), 0);
            completed.set(completed.get() + 1);
        });
        assert_eq!(completed.get(), 3);
    }

    #[test]
    fn parallel_rounds_share_state_and_preserve_total_work() {
        let workers = Mutex::new(HashMap::new());
        Bencher {
            rounds: 19,
            threads: 4,
        }
        .bench(|| {
            *workers
                .lock()
                .unwrap()
                .entry(thread::current().id())
                .or_insert(0) += 1;
        });
        let mut counts: Vec<_> = workers.into_inner().unwrap().into_values().collect();
        counts.sort_unstable();
        assert_eq!(counts, [4, 5, 5, 5]);
    }
}

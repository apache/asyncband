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
use std::time::Duration;
use std::time::Instant;

use clap::Parser;

/// Register a primitive by its Rust path, including its input and worker count when present.
#[macro_export]
macro_rules! primitive {
    ($runner:expr, $op:ident $(($arg:ident))?, $rounds:expr $(, threads = $threads:ident)?) => {{
        let name = concat!(module_path!(), "::", stringify!($op));
        $(let name = format!("{name}/{}", $arg);)?
        $(let name = format!("{name}/threads={}", $threads);)?
        $runner.case(name, $rounds, $crate::primitive!(@threads $($threads)?), |b| $op(b $(, $arg)?));
    }};
    (@threads) => { 1 };
    (@threads $threads:ident) => { $threads };
}

/// Instantiate the same operation and parameters for each adapter. The first is the reference.
#[macro_export]
macro_rules! implementations {
    ([$($implementation:ident),+ $(,)?], |$adapter:ident, $bench:ident| $work:expr) => {
        &[$((stringify!($implementation), &(|$bench| {
            type $adapter = $implementation;
            $work
        }) as &dyn Fn($crate::harness::Bencher))),+]
    };
}

type Implementation<'a> = (&'a str, &'a dyn Fn(Bencher));

#[derive(Parser)]
#[command(about = "Compare implementations at a fixed amount of work.")]
struct Options {
    /// Select scenario names containing all these substrings.
    filters: Vec<String>,
    /// List matching scenarios, rounds and implementations without running them.
    #[arg(long, conflicts_with = "test")]
    list: bool,
    /// Exercise each workload once per worker without timing.
    #[arg(long)]
    test: bool,
    /// Number of measured repetitions per implementation.
    #[arg(long, default_value = "5")]
    repeat: NonZeroUsize,
    /// Override the scenario's fixed rounds (equally for all implementations).
    #[arg(long)]
    rounds: Option<NonZeroUsize>,
    /// Run only the scenario's Asyncband reference, e.g. to choose its fixed work size.
    #[arg(long)]
    baseline_only: bool,
    /// Include individual elapsed times in their original order.
    #[arg(long)]
    raw: bool,
    // Cargo passes this flag to harness-free executables.
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

    /// A primitive workload has a single implementation and may divide rounds among threads.
    pub fn case(
        &mut self,
        name: impl AsRef<str>,
        rounds: usize,
        threads: usize,
        workload: impl Fn(Bencher),
    ) {
        self.run(name.as_ref(), rounds, threads, &[("Asyncband", &workload)]);
    }

    /// All adapters for one scenario use the same rounds. The first is its Asyncband reference.
    pub fn compare(&mut self, name: &str, rounds: usize, implementations: &[Implementation<'_>]) {
        self.run(name, rounds, 1, implementations);
    }

    fn run(
        &mut self,
        name: &str,
        rounds: usize,
        threads: usize,
        implementations: &[Implementation<'_>],
    ) {
        if !self
            .options
            .filters
            .iter()
            .all(|filter| name.contains(filter))
        {
            return;
        }
        self.matched += 1;
        let rounds = if self.options.test {
            threads
        } else {
            self.options.rounds.map_or(rounds, NonZeroUsize::get)
        };
        assert!(
            threads > 0 && rounds >= threads,
            "{name}: need at least one round per worker"
        );
        assert!(
            !implementations.is_empty(),
            "{name}: missing reference implementation"
        );
        let implementations = if self.options.baseline_only {
            &implementations[..1]
        } else {
            implementations
        };
        if self.options.list {
            let names = implementations
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>();
            println!("{name}  {rounds} rounds  [{}]", names.join(", "));
            return;
        }
        if self.options.test {
            for (label, workload) in implementations {
                workload(Bencher { rounds, threads });
                println!("ok  {name} / {label}");
            }
            return;
        }

        let repeat = self.options.repeat.get();
        let mut samples = vec![Vec::with_capacity(repeat); implementations.len()];
        for repetition in 0..repeat {
            // Interleave peers and rotate the first one so any one adapter does not always run
            // cold.
            for offset in 0..implementations.len() {
                let index = (repetition + offset) % implementations.len();
                let workload = implementations[index].1;
                let start = Instant::now();
                workload(Bencher { rounds, threads });
                samples[index].push(start.elapsed());
            }
        }
        println!(
            "\n{name}  ({rounds} rounds, {repeat} runs; reference: {})",
            implementations[0].0
        );
        println!(
            "  {:<16} {:>10} {:>15} {:>9}",
            "implementation", "median ms", "min..max ms", "vs ref"
        );
        let reference = summary(&samples[0]).1;
        for ((label, _), times) in implementations.iter().zip(&samples) {
            let (min, median, max) = summary(times);
            let range = format!("{}..{}", min.as_millis(), max.as_millis());
            let ratio = if reference.is_zero() {
                String::from("n/a")
            } else {
                format!("{:.2}x", median.as_secs_f64() / reference.as_secs_f64())
            };
            print!(
                "  {label:<16} {:>10} {range:>15} {ratio:>9}",
                median.as_millis()
            );
            if self.options.raw {
                print!(
                    "  {:?}",
                    times.iter().map(Duration::as_millis).collect::<Vec<_>>()
                );
            }
            println!();
        }
    }

    pub fn finish(self) {
        if self.matched == 0 {
            eprintln!("No matching scenarios. Use --list for this target.");
            std::process::exit(1);
        }
    }
}

fn summary(samples: &[Duration]) -> (Duration, Duration, Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let lo = sorted[(sorted.len() - 1) / 2];
    let hi = sorted[sorted.len() / 2];
    (sorted[0], lo + (hi - lo) / 2, sorted[sorted.len() - 1])
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
    use std::cell::RefCell;
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
    fn peers_share_work_and_rotate_across_repetitions() {
        let options = Options::try_parse_from(["bench", "--repeat", "3", "--rounds", "7"]).unwrap();
        let mut runner = Runner {
            options,
            matched: 0,
        };
        let order = RefCell::new(Vec::new());
        let workload = |label, b: Bencher| {
            let mut n = 0;
            b.bench_local(|| n += 1);
            order.borrow_mut().push((label, n));
        };
        runner.compare(
            "roundtrip",
            100,
            &[
                ("ref", &|b| workload('A', b)),
                ("peer", &|b| workload('B', b)),
            ],
        );
        assert_eq!(
            order.into_inner(),
            [('A', 7), ('B', 7), ('B', 7), ('A', 7), ('A', 7), ('B', 7)]
        );
    }

    #[test]
    fn median_keeps_timer_precision_and_range_keeps_the_outlier() {
        let times = [99, 1, 6, 3].map(Duration::from_millis);
        assert_eq!(
            summary(&times),
            (times[1], Duration::from_micros(4500), times[0])
        );
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

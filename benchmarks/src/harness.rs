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

use std::cell::Cell;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::process;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use clap::Parser;

/// Instantiate the same operation and parameters for each adapter. The first is the reference.
#[macro_export]
macro_rules! implementations {
    ([$($implementation:ident),+ $(,)?], |$adapter:ident, $bench:ident| $work:expr) => {
        &[$((stringify!($implementation), &(|$bench: $crate::harness::Bencher<'_>| {
            type $adapter = $implementation;
            $work
        }) as &dyn Fn($crate::harness::Bencher<'_>))),+]
    };
}

/// Named benchmark workload closure used in comparison scenarios.
pub type Implementation<'a> = (&'a str, &'a dyn Fn(Bencher<'_>));

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
    /// Run only comparison scenarios that include peer implementations.
    #[arg(long, conflicts_with = "baseline_only")]
    peers_only: bool,
    /// Include individual elapsed times in their original order.
    #[arg(long)]
    raw: bool,
    // Cargo passes this flag to harness-free executables.
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

/// Fixed-workload benchmark runner with interleaved peer execution and rotation.
pub struct Runner {
    options: Options,
    matched: usize,
}

impl Runner {
    /// Parses benchmark CLI options from the process arguments.
    pub fn from_args() -> Self {
        Self {
            options: Options::parse(),
            matched: 0,
        }
    }

    /// Runs a single-threaded Asyncband workload for `rounds` iterations.
    pub fn local(&mut self, name: &str, rounds: usize, workload: impl Fn(Bencher<'_>)) {
        self.run(name, rounds, 1, &[("Asyncband", &workload)]);
    }

    /// Runs an Asyncband workload dividing `rounds` across `threads` OS threads.
    pub fn parallel(
        &mut self,
        name: &str,
        rounds: usize,
        threads: usize,
        workload: impl Fn(Bencher<'_>),
    ) {
        self.run(name, rounds, threads, &[("Asyncband", &workload)]);
    }

    /// Compares implementations on the same `rounds` budget. The first entry is the reference.
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
        if self.options.peers_only && implementations.len() <= 1 {
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
                let elapsed = Cell::new(None);
                workload(Bencher {
                    rounds,
                    threads,
                    elapsed: &elapsed,
                });
                assert!(
                    elapsed.get().is_some(),
                    "{name} / {label}: workload did not invoke Bencher"
                );
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
                let (label, workload) = implementations[index];
                let elapsed = Cell::new(None);
                workload(Bencher {
                    rounds,
                    threads,
                    elapsed: &elapsed,
                });
                let duration = elapsed
                    .get()
                    .unwrap_or_else(|| panic!("{name} / {label}: workload did not invoke Bencher"));
                samples[index].push(duration);
            }
        }
        let width = implementations
            .iter()
            .map(|(label, _)| label.len())
            .max()
            .unwrap_or(16)
            .max(16);
        println!(
            "\n{name}  ({rounds} rounds, {repeat} runs; reference: {})",
            implementations[0].0
        );
        println!(
            "  {:<width$} {:>10} {:>15} {:>9}",
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
                "  {label:<width$} {:>10} {range:>15} {ratio:>9}",
                median.as_millis()
            );
            if self.options.raw {
                let raw = times
                    .iter()
                    .map(|d| format!("{:.2}", d.as_secs_f64() * 1_000.0))
                    .collect::<Vec<_>>()
                    .join(", ");
                print!("  [{raw}]");
            }
            println!();
        }
    }

    /// Exits with a non-zero status if no scenarios matched the active filters.
    pub fn finish(self) {
        if self.matched == 0 {
            eprintln!("No matching scenarios. Use --list for this target.");
            process::exit(1);
        }
    }
}

/// Returns the minimum, median, and maximum of a nonempty sample.
pub fn summary(samples: &[Duration]) -> (Duration, Duration, Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let lo = sorted[(sorted.len() - 1) / 2];
    let hi = sorted[sorted.len() / 2];
    (sorted[0], lo + (hi - lo) / 2, sorted[sorted.len() - 1])
}

/// Executes a fixed total number of rounds, timing only the measured loop.
pub struct Bencher<'a> {
    rounds: usize,
    threads: usize,
    elapsed: &'a Cell<Option<Duration>>,
}

impl Bencher<'_> {
    /// Times `self.rounds` sequential invocations of `operation` on the current thread.
    pub fn bench_local<O>(self, mut operation: impl FnMut() -> O) {
        assert_eq!(self.threads, 1);
        let start = Instant::now();
        for _ in 0..self.rounds {
            black_box(operation());
        }
        self.elapsed.set(Some(start.elapsed()));
    }

    /// Times `self.rounds` invocations of `operation` divided across `self.threads` workers.
    pub fn bench<O>(self, operation: impl Fn() -> O + Sync) {
        if self.threads == 1 {
            self.bench_local(operation);
            return;
        }
        let start_gate = Barrier::new(self.threads);
        let (earliest_start, latest_end) = thread::scope(|scope| {
            let handles: Vec<_> = (0..self.threads)
                .map(|worker| {
                    let operation = &operation;
                    let start_gate = &start_gate;
                    // Keep total work constant across thread counts, including uneven divisions.
                    let rounds = self.rounds / self.threads
                        + usize::from(worker < self.rounds % self.threads);
                    scope.spawn(move || {
                        start_gate.wait();
                        let worker_start = Instant::now();
                        for _ in 0..rounds {
                            black_box(operation());
                        }
                        (worker_start, Instant::now())
                    })
                })
                .collect();
            let mut earliest_start: Option<Instant> = None;
            let mut latest_end: Option<Instant> = None;
            for handle in handles {
                let (start, end) = handle.join().expect("benchmark worker panicked");
                earliest_start = Some(earliest_start.map_or(start, |prev| prev.min(start)));
                latest_end = Some(latest_end.map_or(end, |prev| prev.max(end)));
            }
            (
                earliest_start.expect("threads > 0"),
                latest_end.expect("threads > 0"),
            )
        });
        self.elapsed
            .set(Some(latest_end.saturating_duration_since(earliest_start)));
    }
}

#[cfg(test)]
mod tests {
    use std::array;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn untimed_setup_and_teardown_are_excluded_from_sample() {
        let elapsed = Cell::new(None);
        let bencher = Bencher {
            rounds: 1,
            threads: 1,
            elapsed: &elapsed,
        };
        thread::sleep(Duration::from_millis(20));
        let after_setup = Instant::now();
        bencher.bench_local(|| 1usize);
        let before_teardown = Instant::now();
        thread::sleep(Duration::from_millis(20));

        let measured = elapsed.get().expect("sample should be recorded");
        let outer_wall_span = before_teardown.saturating_duration_since(after_setup);
        assert!(
            measured <= outer_wall_span,
            "measured loop {measured:?} exceeded untimed bracket {outer_wall_span:?}"
        );
    }

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
        runner.local("lifecycle", 100, |bencher| {
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
        let workload = |label, b: Bencher<'_>| {
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

        struct ImplA;
        struct ImplB;
        trait Tag {
            const TAG: char;
        }
        impl Tag for ImplA {
            const TAG: char = 'A';
        }
        impl Tag for ImplB {
            const TAG: char = 'B';
        }
        let captured = RefCell::new(Vec::new());
        runner.compare(
            "macro-capture",
            100,
            crate::implementations!([ImplA, ImplB], |C, b| {
                b.bench_local(|| ());
                captured.borrow_mut().push(C::TAG);
            }),
        );
        assert_eq!(captured.into_inner(), ['A', 'B', 'B', 'A', 'A', 'B']);
    }

    #[test]
    fn peers_only_and_baseline_only_filtering() {
        assert!(Options::try_parse_from(["bench", "--peers-only", "--baseline-only"]).is_err());

        let peers_options =
            Options::try_parse_from(["bench", "--peers-only", "--repeat", "1", "--rounds", "1"])
                .unwrap();
        let mut peers_runner = Runner {
            options: peers_options,
            matched: 0,
        };
        let ran = RefCell::new(Vec::new());
        peers_runner.local("local-only", 1, |b| {
            ran.borrow_mut().push("local");
            b.bench_local(|| ());
        });
        peers_runner.parallel("parallel-only", 2, 2, |b| {
            ran.borrow_mut().push("parallel");
            b.bench(|| ());
        });
        peers_runner.compare(
            "single-compare",
            1,
            &[("Asyncband", &|b| {
                ran.borrow_mut().push("single");
                b.bench_local(|| ());
            })],
        );
        peers_runner.compare(
            "multi-compare",
            1,
            &[
                ("Asyncband", &|b| {
                    ran.borrow_mut().push("multi-a");
                    b.bench_local(|| ());
                }),
                ("Tokio", &|b| {
                    ran.borrow_mut().push("multi-b");
                    b.bench_local(|| ());
                }),
            ],
        );
        assert_eq!(peers_runner.matched, 1);
        assert_eq!(ran.into_inner(), ["multi-a", "multi-b"]);

        let baseline_options =
            Options::try_parse_from(["bench", "--baseline-only", "--repeat", "1", "--rounds", "1"])
                .unwrap();
        let mut baseline_runner = Runner {
            options: baseline_options,
            matched: 0,
        };
        let baseline_ran = RefCell::new(Vec::new());
        baseline_runner.compare(
            "multi-compare",
            1,
            &[
                ("Asyncband", &|b| {
                    baseline_ran.borrow_mut().push("a");
                    b.bench_local(|| ());
                }),
                ("Tokio", &|b| {
                    baseline_ran.borrow_mut().push("b");
                    b.bench_local(|| ());
                }),
            ],
        );
        assert_eq!(baseline_runner.matched, 1);
        assert_eq!(baseline_ran.into_inner(), ["a"]);
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
        let elapsed = Cell::new(None);
        Bencher {
            rounds: 19,
            threads: 4,
            elapsed: &elapsed,
        }
        .bench(|| {
            *workers
                .lock()
                .unwrap()
                .entry(thread::current().id())
                .or_insert(0) += 1;
        });
        assert!(elapsed.get().is_some());
        let mut counts: Vec<_> = workers.into_inner().unwrap().into_values().collect();
        counts.sort_unstable();
        assert_eq!(counts, [4, 5, 5, 5]);
    }

    #[test]
    fn parallel_timing_excludes_setup_and_records_wall_time() {
        let elapsed = Cell::new(None);
        let slots: [OnceLock<(Instant, Instant)>; 4] = array::from_fn(|_| OnceLock::new());
        let next_id = AtomicUsize::new(0);
        let all_started = Barrier::new(4);

        thread::sleep(Duration::from_millis(20));
        let after_setup = Instant::now();
        Bencher {
            rounds: 4,
            threads: 4,
            elapsed: &elapsed,
        }
        .bench(|| {
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let op_start = Instant::now();
            all_started.wait();
            if id < 2 {
                thread::sleep(Duration::from_millis(15));
            }
            let op_end = Instant::now();
            slots[id].set((op_start, op_end)).unwrap();
        });
        let before_teardown = Instant::now();
        thread::sleep(Duration::from_millis(20));

        let measured = elapsed.get().expect("sample should be recorded");
        let first_op_start = slots.iter().map(|s| s.get().unwrap().0).min().unwrap();
        let last_op_end = slots.iter().map(|s| s.get().unwrap().1).max().unwrap();
        let inner_wall_span = last_op_end.saturating_duration_since(first_op_start);
        let outer_wall_span = before_teardown.saturating_duration_since(after_setup);

        // Allow a small margin for cross-core Instant read skew on virtualized CI runners.
        let skew_margin = Duration::from_millis(1);
        assert!(
            measured.saturating_add(skew_margin) >= inner_wall_span,
            "measured {measured:?} shorter than synchronized inner span {inner_wall_span:?}"
        );
        assert!(
            measured <= outer_wall_span.saturating_add(skew_margin),
            "measured {measured:?} exceeded untimed bracket {outer_wall_span:?}"
        );
    }
}

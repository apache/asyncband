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
use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::future;
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::hint::black_box;
use std::pin::Pin;
use std::pin::pin;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::once::LazyCell;
use asyncband::once::Once;
use asyncband::once::OnceCell;
use asyncband::once::OnceMap;
use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded;
use asyncband::pool::unbounded;
use asyncband::singleflight::Group;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::next_ticket;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use tokio::sync;

struct Asyncband;
struct Tokio;

struct GatedValue<'a> {
    open: &'a Cell<bool>,
    value: usize,
}

impl Future for GatedValue<'_> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.open.get() {
            Poll::Ready(self.value)
        } else {
            Poll::Pending
        }
    }
}

trait OnceCellBench {
    type Cell;

    fn new() -> Self::Cell;
    fn initialized(value: usize) -> Self::Cell;
    fn get_or_init<'a>(
        cell: &'a Self::Cell,
        open: &'a Cell<bool>,
    ) -> impl Future<Output = &'a usize> + 'a;
    fn get_or_init_ready(cell: &Self::Cell) -> impl Future<Output = &usize> + '_;
}

impl OnceCellBench for Asyncband {
    type Cell = OnceCell<usize>;

    fn new() -> Self::Cell {
        OnceCell::new()
    }

    fn initialized(value: usize) -> Self::Cell {
        OnceCell::from_value(value)
    }

    fn get_or_init<'a>(
        cell: &'a Self::Cell,
        open: &'a Cell<bool>,
    ) -> impl Future<Output = &'a usize> + 'a {
        cell.get_or_init(async move || GatedValue { open, value: 1 }.await)
    }

    fn get_or_init_ready(cell: &Self::Cell) -> impl Future<Output = &usize> + '_ {
        cell.get_or_init(async || 1usize)
    }
}

impl OnceCellBench for Tokio {
    type Cell = sync::OnceCell<usize>;

    fn new() -> Self::Cell {
        sync::OnceCell::new()
    }

    fn initialized(value: usize) -> Self::Cell {
        sync::OnceCell::new_with(Some(value))
    }

    fn get_or_init<'a>(
        cell: &'a Self::Cell,
        open: &'a Cell<bool>,
    ) -> impl Future<Output = &'a usize> + 'a {
        cell.get_or_init(move || GatedValue { open, value: 1 })
    }

    fn get_or_init_ready(cell: &Self::Cell) -> impl Future<Output = &usize> + '_ {
        cell.get_or_init(|| async { 1usize })
    }
}

struct UsizeManager;

impl ManageObject for UsizeManager {
    type Object = usize;
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Ok(1)
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub fn register(runner: &mut Runner) {
    runner.local("once/call-once-4-waiters", 2_400_000, |bencher| {
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let once = Once::new();
            let [f0, f1, f2, f3] = [once.wait(), once.wait(), once.wait(), once.wait()];
            let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut cx);
            }
            poll_ready(once.call_once(async || {}), &mut cx);
            for waiter in &mut waiters {
                poll_pinned_ready(waiter.as_mut(), &mut cx);
            }
        });
    });

    runner.compare(
        "once-cell/initialized-get",
        105_000_000,
        implementations!([Asyncband, Tokio], |C, bencher| {
            let cell = C::initialized(1usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                black_box(*poll_ready(C::get_or_init_ready(&cell), &mut cx));
            });
        }),
    );

    runner.compare(
        "once-cell/coalesce-4-waiters",
        2_600_000,
        implementations!([Asyncband, Tokio], |C, bencher| {
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let cell = C::new();
                let open = Cell::new(false);
                let [f0, f1, f2, f3] = [
                    C::get_or_init(&cell, &open),
                    C::get_or_init(&cell, &open),
                    C::get_or_init(&cell, &open),
                    C::get_or_init(&cell, &open),
                ];
                let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
                for waiter in &mut waiters {
                    poll_pending(waiter.as_mut(), &mut cx);
                }
                open.set(true);
                for waiter in &mut waiters {
                    black_box(*poll_pinned_ready(waiter.as_mut(), &mut cx));
                }
            });
        }),
    );

    runner.local("lazy-cell/forced-get", 100_000_000, |bencher| {
        let cell = LazyCell::from_value(1usize);
        let mut cx = bench_context();
        bencher.bench_local(|| {
            black_box(*poll_ready(LazyCell::force(&cell), &mut cx));
        });
    });

    runner.local("once-map/get-hit", 30_000_000, |bencher| {
        let map: OnceMap<usize, usize, BuildHasherDefault<DefaultHasher>> =
            OnceMap::with_hasher(BuildHasherDefault::default());
        let mut cx = bench_context();
        for key in 0..64usize {
            poll_ready(map.compute(key, async move || key), &mut cx);
        }
        bencher.bench_local(|| {
            let key = next_ticket() % 64;
            black_box(map.get(&key).expect("pre-populated key should exist"));
        });
    });

    runner.parallel("once-map/get-hit-4t", 5_500_000, 4, |bencher| {
        let map: OnceMap<usize, usize, BuildHasherDefault<DefaultHasher>> =
            OnceMap::with_hasher(BuildHasherDefault::default());
        let mut cx = bench_context();
        for key in 0..64usize {
            poll_ready(map.compute(key, async move || key), &mut cx);
        }
        bencher.bench(|| {
            let key = next_ticket() % 64;
            black_box(map.get(&key).expect("pre-populated key should exist"));
        });
    });

    runner.local("once-map/compute-coalesce-4", 1_600_000, |bencher| {
        let map: OnceMap<usize, usize, BuildHasherDefault<DefaultHasher>> =
            OnceMap::with_hasher(BuildHasherDefault::default());
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let open = Cell::new(false);
            let [f0, f1, f2, f3] = std::array::from_fn(|_| {
                map.compute(0, async || {
                    GatedValue {
                        open: &open,
                        value: 1,
                    }
                    .await
                })
            });
            let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut cx);
            }
            open.set(true);
            for waiter in &mut waiters {
                black_box(poll_pinned_ready(waiter.as_mut(), &mut cx));
            }
            assert_eq!(map.remove(&0), Some(1));
        });
    });

    runner.local("singleflight/coalesce-4", 1_600_000, |bencher| {
        let group: Group<usize, usize, BuildHasherDefault<DefaultHasher>> =
            Group::with_hasher(BuildHasherDefault::default());
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let open = Cell::new(false);
            let [f0, f1, f2, f3] = std::array::from_fn(|_| {
                group.work(0, async || {
                    GatedValue {
                        open: &open,
                        value: 1,
                    }
                    .await
                })
            });
            let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut cx);
            }
            open.set(true);
            for waiter in &mut waiters {
                black_box(poll_pinned_ready(waiter.as_mut(), &mut cx));
            }
        });
    });

    runner.local("pool/bounded-warm", 7_200_000, |bencher| {
        let pool = bounded::Pool::new(bounded::PoolConfig::new(8), UsizeManager);
        let mut cx = bench_context();
        poll_ready(pool.replenish_to(1), &mut cx).expect("infallible manager");
        bencher.bench_local(|| {
            let obj = poll_ready(pool.get(), &mut cx).expect("infallible manager");
            black_box(*obj);
        });
    });

    runner.local("pool/bounded-handoff", 2_600_000, |bencher| {
        let pool = bounded::Pool::new(bounded::PoolConfig::new(1), UsizeManager);
        let mut cx = bench_context();
        poll_ready(pool.replenish_to(1), &mut cx).expect("infallible manager");
        bencher.bench_local(|| {
            let held = poll_ready(pool.get(), &mut cx).expect("infallible manager");
            let mut waiter = pin!(pool.get());
            poll_pending(waiter.as_mut(), &mut cx);
            drop(held);
            let obj = poll_pinned_ready(waiter.as_mut(), &mut cx).expect("infallible manager");
            black_box(*obj);
        });
    });

    runner.local("pool/unbounded-warm", 10_000_000, |bencher| {
        let pool = unbounded::Pool::never_manage(unbounded::PoolConfig::new());
        pool.extend_one(0usize);
        bencher.bench_local(|| {
            let obj = pool.try_get().expect("warm object should be idle");
            black_box(*obj);
        });
    });

    runner.local("blocking/block-on-ready", 200_000_000, |bencher| {
        bencher.bench_local(|| {
            black_box(async { black_box(42usize) }.block_on());
        });
    });

    runner.local("blocking/block-on-wake", 68_000, |bencher| {
        let (tx, rx) = mpsc::channel::<Waker>();
        let waker_thread = thread::spawn(move || {
            while let Ok(waker) = rx.recv() {
                waker.wake();
            }
        });
        bencher.bench_local(|| {
            let mut yielded = false;
            let tx = &tx;
            future::poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    tx.send(cx.waker().clone())
                        .expect("waker helper thread should be alive");
                    Poll::Pending
                }
            })
            .block_on();
        });
        drop(tx);
        waker_thread.join().expect("waker helper thread panicked");
    });
}

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

use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Poll;
use std::time::Duration;

use futures_lite::FutureExt;

/// A normalized handle for a `Send` task.
pub type Task<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A normalized handle for a thread-local task.
pub type LocalTask<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

#[derive(Debug)]
pub enum JoinError {
    Cancelled,
    Panicked,
}

/// The executor operations available to every shared test.
pub trait Runtime: 'static {
    const NAME: &'static str;

    /// Drives a root future to completion on this runtime.
    fn block_on<F: Future>(future: F) -> F::Output;

    /// Spawns a `Send` task on this runtime's worker facility.
    ///
    /// The scheduler is free to run different tasks on different threads. It
    /// does not guarantee that an individual task will migrate between polls.
    fn spawn<F, T>(future: F) -> Task<Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Spawns a `Send` task without retaining its join handle.
    fn spawn_detached<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static;

    /// Spawns a task which is permanently bound to the current runtime thread.
    fn spawn_local<F, T>(future: F) -> LocalTask<Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
        T: 'static;

    /// Spawns a thread-local task without retaining its join handle.
    fn spawn_local_detached<F>(future: F)
    where
        F: Future<Output = ()> + 'static;

    /// Executes one test case.
    ///
    /// The factory is deliberately reusable. A normal runtime invokes it once;
    /// a Loom adapter can invoke it once per explored schedule.
    fn run_test<Test, Fut>(test: Test)
    where
        Self: Sized,
        Test: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        Self::block_on(test());
    }
}

/// Optional wall-clock capability for real runtimes.
///
/// A model runtime does not have to implement this trait: Loom bounds an
/// exploration by branches, permutations, or duration rather than by racing a
/// future against a wall-clock timer.
pub trait TimeRuntime: Runtime {
    fn sleep(duration: Duration) -> LocalTask<()>;

    fn timeout<'a, F, T>(duration: Duration, future: F) -> TaskRef<'a, Result<T, Elapsed>>
    where
        F: Future<Output = T> + 'a,
        T: 'a;
}

pub type TaskRef<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug)]
pub struct Elapsed;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs one fresh test future with the selected real runtime.
///
/// Taking a factory rather than a future is intentional: a future Loom adapter
/// can invoke the factory once for each explored schedule.
pub fn run<R, Test, Fut>(test: Test)
where
    R: Runtime,
    Test: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + 'static,
{
    R::run_test(test);
}

fn run_with_timeout<R, Test, Fut>(test: Test)
where
    R: Runtime + TimeRuntime,
    Test: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    R::block_on(async {
        if R::timeout(TEST_TIMEOUT, test()).await.is_err() {
            panic!("{} test timed out after {TEST_TIMEOUT:?}", R::NAME);
        }
    });
}

/// Polls a future once using the current task's real waker.
///
/// Returning `None` means the inner future returned `Poll::Pending`. The
/// wrapper itself still completes immediately, so this operation is executor
/// independent and also usable under a model runtime.
pub async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Option<F::Output> {
    std::future::poll_fn(move |context| {
        let output = match future.as_mut().poll(context) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        };
        Poll::Ready(output)
    })
    .await
}

/// Yields exactly one poll turn without depending on a runtime-specific API.
pub async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(move |context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

/// Statically bound runtime operations injected by [`runtime_test`].
///
/// The attribute macro aliases this type to `runtime` inside each generated
/// test, keeping the concrete runtime out of the test function's signature.
pub struct RuntimeOps<R>(std::marker::PhantomData<fn() -> R>);

impl<R: Runtime> RuntimeOps<R> {
    pub const NAME: &'static str = R::NAME;

    pub fn block_on<F: Future>(future: F) -> F::Output {
        R::block_on(future)
    }

    pub fn spawn<F, T>(future: F) -> Task<Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        R::spawn(future)
    }

    pub fn spawn_detached<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        R::spawn_detached(future);
    }

    pub fn spawn_local<F, T>(future: F) -> LocalTask<Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        R::spawn_local(future)
    }

    pub fn spawn_local_detached<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        R::spawn_local_detached(future);
    }

    pub async fn poll_once<F: Future>(future: Pin<&mut F>) -> Option<F::Output> {
        crate::runtime::poll_once(future).await
    }

    pub async fn yield_once() {
        crate::runtime::yield_once().await;
    }
}

impl<R: TimeRuntime> RuntimeOps<R> {
    pub fn sleep(duration: Duration) -> LocalTask<()> {
        R::sleep(duration)
    }

    pub fn timeout<'a, F, T>(duration: Duration, future: F) -> TaskRef<'a, Result<T, Elapsed>>
    where
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        R::timeout(duration, future)
    }
}

pub struct Tokio;

impl Runtime for Tokio {
    const NAME: &'static str = "tokio";

    fn block_on<F: Future>(future: F) -> F::Output {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .expect("failed to build Tokio runtime");
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(future))
    }

    fn spawn<F, T>(future: F) -> Task<Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(AssertUnwindSafe(future).catch_unwind());
        Box::pin(async move {
            match handle.await {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(_)) => Err(JoinError::Panicked),
                Err(_) => Err(JoinError::Cancelled),
            }
        })
    }

    fn spawn_detached<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        drop(tokio::spawn(AssertUnwindSafe(future).catch_unwind()));
    }

    fn spawn_local<F, T>(future: F) -> LocalTask<Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let handle = tokio::task::spawn_local(AssertUnwindSafe(future).catch_unwind());
        Box::pin(async move {
            match handle.await {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(_)) => Err(JoinError::Panicked),
                Err(_) => Err(JoinError::Cancelled),
            }
        })
    }

    fn spawn_local_detached<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        drop(tokio::task::spawn_local(
            AssertUnwindSafe(future).catch_unwind(),
        ));
    }

    fn run_test<Test, Fut>(test: Test)
    where
        Test: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        run_with_timeout::<Self, _, _>(test);
    }
}

impl TimeRuntime for Tokio {
    fn sleep(duration: Duration) -> LocalTask<()> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn timeout<'a, F, T>(duration: Duration, future: F) -> TaskRef<'a, Result<T, Elapsed>>
    where
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        Box::pin(async move {
            tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Elapsed)
        })
    }
}

pub struct Smol;

const SMOL_WORKER_THREADS: usize = 2;

fn smol_executor() -> &'static smol::Executor<'static> {
    static EXECUTOR: OnceLock<Arc<smol::Executor<'static>>> = OnceLock::new();

    EXECUTOR.get_or_init(|| {
        let executor = Arc::new(smol::Executor::new());
        for worker in 0..SMOL_WORKER_THREADS {
            let executor = executor.clone();
            std::thread::Builder::new()
                .name(format!("asyncband-smol-{worker}"))
                .spawn(move || smol::block_on(executor.run(std::future::pending::<()>())))
                .expect("failed to spawn Smol executor worker");
        }
        executor
    })
}

thread_local! {
    static SMOL_LOCAL_EXECUTOR: smol::LocalExecutor<'static> = const { smol::LocalExecutor::new() };
}

impl Runtime for Smol {
    const NAME: &'static str = "smol";

    fn block_on<F: Future>(future: F) -> F::Output {
        SMOL_LOCAL_EXECUTOR.with(|executor| smol::block_on(executor.run(future)))
    }

    fn spawn<F, T>(future: F) -> Task<Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = smol_executor().spawn(AssertUnwindSafe(future).catch_unwind());
        Box::pin(async move { handle.await.map_err(|_| JoinError::Panicked) })
    }

    fn spawn_detached<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        smol_executor()
            .spawn(AssertUnwindSafe(future).catch_unwind())
            .detach();
    }

    fn spawn_local<F, T>(future: F) -> LocalTask<Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let handle = SMOL_LOCAL_EXECUTOR
            .with(|executor| executor.spawn(AssertUnwindSafe(future).catch_unwind()));
        Box::pin(async move { handle.await.map_err(|_| JoinError::Panicked) })
    }

    fn spawn_local_detached<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        SMOL_LOCAL_EXECUTOR.with(|executor| {
            executor
                .spawn(AssertUnwindSafe(future).catch_unwind())
                .detach();
        });
    }

    fn run_test<Test, Fut>(test: Test)
    where
        Test: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        run_with_timeout::<Self, _, _>(test);
    }
}

impl TimeRuntime for Smol {
    fn sleep(duration: Duration) -> LocalTask<()> {
        Box::pin(async move {
            smol::Timer::after(duration).await;
        })
    }

    fn timeout<'a, F, T>(duration: Duration, future: F) -> TaskRef<'a, Result<T, Elapsed>>
    where
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        Box::pin(futures_lite::future::race(
            async move { Ok(future.await) },
            async move {
                smol::Timer::after(duration).await;
                Err(Elapsed)
            },
        ))
    }
}

pub struct Compio;

const COMPIO_WORKER_THREADS: usize = 2;

fn compio_dispatcher() -> &'static compio::dispatcher::Dispatcher {
    static DISPATCHER: OnceLock<compio::dispatcher::Dispatcher> = OnceLock::new();

    DISPATCHER.get_or_init(|| {
        compio::dispatcher::Dispatcher::builder()
            .worker_threads(
                NonZeroUsize::new(COMPIO_WORKER_THREADS)
                    .expect("Compio worker count must be non-zero"),
            )
            .build()
            .expect("failed to build Compio dispatcher")
    })
}

impl Runtime for Compio {
    const NAME: &'static str = "compio";

    fn block_on<F: Future>(future: F) -> F::Output {
        compio::runtime::Runtime::new()
            .expect("failed to build Compio runtime")
            .block_on(future)
    }

    fn spawn<F, T>(future: F) -> Task<Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let receiver = compio_dispatcher()
            .dispatch(move || AssertUnwindSafe(future).catch_unwind())
            .unwrap_or_else(|_| panic!("failed to dispatch Compio task"));
        Box::pin(async move {
            match receiver.await {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(_)) => Err(JoinError::Panicked),
                Err(_) => Err(JoinError::Cancelled),
            }
        })
    }

    fn spawn_detached<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let receiver = compio_dispatcher()
            .dispatch(move || AssertUnwindSafe(future).catch_unwind())
            .unwrap_or_else(|_| panic!("failed to dispatch Compio task"));
        drop(receiver);
    }

    fn spawn_local<F, T>(future: F) -> LocalTask<Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let handle = compio::runtime::spawn(AssertUnwindSafe(future).catch_unwind());
        Box::pin(async move {
            match handle.await {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(_)) | Err(_) => Err(JoinError::Panicked),
            }
        })
    }

    fn spawn_local_detached<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        compio::runtime::spawn(AssertUnwindSafe(future).catch_unwind()).detach();
    }

    fn run_test<Test, Fut>(test: Test)
    where
        Test: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        run_with_timeout::<Self, _, _>(test);
    }
}

impl TimeRuntime for Compio {
    fn sleep(duration: Duration) -> LocalTask<()> {
        Box::pin(compio::runtime::time::sleep(duration))
    }

    fn timeout<'a, F, T>(duration: Duration, future: F) -> TaskRef<'a, Result<T, Elapsed>>
    where
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        Box::pin(async move {
            compio::runtime::time::timeout(duration, future)
                .await
                .map_err(|_| Elapsed)
        })
    }
}

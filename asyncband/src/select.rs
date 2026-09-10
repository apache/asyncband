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

/// Implementation support for the exported macro.
#[doc(hidden)]
pub fn random_start(branches: usize) -> usize {
    fastrand::usize(..branches)
}

/// Waits for one of several asynchronous branches to complete.
///
/// Available with the `select` feature. Each branch has the form
/// `binding = expression, if condition => handler`, with an optional condition. Expressions
/// implement [`IntoFuture`](std::future::IntoFuture), and their outputs may have different types.
/// Every handler must produce the same result type, which becomes the value of the selection.
/// Branches are separated by commas; the last comma is optional. Up to 32 branches are supported.
///
/// Bindings must be irrefutable patterns, such as `value`, `_`, or `(left, right)`. Handle errors,
/// channel disconnection, and other alternatives explicitly inside the handler. Unlike selectors
/// that filter outputs with refutable patterns, this macro never discards a completed result to
/// try another branch.
///
/// ```
/// # async fn example() {
/// let value = asyncband::select! {
///     value = async { 42 } => value,
///     _ = std::future::pending::<()>() => 0,
/// };
/// assert_eq!(value, 42);
/// # }
/// ```
///
/// # Execution and polling order
///
/// The macro awaits its branches on the current task, without spawning tasks or allocating storage
/// for them on the heap. Branches need not be `Send`, `'static`, or `Unpin`. A branch that blocks
/// inside `poll` prevents every other branch from progressing. A ready error is a completed
/// result, just like a ready success.
///
/// Each poll starts at a randomly chosen branch and scans circularly, stopping at the first ready
/// enabled branch. This reduces fixed-order bias across repeated selections; it does not guarantee
/// equal probabilities among ready branches, starvation freedom, or fairness between tasks. Add
/// `biased;` to poll in source order instead. Put higher-priority branches first in that mode.
///
/// ```
/// # async fn example() {
/// let value = asyncband::select! {
///     biased;
///     value = async { 1 } => value,
///     value = async { 2 } => value,
/// };
/// assert_eq!(value, 1);
/// # }
/// ```
///
/// # Conditions and fallback
///
/// All conditions are evaluated once in source order, before constructing any branch. Then every
/// branch expression is evaluated and converted with `IntoFuture::into_future` in source order,
/// including disabled branches. A false condition prevents polling, not construction or ownership
/// transfer. Conditions remain fixed for this selection, even after a wakeup.
///
/// An optional final `else => handler` runs when all conditions are false. It does not run when
/// enabled branches are merely pending. Without an `else`, an all-disabled selection panics. At
/// least one asynchronous branch is required; an empty selection is a compile error.
///
/// # Cancellation and reuse
///
/// Before running the selected handler, the macro drops all branch futures it owns, including the
/// completed one. This ends their borrows and runs their cancellation cleanup. Handlers may await,
/// use `?`, return from the enclosing function, or break and continue enclosing loops.
///
/// Dropping a future does not undo work already performed by its polls. For example, cancelling a
/// pending send may drop its message, cancelling a lock request loses its queue position, and
/// cancelling a barrier wait does not retract its arrival. Dropping a task handle also does not
/// necessarily stop the underlying task. Consult each operation's cancellation contract.
///
/// To preserve an unfinished operation across selections, create and pin it outside the selection,
/// then pass `future.as_mut()`. Only that temporary borrow is dropped when another branch wins;
/// the underlying future remains available to poll again. Disable or remove it after completion:
/// this macro does not fuse futures across separate selections.
///
/// ```
/// # async fn example() {
/// let mut operation = std::pin::pin!(async { String::from("done") });
/// let mut completed = false;
/// loop {
///     asyncband::select! {
///         result = operation.as_mut(), if !completed => {
///             assert_eq!(result, "done");
///             completed = true;
///         },
///         else => break,
///     }
/// }
/// # }
/// ```
///
/// Timers are ordinary caller-provided branches. Keep a deadline future outside a loop when its
/// deadline must survive other branches winning. The macro has no built-in timer or nonblocking
/// `default` branch.
///
/// ```compile_fail
/// # async fn example() {
/// asyncband::select! {
///     Ok(value) = async { Ok::<_, ()>(1) } => value,
/// };
/// # }
/// ```
#[macro_export]
macro_rules! select {
    (@condition) => { true };
    (@condition $condition:expr) => { $condition };
    (@start biased $count:expr) => { 0usize };
    (@start random $count:expr) => { $crate::__select_random_start($count) };

    (@build $mode:ident [
        $(($index:tt $variant:ident ($binding:pat) ($future:expr) ($condition:expr) ($handler:expr)))+
    ] ($fallback:expr)) => {{
        enum __SelectOutput<$($variant),+> {
            $($variant($variant),)+
            Disabled,
        }

        let output = {
            let enabled = [$($condition,)+];
            // Pin each future separately so tuple projection needs no unsafe code. This scope also
            // drops the underlying futures, rather than just their pins, before invoking a handler.
            let mut futures = ($(
                ::core::pin::pin!(::core::future::IntoFuture::into_future($future)),
            )+);

            ::core::future::poll_fn(|cx| {
                if !enabled.iter().any(|enabled| *enabled) {
                    return ::core::task::Poll::Ready(__SelectOutput::Disabled);
                }
                let start = $crate::select!(@start $mode enabled.len());
                for offset in 0..enabled.len() {
                    let index = (start + offset) % enabled.len();
                    if !enabled[index] {
                        continue;
                    }
                    match index {
                        $(
                            $index => {
                                if let ::core::task::Poll::Ready(value) =
                                    ::core::future::Future::poll(futures.$index.as_mut(), cx)
                                {
                                    return ::core::task::Poll::Ready(__SelectOutput::$variant(value));
                                }
                            }
                        )+
                        _ => ::core::unreachable!(),
                    }
                }
                ::core::task::Poll::Pending
            }).await
        };

        match output {
            $(
                __SelectOutput::$variant(value) => {
                    let $binding = value;
                    $handler
                }
            )+
            __SelectOutput::Disabled => $fallback,
        }
    }};

    (@collect $mode:ident [] [$($slots:tt)*]; $(,)?) => {
        ::core::compile_error!("select! requires at least one asynchronous branch")
    };
    (@collect $mode:ident [] [$($slots:tt)*]; else => $fallback:expr $(,)?) => {
        ::core::compile_error!("select! requires at least one asynchronous branch")
    };
    (@collect $mode:ident [$($branches:tt)+] [$($slots:tt)*]; $(,)?) => {
        $crate::select!(@build $mode [$($branches)+]
            (::core::panic!("select! has no enabled branches")))
    };
    (@collect $mode:ident [$($branches:tt)+] [$($slots:tt)*]; else => $fallback:expr $(,)?) => {
        $crate::select!(@build $mode [$($branches)+] ($fallback))
    };
    (@collect $mode:ident [$($branches:tt)*] [($index:tt $variant:ident) $($slots:tt)*];
        $binding:pat = $future:expr $(, if $condition:expr)? => $handler:expr, $($rest:tt)*
    ) => {
        $crate::select!(@collect $mode [
            $($branches)*
            ($index $variant ($binding) ($future)
                ($crate::select!(@condition $($condition)?)) ($handler))
        ] [$($slots)*]; $($rest)*)
    };
    (@collect $mode:ident [$($branches:tt)*] [$($slots:tt)*];
        $binding:pat = $future:expr $(, if $condition:expr)? => $handler:expr
    ) => {
        $crate::select!(@collect $mode [$($branches)*] [$($slots)*];
            $binding = $future $(, if $condition)? => $handler,)
    };
    (@collect $mode:ident [$($branches:tt)*] []; $($rest:tt)+) => {
        ::core::compile_error!("select! supports at most 32 asynchronous branches")
    };
    (@collect $($invalid:tt)*) => {
        ::core::compile_error!("expected `binding = future, if condition => handler,` or a final `else => handler`")
    };
    (@init $mode:ident; $($branches:tt)*) => {
        $crate::select!(@collect $mode [] [
            (0 V0) (1 V1) (2 V2) (3 V3) (4 V4) (5 V5) (6 V6) (7 V7)
            (8 V8) (9 V9) (10 V10) (11 V11) (12 V12) (13 V13) (14 V14) (15 V15)
            (16 V16) (17 V17) (18 V18) (19 V19) (20 V20) (21 V21) (22 V22) (23 V23)
            (24 V24) (25 V25) (26 V26) (27 V27) (28 V28) (29 V29) (30 V30) (31 V31)
        ]; $($branches)*)
    };
    (biased; $($branches:tt)*) => {
        $crate::select!(@init biased; $($branches)*)
    };
    ($($branches:tt)*) => {
        $crate::select!(@init random; $($branches)*)
    };
}

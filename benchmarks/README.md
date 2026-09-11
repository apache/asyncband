# MPMC benchmark workloads

The MPMC benchmarks compare competing queues with the same delivery semantics: asyncband, async-channel, and Flume. Both the asyncband-only and ecosystem suites use the fixtures in `mpmc/` so their execution models stay aligned.

## Execution models

| Benchmark | What it measures |
| --- | --- |
| `tokio_tasks<..., 0>` | Producers and consumers spawned on a Tokio current-thread runtime. |
| `tokio_tasks<..., 4>` | Producers and consumers spawned on a Tokio runtime with four worker threads. |
| `blocking_threads` | Dedicated OS threads using asynchronous channel operations through `asyncband::blocking::FutureExt`; unbounded sends use synchronous publication. |

Use `tokio_tasks` to compare channels used by async tasks. The caller's `Runtime::block_on` only starts the batch and collects task completion; it does not send or receive measured messages. The runtime is reused across samples, while each sample gets a fresh channel and fresh tasks. Consumers compete freely until the last producer drops its sender and the queue drains, with received counts and checksums checked before the batch completes.

The blocking workload represents synchronous callers bridging into asynchronous channel APIs, such as a pipeline built from dedicated worker threads. It includes the bridge's polling and thread-parking costs and should not be presented as Tokio task performance or as an isolated channel-operation cost. All libraries use the same blocking driver. This workload retains its equal per-consumer message quotas; compare libraries within a workload rather than attributing every difference between workloads solely to their executors.

Both workloads send 16,384 `usize` values per batch across 1P/1C, 1P/8C, 8P/1C, and 8P/8C topologies. Bounded queues have capacity 64. Channel and worker creation are outside the measured interval; publication, consumption, and start/completion coordination are included. The Tokio workload also includes draining disconnection and joining the tasks. No application work or artificial per-message yield is inserted: on a current-thread runtime, a producer whose sends are always ready can finish its burst before a consumer runs.

Results describe batch completion time and aggregate message throughput. They do not measure per-message latency, memory retention, arbitrary payload sizes, or a sustained stream reusing one channel. Changing the blocking driver from Pollster to asyncband also changes that composed workload; results from the two driver versions are not a channel-only before/after comparison.

## Running the comparisons

Use `cargo x` for the repository workflow and compile both suites before selecting a workload:

```sh
cargo x bench --no-run
cargo bench --package benchmarks --all-features --bench ecosystem -- '^ecosystem::mpmc::.*tokio_tasks' --sample-count 20
cargo bench --package benchmarks --all-features --bench ecosystem -- '^ecosystem::mpmc::.*blocking_threads' --sample-count 20
```

The asyncband-only target is `--bench benchmarks` with the `^benchmarks::mpmc::` prefix. Record the commit, toolchain, hardware, execution model, runtime worker count, and sampling settings when reporting results.

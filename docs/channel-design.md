# Channel design for 0.7

## Status

This document describes the API and semantic reference implementation in the channel redesign draft. It is intentionally a correctness-first baseline, not a claim that one synchronization strategy wins every workload.

The old 0.6 channel implementations are replaced rather than used as architectural constraints. The new channel families do not depend on the internal atomic pointer slot retained for legacy implementation needs.

## Design dimensions

Producer and consumer counts are only one part of a channel contract. The public family must also make delivery, capacity, retention, overload, and waiting semantics discoverable.

| Dimension | Choices represented in the draft |
| --- | --- |
| Delivery | one value once, competing consumers, multicast log, coalesced latest state |
| Producers | single, multiple |
| Consumers | single, multiple |
| Capacity | rendezvous, bounded, unbounded |
| Full behavior | wait, reject, explicitly replace oldest, explicitly replace newest |
| Multicast retention | overwrite and report lag, gate on slowest receiver, grow and reclaim |
| Waiting | immediate try operation or runtime-agnostic async task parking |

An MPMC queue and an MPMC broadcast are therefore separate families: queue receivers compete for each value, while broadcast receivers each observe every retained value.

## Public shape

All channel APIs live under `asyncband::channel`.

| Family | Constructors or variants | Producers | Consumers | Delivery |
| --- | --- | ---: | ---: | --- |
| oneshot | channel | 1 | 1 | one value once |
| spsc | rendezvous, bounded, unbounded | 1 | 1 | each value once |
| mpsc | rendezvous, bounded, unbounded | many | 1 | each value once |
| spmc | rendezvous, bounded, unbounded | 1 | many | each value once |
| mpmc | rendezvous, bounded, unbounded | many | many | each value once |
| broadcast::overflow | bounded | many | many | every retained value; slow receivers report Lagged |
| broadcast::backpressure | bounded | many | many | every value; slowest receiver gates producers |
| broadcast::unbounded | unbounded | many | many | every value; prefix reclaimed after all receivers advance |
| watch | one retained state | many | many | latest version only |
| disruptor::single_producer | power-of-two bounded ring | 1 | many | every contiguous published sequence |
| disruptor::multi_producer | power-of-two bounded ring | many | many | every contiguous published sequence |

Single-producer and single-consumer endpoint types are non-cloneable and non-Sync. Their operations require mutable access, so a caller cannot accidentally turn an SPSC endpoint into a locally concurrent producer or consumer. Multiple endpoints are cloneable and Sync. Every endpoint remains Send when its value type permits it.

The four competing-consumer queue modules are aliases over one semantic core. The aliases are not merely names: both endpoint cardinalities are part of their nominal type, while the local endpoint cardinality determines Clone and Sync behavior and whether an operation requires mutable or shared access.

## Immediate, waiting, and blocking operations

Every operation has one nonblocking state transition:

| Situation | API |
| --- | --- |
| Send immediately or report Full or Disconnected | send, try_send, or try_publish |
| Receive immediately or report Empty or Disconnected | try_recv |
| Wait for capacity | send().await or publish().await |
| Wait for a value | recv().await or changed().await |

Async methods park only the current task by registering its Waker. Asyncband does not add thread-blocking channel methods. A synchronous caller can park its thread around the same Future with the optional `asyncband::blocking` adapter, following the existing runtime-agnostic policy in the README.

This split also gives integrations a direct base for future Stream and Sink adapters without making either trait part of the core API.

## Queue capacity and overload

Rendezvous has logical capacity zero. An async send owns its value until a receiver accepts it, and cancellation removes an unaccepted handoff. A nonwaiting try_send succeeds only when a receiver is already registered; once accepted, the handoff remains committed even if that particular receive future is subsequently cancelled.

Bounded queues use send().await for backpressure and try_send for rejection. Loss is never an invisible constructor setting. force_send must be called explicitly with FullBehavior::DropOldest or FullBehavior::DropNewest, and SendOutcome::Replaced returns the displaced value to the caller.

Bounded queue and broadcast constructors accept NonZeroUsize because rendezvous is a separate constructor and zero has no valid buffered interpretation. Disruptor constructors accept a validated Capacity that also makes the power-of-two ring requirement unrepresentable after construction.

Unbounded queues accept while receivers exist and reclaim values as consumers pop them. They are logically unbounded, not memory-safe under an indefinitely faster producer.

The strategy set is informed by [.NET System.Threading.Channels full modes](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels.boundedchannelfullmode), which distinguishes Wait, DropNewest, DropOldest, and DropWrite. The Asyncband draft maps Wait to send().await, maps rejection and caller-controlled drop-write to try_send, and makes replacement observable through force_send. Crossbeam's [ArrayQueue](https://docs.rs/crossbeam-queue/latest/crossbeam_queue/struct.ArrayQueue.html) similarly separates normal push failure from an explicit force_push overwrite operation.

[Flume](https://docs.rs/flume/latest/flume/) provides the ecosystem precedent for one MPMC API spanning rendezvous, bounded, and unbounded queues. [Postage](https://docs.rs/postage/latest/postage/) makes the equally important semantic split between a competing-consumer dispatch queue, lossless broadcast, MPSC, oneshot, and watch. The Asyncband taxonomy combines those lessons while retaining explicit endpoint cardinalities.

## Queue state and cancellation

The ordinary queue core uses one short std mutex around a VecDeque, endpoint counts, pending rendezvous sends, and send or receive waiter queues. User Wakers are always invoked after releasing the state lock.

Waiter identities are monotonically generated tokens rather than reusable slab indices. Cancelling a pending future removes only the registration bearing its exact identity, so an old future cannot remove a newer waiter after a wake-and-reuse cycle. Normal completion does not require an extra cleanup lock after its waiter has already been drained.

The mutex baseline has three useful properties for an async runtime-agnostic crate: the state transition is auditable, a poll never spins waiting for another thread, and no endpoint pays an atomic read-modify-write merely to discover that it must park. Lock-free specialization remains possible behind the same topology types if measurements justify it.

Queue value order is FIFO, but waiter scheduling does not promise strict fairness. Capacity and data transitions wake all eligible waiters so cancellation of the first scheduled task cannot strand a permit or buffered value; the executor then determines which competing task wins the state transition.

## Implementation strategies surveyed

The public contract and the storage algorithm are deliberately separate decisions. Existing libraries make different, internally coherent choices:

| Concern | Established choices | Choice in this draft |
| --- | --- | --- |
| Queue type surface | Crossbeam and Flume use one endpoint type for rendezvous, bounded, and unbounded queues; Tokio gives bounded and unbounded MPSC distinct endpoint types | one endpoint type per producer/consumer topology, with capacity selected by constructor |
| Buffered storage | mutex-protected deque; fixed ring with per-slot state; segmented linked blocks | mutex-protected VecDeque reference core |
| Rendezvous | zero-capacity queue with paired waiters or a dedicated handoff state | dedicated pending-send handoff state inside the queue core |
| Producer progress | serialized critical section; single-writer cursor; multi-writer CAS reservation plus publication tracking | serialized state transition for every topology |
| Waiting | thread blocking, task parking, spinning, yielding, sleeping, or phased backoff | task parking through Wakers only |
| Multicast publication | serialize append; or reserve independently and expose only a contiguous published prefix | serialized append for broadcast; explicit reservation and contiguous publication for Disruptor |

[Crossbeam channels](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/) and [Flume](https://docs.rs/flume/latest/flume/) show that one endpoint type can consistently span zero, bounded, and unbounded capacities. [Tokio MPSC](https://docs.rs/tokio/latest/tokio/sync/mpsc/) instead uses distinct bounded and unbounded endpoints; that lets unbounded send be synchronous, and its implementation uses a lock-free linked list of fixed-size blocks. The current Asyncband draft chooses the smaller common capacity surface: `try_send` is always synchronous, while `send` is uniformly async and immediately becomes ready for unbounded queues. A policy-typed capacity axis remains a viable follow-up if the ability to remove impossible methods and make unbounded `send` synchronous is worth the extra public types.

A fixed ring with per-slot sequence generations is a strong candidate for bounded SPSC and MPMC specialization; a segmented list avoids reallocating a monolithic unbounded buffer; a single-producer cursor can eliminate producer-side contention; and a CAS claim cursor can scale multiple producers. None is a free substitution. Each needs its own proof for cancellation, destruction, publication gaps, ABA or generation reuse, wrap-around, and wake registration. The draft therefore first fixes the observable protocol and leaves these as benchmark-driven internal specializations.

The Broadcast policy is nominal in the public API even though the reference implementation shares log and cursor machinery. This prevents accidentally exchanging overflow and lossless endpoints and permits policy-specific send and error APIs. Splitting the three storage backends remains possible without changing callers if profiling or a simpler invariant warrants it.

## Broadcast retention

Broadcast uses one committed tail under the same state lock as its log and waiter metadata. A sender appends the complete value before advancing the tail, so a receiver never mistakes reservation for publication. This directly closes the publication-hole and stale-writer class tracked by the Broadcast correctness issue.

The three retention policies share the sequenced log and receiver-cursor machinery because their state invariants are the same:

| Policy | Full behavior | Slow receiver behavior | Reclamation |
| --- | --- | --- | --- |
| overflow | remove oldest and publish new value | next receive returns exact Lagged count, then resumes at oldest retained value | bounded ring-like prefix |
| backpressure | try_send returns Full; send().await parks | no loss | minimum receiver cursor gates producers |
| unbounded | always accepts while connected | no loss | prefix removed after every receiver advances or drops |

DropNewest is not offered for multicast. Replacing the newest retained sequence can mutate a value after some receivers have already observed that sequence while other receivers have not, violating the single-value-per-sequence contract. Dropping the incoming broadcast would also make send success ambiguous. A caller that wants coalescing should use watch instead.

The retention choice is part of the endpoint's nominal type rather than runtime configuration. Overflow and unbounded `send` calls are synchronous because those policies never wait for capacity. Backpressure alone exposes an async `send`, alongside `try_send`. Only overflow receive errors contain `Lagged`; the two lossless policies reuse the common channel receive errors, so callers do not need to handle an impossible state.

New subscriptions start at the committed tail. Cloning a receiver preserves its cursor. Sending with no receivers returns the original value, rather than retaining values for a hypothetical future subscriber.

These policies implement the three levels proposed in [issue #95](https://github.com/apache/asyncband/issues/95) and match the distinction between lagging broadcast and backpressured broadcast discussed in [issue #88](https://github.com/apache/asyncband/issues/88). Tokio's [broadcast channel](https://docs.rs/tokio/latest/tokio/sync/broadcast/) is the primary reference for bounded lag reporting.

[async-broadcast](https://docs.rs/async-broadcast/latest/async_broadcast/) demonstrates that bounded backpressure and opt-in oldest-value overflow can share one multicast abstraction. Its overflow send reports the removed value, reinforcing the rule that deliberate loss should be observable rather than silently configured.

## Watch

Watch is included because latest-state distribution is not a special case of queue or broadcast retention. Each send publishes a new version and replaces the previous Arc-backed value. changed().await coalesces intermediate versions and returns the latest value. A receiver can borrow without marking the version observed or borrow_and_update to advance explicitly.

The contract follows the broad shape of [Tokio watch](https://docs.rs/tokio/latest/tokio/sync/watch/) while returning Arc values so the channel itself does not require T: Clone.

## Disruptor-style sequencers

The Disruptor modules are bounded multicast logs, not MPMC work queues. Their essential contract follows the [LMAX Disruptor user guide](https://lmax-exchange.github.io/disruptor/user-guide/):

1. A producer reserves a monotonically increasing sequence.
2. It writes the corresponding preallocated ring slot.
3. It marks that sequence available.
4. Consumers see only the highest contiguous published prefix.
5. The minimum subscriber sequence gates wrap-around so an unread slot is never overwritten.

The multi-producer implementation deliberately permits reservations to finish out of order. If sequence N+1 is ready before N, availability records N+1 but the published cursor does not advance. Publishing N then scans the per-slot availability generations and exposes both sequences together. This is the distinction that an atomic reservation cursor alone cannot provide.

The single-producer type enforces the single-writer rule and uses the same semantic core, without cloning or concurrent access to the publisher. The multi-producer type is cloneable and allows concurrent reservations. Both currently serialize reservation metadata with the state mutex; no atomic pointer utility or standard atomic is required for the reference implementation.

This differs from the Java implementation's CAS-oriented [MultiProducerSequencer](https://github.com/LMAX-Exchange/disruptor/blob/c871ca49826a6be7ada6957f6fbafcfecf7b1f87/src/main/java/com/lmax/disruptor/MultiProducerSequencer.java), but preserves its observable claim, availability, contiguous publication, and gating rules. A lock-free backend would be an optimization of this contract, not a different public channel.

Only task-parking waits are built in. Busy spin, yield, phased backoff, blocking conditions, batch translation, mutable preallocated event factories, and consumer dependency graphs are intentionally outside this first API. Busy waiting is a scheduler and deployment choice that a runtime-agnostic async primitive should not perform inside Future::poll.

LMAX exposes [blocking, timeout-blocking, sleeping, yielding, busy-spin, and phased-backoff wait strategies](https://lmax-exchange.github.io/disruptor/javadoc/com.lmax.disruptor/com/lmax/disruptor/WaitStrategy.html). In Asyncband, Waker registration is the task-level analogue of the conservative blocking strategy. Timeouts compose around the returned Future, while spin, yield, sleep, and phased policies belong in a dedicated thread-driven event processor rather than in the channel Future itself. If such an event processor is added later, its wait strategy should be separate from the sequencer so the same ring correctness contract remains usable by ordinary async tasks.

## Disconnection and error vocabulary

Channel modules re-export a common SendError, TrySendError, RecvError, and TryRecvError when their state space matches. Only overflow broadcast adds Lagged to its receive errors. Endpoint predicates use is_disconnected, matching the Disconnected variants and the 0.7 naming decision in [issue #141](https://github.com/apache/asyncband/issues/141).

Buffered channels drain accepted values before returning Disconnected. Sending fails as soon as no receiver remains and returns ownership of the unsent value.

## Correctness invariants

- Accepted queue values are received at most once, and FIFO order is preserved at the shared queue boundary.
- A rendezvous send future does not complete before a receiver accepts its value.
- Every published broadcast sequence identifies one immutable value.
- Broadcast tail never advances past an incomplete value.
- Backpressured multicast producers never wrap or reclaim past the minimum active receiver cursor.
- A Disruptor cursor denotes a contiguous published prefix, not the largest reservation.
- No Waker is invoked while an internal channel lock is held.
- Cancelling a pending send or receive deregisters only that future's waiter.
- Dropping the last opposite endpoint wakes every task that can now observe Disconnected.

## 0.7 crate grouping

The draft introduces three top-level groups while leaving non-channel root modules available during review. The formerly public atomic pointer utilities and admission policy have already been removed on `main` and are not reintroduced here:

| Group | Contents |
| --- | --- |
| `asyncband::sync` | Barrier, Condvar, Latch, Mutex, Once, RwLock, Semaphore, WaitGroup, and guards |
| `asyncband::channel` | all transfer, queue, multicast, state, and sequenced-ring channels |
| `asyncband::coordination` | shutdown and singleflight protocols |

For the breaking release, the remaining synchronization and coordination root-level duplicates can be reviewed separately. This draft removes the superseded root-level channel paths.

A matching additive Cargo feature layout can be introduced after the module names settle:

~~~toml
[features]
default = []
full = ["blocking", "sync", "channel", "coordination"]
sync = ["barrier", "condvar", "latch", "mutex", "once", "rwlock", "semaphore", "waitgroup"]
channel = ["oneshot", "queue", "broadcast", "watch", "disruptor"]
coordination = ["shutdown", "singleflight"]
~~~

Umbrella features only group leaf features; leaf features encode implementation dependencies. CI should cover no-default, each umbrella, default, and all-features rather than the exponential set of every leaf combination. This follows Cargo's [additive feature model](https://doc.rust-lang.org/cargo/reference/features.html).

The feature grouping is applied in this rebased draft because `main` now enables no primitive modules by default and validates every individual feature in CI.

## Benchmark gates for later specialization

The reference implementation should be benchmarked by operation and contention shape before replacing its synchronization mechanism:

- uncontended try send and try receive for each topology;
- producer contention for MPSC and MPMC;
- consumer contention and fairness for SPMC and MPMC;
- cancellation-heavy async waits;
- broadcast fan-out with fast and slow receivers;
- Disruptor single-producer and multi-producer throughput at several subscriber counts;
- tail latency under task oversubscription, where busy-spin designs commonly regress.

An optimized backend must retain the invariants above and demonstrate a material workload benefit. In particular, replacing a mutex with an atomic claim cursor is incomplete unless publication gaps, per-slot generations, wrap gating, cancellation, and Waker registration all remain correct.

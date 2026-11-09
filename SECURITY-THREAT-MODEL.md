<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Security Threat Model

This model describes the security boundary of Apache Asyncband (Incubating), including its synchronization primitives, channels, initialization and work-coalescing utilities, object pools, and blocking adapter. Use it alongside the affected version's API documentation when assessing a report. The [security policy](SECURITY.md) defines the private reporting and disclosure process.

## System and trust boundary

Asyncband runs inside its caller's process. Its async APIs use standard Rust futures and wakers; they do not spawn tasks, own worker threads, or install runtime-specific timers or I/O drivers. The optional blocking adapter polls a future on the calling thread. Applications provide the executor, values, keys, callbacks, and resource-management policy.

The assets Asyncband protects are the memory it manages or exposes, the ownership and synchronization of caller values, and the internal state needed to uphold each primitive's documented contract. Its library boundary includes inputs, concurrent operations, and cancellations reachable through public APIs. A remote user may influence these operations through an embedding application even though Asyncband itself has no network interface.

The host application controls which tasks share a channel, lock, pool, or work-coalescing namespace. Asyncband does not authenticate users, authorize access to application data, or isolate untrusted executable code within the process. Callers must uphold Rust's unsafe preconditions and those of any unsafe APIs they use. Safe caller code cannot be assumed to preserve Asyncband's internal memory-safety invariants on its behalf.

## Library responsibilities

### Memory safety and ownership

Safe use of Asyncband must not cause undefined behavior, including data races, use-after-free, double drops, or invalid references. This includes concurrent polling of different futures, cancellation by dropping a pending future, endpoint or guard destruction, and moving values between threads where the API permits it. `Send` and `Sync` implementations and mapped guards must enforce the required ownership, lifetime, and aliasing constraints.

Safe Rust permits leaking a value with `mem::forget`; leaked guards can prevent progress, but must not make other safe operations memory-unsafe. Similarly, a caller panic or a logic error in a safe trait implementation must not become a memory-safety violation in Asyncband. Recovery and continued progress after a panic depend on the particular API contract.

### Coordination and cancellation

Locks must enforce their documented access rules. Channels must preserve their documented delivery and disconnection semantics. Permit accounting, initialization, and object checkout must remain consistent with each API's contract when operations race or are cancelled. Lost wakeups, incorrect cleanup, or incorrect sharing between independent primitive instances are library bugs and may have security impact in an embedding application.

Cancellation is not application-level rollback. A dropped future does not undo side effects already performed by caller code, and work-coalescing APIs may retry initialization after cancellation or failure as documented. Applications must account for those semantics when performing external actions.

## Application responsibilities and limits

### Resource use and availability

Applications choose capacities, bound request concurrency and payload sizes, and apply deadlines using their runtime. A bounded channel or pool limits the quantity documented by that API, not total process memory or CPU consumption. Pending sends can hold values outside a channel's buffer; received values and shared payloads can remain alive in application code. Unbounded channels, cached `OnceMap` entries, and concurrent work for distinct singleflight keys can grow with the workload.

Progress also depends on the caller: tasks must be polled, guards and permits must be released, and receivers must drain or disconnect. A slow receiver can stall a lossless bounded broadcast channel. Cyclic lock dependencies, retained guards, or blocking an executor thread can deadlock an application. The blocking adapter's timeout cannot interrupt a call to `Future::poll` that does not return.

These limits do not excuse a violation of a documented capacity or progress guarantee. For example, permanently losing permits after a supported cancellation sequence is a library defect, even when the application also needs its own admission control.

### Shared data and caller code

When results depend on caller identity or permissions, applications must include that context in `OnceMap` and singleflight keys, or use separate instances for different security domains. Equal keys deliberately share results within an instance. Object pools reuse application-managed objects; applications must validate and reset any tenant-specific or sensitive state before reuse. Asyncband does not erase secrets from released memory or redact arbitrary payloads exposed through their `Debug` implementations.

Callbacks, initializers, trait implementations, and destructors execute as application code and may block, panic, or perform I/O. Reentrancy and panic recovery follow the individual API's documentation. Locks that do not poison cannot restore an application's partially modified data after a panic. Executor waker operations, including clone, wake, and drop, are expected not to panic; recovery from panicking wakers and arbitrary clone reentrancy are not general guarantees. These limits on recovery do not waive the memory-safety obligations above.

## Assessing reports

Distinguish a library defect from its security impact. A report should identify the affected API contract, how the behavior is reachable, and the consequences for an application. Potential memory-safety violations through safe APIs merit private assessment even without a demonstrated exploit. Availability failures can also be security issues when an attacker can trigger them across an application's trust boundary; they are not automatically excluded because Asyncband is a library.

| Example                                                                                          | Assessment                                                                                                                  |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| Cancelling a receive while a sender completes causes use-after-free.                             | A memory-safety defect requiring private security assessment.                                                               |
| Supported cancellation permanently loses permits and lets requests exhaust a service's capacity. | A coordination defect; assess attacker reachability and availability impact.                                                |
| A receiver obtains a value sent to an unrelated channel instance.                                | A library isolation defect with potential confidentiality or integrity impact.                                              |
| Distinct users share a result because the application supplies equal keys to one singleflight.   | Application key and authorization design, unless Asyncband violates the documented key semantics.                           |
| An unbounded queue grows, or a bounded queue contains a few very large values.                   | Expected resource use requiring application limits, unless the library violates its documented accounting or cleanup rules. |
| A task retains a lock indefinitely or a caller callback never returns.                           | Application progress responsibility; the library provides no preemption of caller code.                                     |

The presence of `unsafe` code, a panic, or a dependency advisory alone does not establish exploitability. Maintainers assess the actual reachable behavior and affected versions with the Apache Security Team. Unclear impact is a reason to investigate privately, not a requirement for the reporter to develop an exploit. Ordinary correctness and performance bugs remain actionable through the public issue tracker.

## Maintaining this model

This model covers the library boundary. Suspected compromise of dependencies, release artifacts, or project infrastructure still warrants reporting through the appropriate [ASF security channel](https://www.apache.org/security/#reporting-a-vulnerability).

Revisit the model when public APIs change ownership, sharing, cancellation, or resource guarantees; when new runtime, I/O, or FFI surfaces are introduced; or when a report exposes an unaddressed boundary. Evaluate older releases against their actual behavior and documented contracts; this document does not retroactively change those contracts.

# like-a-clockwork

> Causal event tracking SDK for Rust, based on the work of Leslie Lamport (1978).

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

---

## The Problem

In microservice architectures, events are generated asynchronously across multiple services.
Without a causal ordering mechanism, it's impossible to answer questions like:

- Did service B process the response *before* service A finished writing?
- Did these two events happen in parallel, or did one cause the other?
- Why do logs show events out of order even with system timestamps?

Physical wall-clock timestamps don't solve this — each machine has its own clock with
drift and no guaranteed synchronization. The result: race condition bugs that are
extremely hard to reproduce and debug.

## The Solution

`like-a-clockwork` implements two complementary mechanisms from Lamport's seminal paper:

**Lamport Clock** — A monotonic logical counter per process. Guarantees that if event A
caused event B, then `clock(A) < clock(B)`. Simple, lightweight, provides total ordering.

**Vector Clock** — A vector of counters, one per process. Detects all three causal
relationships: *happens-before*, *happens-after*, and *concurrent*. Enables conflict
detection and race condition identification.

---

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
like-a-clockwork = { git = "https://github.com/gventino/like-a-clockwork.git" }
```

### Lamport Clock — Total Ordering

```rust
use like_a_clockwork::LamportClock;

// Each service creates its own clock
let mut order_svc = LamportClock::new("order-service");
let mut payment_svc = LamportClock::new("payment-service");

// Internal event
order_svc.tick(); // time = 1

// Sending a message: get a timestamp to propagate
let ts = order_svc.send(); // time = 2, returns LamportTimestamp

// Receiving: the other service syncs its clock
payment_svc.receive(&ts); // time = max(0, 2) + 1 = 3

// Guaranteed: payment's receive happened *after* order's send
assert!(payment_svc.time() > ts.time());
```

### Vector Clock — Concurrency Detection

```rust
use like_a_clockwork::{VectorClock, CausalityRelation};

let mut svc_a = VectorClock::new("svc-a", &["svc-b"]);
let mut svc_b = VectorClock::new("svc-b", &["svc-a"]);

// Both services process events independently
svc_a.tick(); // svc-a: {svc-a: 1, svc-b: 0}
svc_b.tick(); // svc-b: {svc-a: 0, svc-b: 1}

// Detect the relationship
match svc_a.relation(&svc_b) {
    CausalityRelation::Concurrent => {
        // These events are concurrent — possible race condition!
        println!("conflict detected: both services wrote independently");
    }
    CausalityRelation::HappensBefore => println!("A caused B"),
    CausalityRelation::HappensAfter  => println!("B caused A"),
    CausalityRelation::Equal         => println!("same causal state"),
}
```

### Traced Events — Causal Metadata Envelope

```rust
use like_a_clockwork::{VectorClock, TracedEvent};

let mut clock = VectorClock::new("order-service", &["payment-service"]);
clock.tick();

// Wrap any domain event with causal metadata
let event = TracedEvent::new(
    "order.created",
    b"{\"order_id\": 42}",
    clock.snapshot(),
).unwrap();

// Serialize to headers for transport (HTTP, Kafka, etc.)
let headers = event.to_headers();
// {
//   "X-Causality-Vector": "order-service=1,payment-service=0",
//   "X-Causality-EventId": "019476a0-b1c2-...",
//   "X-Causality-EventType": "order.created",
// }

// Reconstruct on the consumer side
let received = TracedEvent::from_headers(&headers, b"{\"order_id\": 42}").unwrap();
assert_eq!(received.event_type(), "order.created");
```

### Transport Layer — Framework Agnostic

The transport layer works with plain `HashMap`s — no framework dependencies.
You bridge it to your HTTP/Kafka/gRPC library of choice.

```rust
use like_a_clockwork::transport::text;
use like_a_clockwork::{LamportClock, VectorClock};
use std::collections::HashMap;

// === Text transport (HTTP headers, gRPC ASCII metadata) ===

let mut clock = VectorClock::new("api-gateway", &["auth-service"]);
let ts = clock.send();

let mut headers = HashMap::new();
text::inject_vector(&mut headers, &ts).unwrap();
// headers: {"X-Causality-Vector": "api-gateway=1,auth-service=0"}

// On the receiving end
let extracted = text::extract_vector(&headers).unwrap();
assert!(extracted.is_some());
```

```rust
use like_a_clockwork::transport::binary;
use like_a_clockwork::VectorClock;
use std::collections::HashMap;

// === Binary transport (Kafka record headers) ===

let mut clock = VectorClock::new("producer", &["consumer"]);
let ts = clock.send();

let mut headers: HashMap<String, Vec<u8>> = HashMap::new();
binary::inject_vector(&mut headers, &ts).unwrap();
// Serialized as compact msgpack bytes

let extracted = binary::extract_vector(&headers).unwrap();
assert!(extracted.is_some());
```

```rust
use like_a_clockwork::transport::json;
use like_a_clockwork::VectorClock;

// === JSON transport (embedded _causality field) ===

let mut clock = VectorClock::new("order-service", &["inventory-service"]);
clock.tick();

let payload = serde_json::json!({"order_id": 42, "status": "created"});
let enriched = json::inject(
    &payload,
    &clock.snapshot(),
    "order.created",
    "event-123",
).unwrap();

// Result:
// {
//   "order_id": 42,
//   "status": "created",
//   "_causality": {
//     "vector": {"order-service": 1, "inventory-service": 0},
//     "event_type": "order.created",
//     "event_id": "event-123"
//   }
// }

assert!(json::has_causality(&enriched));
```

---

## Use Cases

### 1. Distributed Log Ordering

Each service serializes its Vector Clock in message headers. A log aggregator
(Grafana Loki, Datadog, etc.) can reconstruct the causal graph and show exactly
which service caused what — without relying on physical timestamps.

### 2. Concurrent Write Detection

Two services read the same record and attempt to write. The Vector Clock detects
that the events are `Concurrent` before persisting — the application can apply a
merge strategy or reject with an explicit conflict.

```
order-service     [1, 0]  reads product #42
inventory-service [0, 1]  reads product #42 at the same time
→ CausalityRelation::Concurrent  ← both try to write
```

### 3. Causal Deduplication

A Kafka consumer tracks the last processed Vector Clock per key. If an incoming
event is `HappensBefore` the already-processed one, it's discarded as a duplicate.
If it's `Concurrent`, it enters a conflict resolution queue.

### 4. Race Condition Debugging

Development middleware captures all events from a request, reconstructs the causal
graph, and displays which services raced against each other in the terminal.

---

## Architecture

```
src/
├── lib.rs              # Public re-exports
├── lamport.rs          # LamportClock + LamportTimestamp
├── causality.rs        # CausalityRelation enum + compare()
├── vector.rs           # VectorClock + VectorTimestamp
├── event.rs            # TracedEvent causal envelope
└── transport/
    ├── mod.rs          # HeaderMap / BinaryHeaderMap traits + TransportError
    ├── text.rs         # Key-value text serialization (HTTP, gRPC ASCII)
    ├── binary.rs       # Key-value binary serialization (Kafka, gRPC binary)
    └── json.rs         # Embedded _causality in JSON payloads
```

### Design Principles

- **Zero framework dependencies** — the transport layer works with `HashMap<String, String>`
  and `HashMap<String, Vec<u8>>`. Integration with reqwest, axum, tonic, rdkafka, etc. is
  left to the user or future integration crates.
- **Correct by construction** — the API enforces Lamport's clock conditions at the type level.
  Clocks never regress, timestamps are immutable, and ordering is deterministic.
- **Serialization-ready** — all types derive `Serialize`/`Deserialize` via serde.

---

## Safety Properties

The SDK guarantees the three properties from Lamport's clock system:

**Clock Condition:** If event `a` caused event `b`, then `C(a) < C(b)`.

**Strong Clock Condition (Vector Clock):** `C(a) < C(b)` if and only if `a → b`.
This enables concurrency detection — if neither `C(a) < C(b)` nor `C(b) < C(a)`,
then `a ∥ b`.

**Monotonicity:** Clocks never regress. Any sequence of `tick()` / `send()` / `receive()`
produces strictly increasing values per node.

---

## What This SDK Is Not

- **Not a consensus system** — does not implement Paxos or Raft.
- **Not a distributed lock** — does not guarantee mutual exclusion.
- **Not a replacement for distributed tracing** — complementary to OpenTelemetry.
  A Trace ID says "this request passed through these services". A Vector Clock says
  "this event caused that one".
- **Not a message delivery guarantee** — that's the transport's job (Kafka, HTTP, etc.).

---

## References

- Lamport, L. (1978). [*Time, Clocks, and the Ordering of Events in a Distributed System.*](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)
  Communications of the ACM, 21(7), 558–565.
- Fidge, C. (1988). *Timestamps in Message-Passing Systems That Preserve the Partial Ordering.*
  Proceedings of the 11th Australian Computer Science Conference.
- Mattern, F. (1989). *Virtual Time and Global States of Distributed Systems.*
  Parallel and Distributed Algorithms, 215–226.

---

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.

---

*like-a-clockwork — built on the work of Leslie Lamport, 1978.*
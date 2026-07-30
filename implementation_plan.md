# HFT Order Matching Engine — C++20 Architecture Plan

> **Target**: Sub-microsecond matching latency · Millions of orders/sec throughput · Zero heap allocation on hot paths

---

## Executive Summary

We are building a kernel-bypassing, cache-optimized, lock-free Order Matching Engine (OME) in C++20 targeting Linux x86-64. The architecture follows a strict **"Share by Communicating"** philosophy — each stage of the pipeline owns its data exclusively and communicates via wait-free SPSC ring buffers, eliminating all synchronization primitives on the critical path.

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HFT Order Matching Engine                           │
│                                                                             │
│  ┌──────────────┐    SPSC RingBuf    ┌──────────────┐    SPSC RingBuf      │
│  │  I/O Thread  │ ─────────────────► │ Decode Thread│ ─────────────────►  │
│  │ (io_uring)   │                    │ (FIX/Binary) │                      │
│  └──────────────┘                    └──────────────┘                      │
│                                                                             │
│       SPSC RingBuf                    ┌──────────────────┐                 │
│  ◄────────────────────────────────── │  Matching Engine  │                 │
│  (Execution Reports / io_uring send) │   (Single Thread) │                 │
│                                      │   [OrderPool]     │                 │
│                                      │   [LOB per ticker]│                 │
│                                      └──────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Thread Model**: Each component runs on a **pinned, isolated CPU core** (via `pthread_setaffinity_np`). IRQ affinity is reconfigured to avoid matching-engine cores. No thread shares data directly.

---

## 2. Memory Architecture — Custom `OrderPool`

### Problem
`new`/`delete` (glibc malloc) on hot paths causes:
- Non-deterministic latency spikes (internal locking, coalescing, syscalls)
- Cache line pollution from allocator metadata
- Heap fragmentation increasing TLB pressure

### Solution — Slab-Style Memory Pool

```
OrderPool (pre-allocated at startup)
┌────────────────────────────────────────────────────┐
│  Order[0] │ Order[1] │ Order[2] │ ... │ Order[N-1] │
│  (64-byte aligned slabs, packed in contiguous RAM)  │
└────────────────────────────────────────────────────┘
        ▲
        │  acquire() — O(1), no lock, no syscall
        │              pops from a freelist stack
        │
        │  release() — O(1), no lock, no syscall
                       pushes back to freelist stack
```

**Key Design Decisions:**

| Property | Detail |
|---|---|
| Pre-allocation | `mmap(MAP_POPULATE)` at startup — all pages faulted in, zero runtime page faults |
| Alignment | Each `Order` struct aligned to **64 bytes** (one cache line) |
| Freelist | Intrusive singly-linked freelist embedded inside `Order` union — zero extra metadata |
| Capacity | Configurable `N` (e.g., 1M orders) sized to fit in L3 cache or huge pages |
| Huge Pages | `MAP_HUGETLB` (2 MB pages) to eliminate TLB misses across the pool |
| Thread Safety | Pool is **owned exclusively** by the Matching Engine thread — zero contention |

**`Order` struct layout (exactly 64 bytes):**

```cpp
struct alignas(64) Order {
    uint64_t  order_id;       //  8B — unique monotonic ID
    uint64_t  timestamp_ns;   //  8B — CLOCK_REALTIME_COARSE ingestion time
    int64_t   price;          //  8B — price in fixed-point ticks (×10000)
    uint32_t  quantity;       //  4B — remaining quantity
    uint32_t  orig_quantity;  //  4B — original quantity for fills
    uint32_t  instrument_id;  //  4B — instrument/ticker index
    uint8_t   side;           //  1B — BUY=0, SELL=1
    uint8_t   type;           //  1B — LIMIT=0, MARKET=1, IOC=2, FOK=3
    uint8_t   status;         //  1B — NEW, PARTIAL, FILLED, CANCELLED
    uint8_t   _pad0;          //  1B — padding
    Order*    next;           //  8B — intrusive list next ptr (queue or freelist)
    Order*    prev;           //  8B — intrusive list prev ptr (queue only)
    uint64_t  _pad1;          //  8B — future fields / pad to 64B
    //      TOTAL:  64 bytes ✓
};
```

---

## 3. Data Structure Design — Cache-Friendly Limit Order Book (LOB)

### Problem
A naive `std::map<price, std::queue<Order*>>` LOB has:
- Pointer chasing on every price level lookup (tree node → queue node)
- Cache misses at every level of traversal
- Dynamic allocation for tree nodes and queue nodes

### Solution — Flat Array + Intrusive Doubly-Linked List

**Price Level Indexing:**

Prices are mapped to array indices using a fixed tick grid:

```
index = (price_in_ticks - min_price) / tick_size
```

This converts O(log N) tree lookup → **O(1) direct array access**.

**LOB Structure per Instrument:**

```
BidLevels[]:   flat array indexed by tick, each element is a PriceLevel
AskLevels[]:   flat array indexed by tick, each element is a PriceLevel

struct PriceLevel {
    Order*   head;        // front of queue (next to match)
    Order*   tail;        // back of queue (new orders appended)
    int64_t  price;       // for validation / zero-check
    uint32_t total_qty;   // aggregate visible quantity
    uint32_t order_count; // number of resting orders
};  // 32 bytes — fits in half a cache line
```

**Order Queue at each PriceLevel — Intrusive Doubly-Linked List:**

```
PriceLevel.head ──► [Order_A] ──► [Order_B] ──► [Order_C] ──► nullptr
                    [Order_A] ◄── [Order_B] ◄── [Order_C]
                          PriceLevel.tail ─────────────────►  [Order_C]
```

- `enqueue`: O(1) — append to tail (new order arrives)
- `dequeue_head`: O(1) — remove from head (best price fills)
- `cancel_arbitrary`: O(1) — `prev->next = next; next->prev = prev` (no search needed because we keep an `Order*` in an `order_id → Order*` hash map)

**Best Bid/Ask Tracking:**

```cpp
// Two monotonic indices maintained by the matching engine:
int32_t best_bid_idx;  // highest occupied index in BidLevels
int32_t best_ask_idx;  // lowest occupied index in AskLevels
```

After a fill exhausts a level: scan inward from current best — O(gap) but amortized O(1) in practice under active markets.

**Memory Layout Optimization:**

| Technique | Benefit |
|---|---|
| `std::vector<PriceLevel>` (contiguous) | Prefetcher-friendly sequential access during sweep |
| Top-of-book levels pre-fetched | `__builtin_prefetch` on next N levels during matching loop |
| LOB per instrument pinned to NUMA node | No cross-socket memory traffic |
| Hot path touches ≤ 3 cache lines | Ingress order, PriceLevel, resting Order |

---

## 4. Concurrency Strategy — Lock-Free SPSC Ring Buffer

### Philosophy: Share by Communicating

No thread shares mutable state with another. All communication is via **message passing through ring buffers**. This eliminates mutexes, condition variables, and their associated latency spikes (typically 1–10 µs).

### SPSC Ring Buffer Design

```
             Producer (writer)                Consumer (reader)
                    │                                │
                    ▼                                ▼
 ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
 │ 0  │ 1  │ 2  │ 3* │ 4  │ 5  │ 6* │ 7  │ 8  │ 9  │
 └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
                  ▲                 ▲
             write_idx          read_idx
          (producer owns)    (consumer owns)
```

**Critical Implementation Details:**

```cpp
template<typename T, size_t N>
struct alignas(128) SPSCQueue {
    // Producer's cache line (128B to avoid false sharing on adjacent struct)
    alignas(64) std::atomic<size_t> write_idx{0};
    char _pad0[56];

    // Consumer's cache line
    alignas(64) std::atomic<size_t> read_idx{0};
    char _pad1[56];

    // Data — sized as power-of-2 for cheap modulo via bitmask
    std::array<T, N> buffer;  // N must be power of 2
};
```

| Property | Detail |
|---|---|
| **Size** | Power-of-2 → `idx & (N-1)` replaces expensive modulo |
| **False sharing prevention** | `write_idx` and `read_idx` on **separate 64-byte cache lines** |
| **Struct padding** | 128-byte outer alignment prevents adjacent struct false sharing |
| **Memory ordering** | `release` on write, `acquire` on read — no `seq_cst` (expensive mfence) |
| **Busy-wait strategy** | Consumer spins with `_mm_pause()` to reduce bus contention |
| **Batch draining** | Consumer drains in batches of 64 to amortize `atomic::load` overhead |

**Pipeline Queues:**

```
[I/O Thread] ──(RawBytes SPSC)──► [Decoder Thread] ──(OrderMsg SPSC)──► [Matching Engine]
[Matching Engine] ──(ExecReport SPSC)──► [Outbound I/O Thread] ──(io_uring send)──► clients
```

---

## 5. I/O Integration — Linux `io_uring`

### Problem with Traditional I/O
- `recv()` / `send()` syscalls: ~100–300 ns overhead each (context switch, kernel boundary)
- `epoll`: extra syscall per readiness notification
- At 10M orders/sec, syscall overhead alone exceeds budget

### Solution — `io_uring` with Fixed Buffers

**`io_uring` Eliminates the Kernel Boundary:**

```
Userspace                         Kernel
    │                                │
    │  ┌─────────────────────┐       │
    │  │  Submission Queue   │ ──────►│ (shared mmap, no syscall)
    │  │  (SQ ring)          │       │
    │  └─────────────────────┘       │
    │                                │  async I/O
    │  ┌─────────────────────┐       │  completion
    │  │  Completion Queue   │ ◄─────│
    │  │  (CQ ring)          │       │
    │  └─────────────────────┘       │
```

**Key Configuration:**

| Flag | Purpose |
|---|---|
| `IORING_SETUP_SQPOLL` | Kernel-side SQ polling thread — **zero syscalls** for submissions |
| `IORING_SETUP_IOPOLL` | Polling for completions — no interrupt overhead |
| `IORING_REGISTER_BUFFERS` | Pre-register receive buffers → zero copy, no `kmalloc` per I/O |
| `IORING_REGISTER_FILES` | Pre-register socket FDs → avoid fdtable lookup per op |
| Fixed buffer recvmsg | Incoming order bytes land directly into our pre-registered slab |

**Receive Flow:**

```
1. Pre-populate SQ with N RECV requests pointing to our registered buffer pool
2. Kernel SQ polling thread drains network → writes to our buffers → posts CQEs
3. I/O Thread polls CQ in tight loop (no syscall, no interrupt)
4. On CQE: extract byte span → post to Decoder SPSC queue → re-arm RECV into SQ
5. Zero copies: network DMA → registered buffer → decoder reads in place
```

**Outbound Flow (Execution Reports):**

```
1. Matching engine posts ExecReport to Outbound SPSC queue
2. Outbound I/O thread drains queue → builds SEND SQE pointing to fixed buffer
3. SQ polling thread submits to kernel → kernel sends → posts CQE
4. Outbound thread recycles send buffer on CQE completion
```

---

## 6. Latency Budget

| Stage | Target Latency |
|---|---|
| I/O receive (io_uring poll) | ~50 ns |
| Wire decode (binary protocol) | ~20 ns |
| Order routing (SPSC dequeue) | ~10 ns |
| Matching engine hot path | ~100–200 ns |
| Execution report generation | ~30 ns |
| I/O send (io_uring poll) | ~50 ns |
| **Total end-to-end** | **~300–400 ns** |

---

## 7. Key C++20 Features Leveraged

| Feature | Usage |
|---|---|
| `std::atomic` with `memory_order` | Lock-free SPSC, pool freelist CAS |
| `std::span` | Zero-copy buffer views in decoder |
| `[[likely]]` / `[[unlikely]]` | Branch prediction hints in matching loop |
| Concepts | Type-safe `MatchableOrder` concept |
| `std::bit_ceil` | Power-of-2 SPSC buffer sizing |
| Designated initializers | Readable order construction |
| `consteval` | Compile-time tick grid size validation |

---

## Open Questions

> [!IMPORTANT]
> **Protocol**: What wire protocol will the engine accept? Binary (e.g., ITCH/OUCH, proprietary) or FIX? This determines decoder complexity and SPSC message size.

> [!IMPORTANT]
> **Order Types**: Should we support GTC (Good-Till-Cancel), GTD, IOC, FOK, Market orders, and Stop orders in v1? Or start with LIMIT + MARKET only?

> [!IMPORTANT]
> **Number of Instruments**: How many instruments/tickers will the engine handle simultaneously? This determines LOB memory footprint and whether per-instrument threads are viable.

> [!IMPORTANT]
> **Deployment Target**: Is the target a dedicated bare-metal Linux server (ideal for `SQPOLL` + CPU pinning)? Or containerized (some kernel features may be restricted)?

> [!WARNING]
> **`io_uring` SQPOLL** requires `CAP_SYS_NICE` or running as root. If the deployment environment cannot grant this, we fall back to `io_uring_enter()` syscall submission (still ~50% faster than epoll).

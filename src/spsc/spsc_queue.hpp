#pragma once

// ─────────────────────────────────────────────────────────────────────────────
// spsc_queue.hpp — Lock-Free Single-Producer Single-Consumer Ring Buffer
//
// Constraints enforced:
//   • NO std::mutex / std::condition_variable / std::shared_mutex
//   • Memory ordering: std::memory_order_acquire / release ONLY
//     (no seq_cst — that emits costly mfence / lock xchg on x86)
//   • N must be a power of 2  → index wrap via bitmask, not modulo
//   • write_idx and read_idx on SEPARATE 64-byte cache lines
//     → eliminates false sharing between producer and consumer cores
//   • Outer struct aligned to 128 bytes
//     → prevents false sharing with an adjacent object in the same cache line
// ─────────────────────────────────────────────────────────────────────────────

#include <atomic>
#include <array>
#include <bit>        // std::has_single_bit  (C++20)
#include <cstddef>
#include <cstdint>
#include <type_traits>

// ── Platform pause hint ───────────────────────────────────────────────────────
// _mm_pause() lowers bus traffic when spinning; keeps the CPU from hammering
// the coherency fabric while waiting for the other core to catch up.
#if defined(__x86_64__) || defined(_M_X64)
#  include <immintrin.h>
#  define HFT_CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__)
#  define HFT_CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#  define HFT_CPU_PAUSE() ((void)0)
#endif

namespace hft::spsc {

// ─────────────────────────────────────────────────────────────────────────────
// SPSCQueue<T, N>
//
// A wait-free ring buffer for exactly one producer thread and one consumer
// thread.  All operations are non-blocking (they return immediately on
// full/empty); the caller decides whether to spin, yield, or drop.
//
// Template parameters
//   T — element type; must be trivially copyable (verified via static_assert)
//   N — ring capacity; must be a power of 2 (verified via static_assert)
//
// Index arithmetic
//   Indices are never masked before storage — they advance monotonically.
//   The bitmask is applied only when accessing buffer_[idx & kMask].
//   This makes the full/empty distinction unambiguous:
//     • empty : write_idx == read_idx
//     • full  : (write_idx - read_idx) == N
//   No "wasted slot" trick needed.
//
// Memory ordering rationale
//   Producer path
//     load read_idx  → acquire  (observe consumer's release of read_idx)
//     store write_idx→ release  (publish new element to consumer)
//   Consumer path
//     load write_idx → acquire  (observe producer's release of write_idx)
//     store read_idx → release  (publish consumed slot back to producer)
//   Relaxed loads of own index avoid redundant barriers (each thread owns
//   exactly one index).
//
// ─────────────────────────────────────────────────────────────────────────────

template<typename T, std::size_t N>
struct alignas(128) SPSCQueue {

    // ── Compile-time contracts ────────────────────────────────────────────────
    static_assert(std::has_single_bit(N),
        "SPSCQueue: N must be a power of 2 "
        "(enables cheap bitmask wrap instead of modulo)");

    static_assert(std::is_trivially_copyable_v<T>,
        "SPSCQueue: T must be trivially copyable "
        "(no lock-free guarantee otherwise)");

    static constexpr std::size_t kCapacity = N;
    static constexpr std::size_t kMask     = N - 1;

    // ── Producer cache line ───────────────────────────────────────────────────
    // Only the producer writes write_idx; the consumer reads it.
    // Placing it alone on a 64-byte cache line means the consumer's
    // cache-line invalidations never ping-pong this line.
    alignas(64) std::atomic<std::size_t> write_idx_{0};
    // Pad to exactly 64 bytes so the next field starts on a new line.
    char _pad_producer_[64 - sizeof(std::atomic<std::size_t>)]{};

    // ── Consumer cache line ───────────────────────────────────────────────────
    // Only the consumer writes read_idx; the producer reads it.
    alignas(64) std::atomic<std::size_t> read_idx_{0};
    char _pad_consumer_[64 - sizeof(std::atomic<std::size_t>)]{};

    // ── Ring buffer storage ───────────────────────────────────────────────────
    // Intentionally placed AFTER the index lines so the indices and the
    // data they protect do not share cache lines.
    std::array<T, N> buffer_{};

    // ─────────────────────────────────────────────────────────────────────────
    // try_push  (PRODUCER thread only)
    //
    // Enqueue one item by copy.  Returns true on success, false if full.
    // Never blocks; never allocates; never calls any OS primitive.
    // ─────────────────────────────────────────────────────────────────────────
    [[nodiscard]] bool try_push(const T& item) noexcept {
        // Relaxed: we own write_idx — no other thread writes it.
        const std::size_t w = write_idx_.load(std::memory_order_relaxed);

        // Acquire: we need to see the consumer's most-recent read_idx store
        // so we can judge whether the slot at [w] is free.
        if ((w - read_idx_.load(std::memory_order_acquire)) == N) {
            return false;   // ring is full
        }

        buffer_[w & kMask] = item;

        // Release: make the written element visible to the consumer before
        // we advance write_idx (happens-before relationship).
        write_idx_.store(w + 1, std::memory_order_release);
        return true;
    }

    // Move-optimised overload — identical logic, avoids copy for movable T.
    [[nodiscard]] bool try_push(T&& item) noexcept {
        const std::size_t w = write_idx_.load(std::memory_order_relaxed);

        if ((w - read_idx_.load(std::memory_order_acquire)) == N) {
            return false;
        }

        buffer_[w & kMask] = std::move(item);
        write_idx_.store(w + 1, std::memory_order_release);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // try_pop  (CONSUMER thread only)
    //
    // Dequeue one item into `out`.  Returns true on success, false if empty.
    // Never blocks; never allocates; never calls any OS primitive.
    // ─────────────────────────────────────────────────────────────────────────
    [[nodiscard]] bool try_pop(T& out) noexcept {
        // Relaxed: we own read_idx.
        const std::size_t r = read_idx_.load(std::memory_order_relaxed);

        // Acquire: we need to see the producer's most-recent write_idx store
        // (and, via the release/acquire pair, the element written before it).
        if (r == write_idx_.load(std::memory_order_acquire)) {
            return false;   // ring is empty
        }

        out = buffer_[r & kMask];

        // Release: tell the producer this slot is now free.
        read_idx_.store(r + 1, std::memory_order_release);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // try_push_batch  (PRODUCER thread only)
    //
    // Enqueue up to `count` items from `items[]`.
    // Returns the number actually enqueued (may be < count if ring fills).
    //
    // Why this is faster than N individual try_push calls:
    //   The read_idx acquire-load is done ONCE, then the available-slot count
    //   is computed.  All items up to that count are written without touching
    //   another atomic.  A single release store publishes the whole batch.
    // ─────────────────────────────────────────────────────────────────────────
    [[nodiscard]] std::size_t try_push_batch(const T* items, std::size_t count) noexcept {
        const std::size_t w         = write_idx_.load(std::memory_order_relaxed);
        const std::size_t available = N - (w - read_idx_.load(std::memory_order_acquire));
        const std::size_t n         = (count < available) ? count : available;

        for (std::size_t i = 0; i < n; ++i) {
            buffer_[(w + i) & kMask] = items[i];
        }

        if (n > 0) {
            write_idx_.store(w + n, std::memory_order_release);
        }
        return n;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // try_pop_batch  (CONSUMER thread only)
    //
    // Dequeue up to `max_count` items into `out[]`.
    // Returns the number actually dequeued.
    //
    // Same amortisation principle as try_push_batch: one acquire load,
    // one release store, zero intermediate atomics.
    // ─────────────────────────────────────────────────────────────────────────
    [[nodiscard]] std::size_t try_pop_batch(T* out, std::size_t max_count) noexcept {
        const std::size_t r         = read_idx_.load(std::memory_order_relaxed);
        const std::size_t available = write_idx_.load(std::memory_order_acquire) - r;
        const std::size_t n         = (max_count < available) ? max_count : available;

        for (std::size_t i = 0; i < n; ++i) {
            out[i] = buffer_[(r + i) & kMask];
        }

        if (n > 0) {
            read_idx_.store(r + n, std::memory_order_release);
        }
        return n;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Blocking convenience wrappers (spin with pause — for tests / benchmarks)
    // ─────────────────────────────────────────────────────────────────────────

    void spin_push(const T& item) noexcept {
        while (!try_push(item)) { HFT_CPU_PAUSE(); }
    }

    void spin_push(T&& item) noexcept {
        while (!try_push(std::move(item))) { HFT_CPU_PAUSE(); }
    }

    T spin_pop() noexcept {
        T out;
        while (!try_pop(out)) { HFT_CPU_PAUSE(); }
        return out;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Diagnostics (approximate — snapshot across two separate atomic loads)
    // ─────────────────────────────────────────────────────────────────────────

    [[nodiscard]] bool empty() const noexcept {
        return write_idx_.load(std::memory_order_acquire)
            == read_idx_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool full() const noexcept {
        const std::size_t w = write_idx_.load(std::memory_order_acquire);
        const std::size_t r = read_idx_.load(std::memory_order_acquire);
        return (w - r) == N;
    }

    [[nodiscard]] std::size_t size_approx() const noexcept {
        const std::size_t w = write_idx_.load(std::memory_order_acquire);
        const std::size_t r = read_idx_.load(std::memory_order_acquire);
        return w - r;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return N; }
};

// ─────────────────────────────────────────────────────────────────────────────
// Compile-time layout verification
// ─────────────────────────────────────────────────────────────────────────────
// Each index must fit in a single 64-byte cache line with its padding.
static_assert(sizeof(std::atomic<std::size_t>) + sizeof(char[64 - sizeof(std::atomic<std::size_t>)]) == 64,
    "Producer/consumer cache-line padding arithmetic is wrong");

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline message types  (used by the OME pipeline stages)
// All must be trivially copyable for lock-free queue correctness.
// ─────────────────────────────────────────────────────────────────────────────

// I/O → Decoder: a descriptor for a received byte span in a registered buffer.
struct RawByteSpan {
    const uint8_t* data   = nullptr; // pointer into io_uring registered buffer
    uint32_t       len    = 0;       // bytes received
    uint32_t       buf_id = 0;       // buffer index to re-arm after decode
};
static_assert(std::is_trivially_copyable_v<RawByteSpan>);

// Decoder → Matching Engine: a parsed order command.
struct InboundOrderMsg {
    enum class Kind : uint8_t { NewOrder = 0, Cancel = 1, Modify = 2 } kind;
    uint8_t  _pad0[3]     = {};
    uint64_t order_id     = 0;   // for Cancel / Modify
    int64_t  price        = 0;   // fixed-point ticks (NewOrder)
    uint32_t quantity     = 0;   // NewOrder / Modify new qty
    uint32_t instrument_id= 0;
    uint64_t client_id    = 0;
    uint8_t  side         = 0;   // hft::core::Side enum value
    uint8_t  type         = 0;   // hft::core::OrderType enum value
    uint8_t  _pad1[6]     = {};
};
static_assert(std::is_trivially_copyable_v<InboundOrderMsg>);

// Matching Engine → Outbound I/O: fill / cancel / reject notification.
struct ExecutionReport {
    enum class Kind : uint8_t {
        Fill = 0, PartialFill = 1, CancelAck = 2, Reject = 3
    } kind;
    uint8_t  _pad0[3]            = {};
    uint64_t order_id            = 0;
    uint64_t counterpart_order_id= 0;
    int64_t  exec_price          = 0;
    uint32_t exec_quantity       = 0;
    uint32_t leaves_quantity     = 0;
    uint64_t timestamp_ns        = 0;
};
static_assert(std::is_trivially_copyable_v<ExecutionReport>);

// ── Pipeline queue aliases ────────────────────────────────────────────────────
using RawByteQueue  = SPSCQueue<RawByteSpan,     65'536>;
using InboundQueue  = SPSCQueue<InboundOrderMsg, 65'536>;
using OutboundQueue = SPSCQueue<ExecutionReport, 65'536>;

} // namespace hft::spsc

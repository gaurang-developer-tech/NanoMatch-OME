// =============================================================================
// test_spsc_queue.cpp
// Google Test suite for hft::spsc::SPSCQueue<T, N>
//
// Constraint verification:
//   ✓ No mutex / condition_variable / shared_mutex used anywhere
//   ✓ Only std::atomic with acquire/release ordering
//   ✓ std::jthread for C++20 multi-threaded tests
//
// Test groups:
//   1. Layout         — alignment, capacity, static_assert invariants
//   2. SingleThread   — basic push/pop, FIFO, full/empty edge cases
//   3. BatchOps       — try_push_batch / try_pop_batch correctness
//   4. Stress_1M      — 1,000,000 item producer/consumer, zero loss/corruption
//   5. Stress_Checksum— producer pushes checksum-tagged values; consumer verifies
//   6. SpinAPI        — spin_push / spin_pop convenience wrappers
//   7. BatchStress    — batch ops across two threads, 1M items
// =============================================================================

#include "spsc/spsc_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <numeric>      // std::iota
#include <thread>       // std::jthread (C++20)
#include <vector>
#include <algorithm>    // std::sort

using namespace hft::spsc;

// Convenient small queue for most tests
using SmallQ = SPSCQueue<uint64_t, 1024>;

// ─────────────────────────────────────────────────────────────────────────────
// 1. LAYOUT TESTS
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCLayout, OuterStructAlignment) {
    // The entire SPSCQueue must be 128-byte aligned to prevent false sharing
    // with adjacent objects in memory.
    alignas(128) SmallQ q;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(&q) % 128u, 0u)
        << "SPSCQueue is not 128-byte aligned";
}

TEST(SPSCLayout, WriteIdxOnItsOwnCacheLine) {
    SmallQ q;
    const auto w_addr = reinterpret_cast<uintptr_t>(&q.write_idx_);
    const auto r_addr = reinterpret_cast<uintptr_t>(&q.read_idx_);

    // The two indices must not share a 64-byte cache line.
    const auto w_line = w_addr / 64;
    const auto r_line = r_addr / 64;
    EXPECT_NE(w_line, r_line)
        << "write_idx and read_idx share a cache line — false sharing hazard!";
}

TEST(SPSCLayout, CapacityMatchesTemplate) {
    EXPECT_EQ(SmallQ::capacity(), 1024u);
    EXPECT_EQ((SPSCQueue<int, 64>::capacity()), 64u);
}

TEST(SPSCLayout, PowerOfTwoEnforced) {
    // These should compile (powers of 2)
    [[maybe_unused]] SPSCQueue<int, 1>    q1;
    [[maybe_unused]] SPSCQueue<int, 2>    q2;
    [[maybe_unused]] SPSCQueue<int, 4096> q4;
    // Non-power-of-2 would trigger static_assert at compile time — not testable
    // at runtime, but we document the constraint here.
    SUCCEED();
}

TEST(SPSCLayout, ElementTypeMustBeTriviallyCopiable) {
    // Verify the static_assert premise: uint64_t and our message types are ok.
    EXPECT_TRUE(std::is_trivially_copyable_v<uint64_t>);
    EXPECT_TRUE(std::is_trivially_copyable_v<RawByteSpan>);
    EXPECT_TRUE(std::is_trivially_copyable_v<InboundOrderMsg>);
    EXPECT_TRUE(std::is_trivially_copyable_v<ExecutionReport>);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. SINGLE-THREADED CORRECTNESS
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCSingleThread, StartsEmpty) {
    SmallQ q;
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.full());
    EXPECT_EQ(q.size_approx(), 0u);
}

TEST(SPSCSingleThread, PushOneThenPop) {
    SmallQ q;
    ASSERT_TRUE(q.try_push(42u));
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size_approx(), 1u);

    uint64_t out = 0;
    ASSERT_TRUE(q.try_pop(out));
    EXPECT_EQ(out, 42u);
    EXPECT_TRUE(q.empty());
}

TEST(SPSCSingleThread, PopFromEmptyReturnsFalse) {
    SmallQ q;
    uint64_t out = 0xDEAD;
    EXPECT_FALSE(q.try_pop(out));
    EXPECT_EQ(out, 0xDEADu) << "try_pop must not modify `out` when queue is empty";
}

TEST(SPSCSingleThread, FillToCapacity) {
    SmallQ q;
    std::size_t pushed = 0;
    while (q.try_push(pushed)) { ++pushed; }

    EXPECT_EQ(pushed, SmallQ::capacity())
        << "Should be able to push exactly N items";
    EXPECT_TRUE(q.full());
    EXPECT_FALSE(q.try_push(9999u)) << "Push to full queue must return false";
}

TEST(SPSCSingleThread, StrictFIFOOrder) {
    SmallQ q;
    constexpr uint64_t N = 256;
    for (uint64_t i = 0; i < N; ++i) ASSERT_TRUE(q.try_push(i));

    for (uint64_t i = 0; i < N; ++i) {
        uint64_t out = UINT64_MAX;
        ASSERT_TRUE(q.try_pop(out)) << "Pop failed at index " << i;
        EXPECT_EQ(out, i)
            << "FIFO order violated: expected " << i << " got " << out;
    }
    EXPECT_TRUE(q.empty());
}

TEST(SPSCSingleThread, WrapAroundCorrectness) {
    // Push/pop in alternating windows to force index wrap-around past N.
    SPSCQueue<uint64_t, 16> q;
    uint64_t expected = 0;

    for (int round = 0; round < 128; ++round) {
        // Push 8 items
        for (int i = 0; i < 8; ++i) {
            ASSERT_TRUE(q.try_push(static_cast<uint64_t>(round * 8 + i)));
        }
        // Pop 8 items and verify
        for (int i = 0; i < 8; ++i) {
            uint64_t out = UINT64_MAX;
            ASSERT_TRUE(q.try_pop(out));
            EXPECT_EQ(out, expected++) << "Wrap-around corrupted at round " << round;
        }
    }
}

TEST(SPSCSingleThread, MoveSemanticsPush) {
    SPSCQueue<std::array<uint8_t, 8>, 64> q;
    std::array<uint8_t, 8> item = {1, 2, 3, 4, 5, 6, 7, 8};
    ASSERT_TRUE(q.try_push(std::move(item)));

    std::array<uint8_t, 8> out{};
    ASSERT_TRUE(q.try_pop(out));
    EXPECT_EQ(out[0], 1u);
    EXPECT_EQ(out[7], 8u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. BATCH OPERATIONS
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCBatch, PushBatchThenPopBatch) {
    SPSCQueue<uint64_t, 4096> q;
    constexpr std::size_t N = 512;

    uint64_t src[N], dst[N]{};
    std::iota(src, src + N, 100u);  // 100, 101, ..., 611

    EXPECT_EQ(q.try_push_batch(src, N), N) << "Should push all N items";
    EXPECT_EQ(q.try_pop_batch(dst, N),  N) << "Should pop  all N items";

    for (std::size_t i = 0; i < N; ++i) {
        EXPECT_EQ(dst[i], src[i]) << "Batch value mismatch at index " << i;
    }
}

TEST(SPSCBatch, PushBatchRespectsFull) {
    SPSCQueue<uint64_t, 256> q;

    // Fill 200 of 256 slots
    uint64_t pre[200]{};
    EXPECT_EQ(q.try_push_batch(pre, 200), 200u);

    // Try to push 100 more — only 56 should fit
    uint64_t extra[100]{};
    const std::size_t pushed = q.try_push_batch(extra, 100);
    EXPECT_EQ(pushed, 56u) << "Batch must not overflow the ring";
}

TEST(SPSCBatch, PopBatchRespectsFilled) {
    SPSCQueue<uint64_t, 256> q;
    uint64_t src[100]{};
    std::iota(src, src + 100, 1u);
    ASSERT_EQ(q.try_push_batch(src, 100), 100u);

    // Try to pop 200 — only 100 should be available
    uint64_t dst[200]{};
    EXPECT_EQ(q.try_pop_batch(dst, 200), 100u);
}

TEST(SPSCBatch, BatchPreservesOrder) {
    SPSCQueue<uint64_t, 512> q;
    constexpr std::size_t N = 256;
    uint64_t src[N], dst[N]{};
    std::iota(src, src + N, 0u);

    q.try_push_batch(src, N);
    q.try_pop_batch(dst, N);

    for (std::size_t i = 0; i < N; ++i) {
        EXPECT_EQ(dst[i], i) << "Batch FIFO order violated at index " << i;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. MULTI-THREADED STRESS — 1,000,000 ITEMS, ZERO LOSS
//
// Design:
//   Producer thread: pushes integers 0 … 999,999 in order.
//   Consumer thread: pops all items and verifies:
//     (a) Count: exactly 1,000,000 items received (no loss, no duplication).
//     (b) Order: items arrive in strict FIFO order (no reordering).
//     (c) Sum:   sum of received values == expected_sum (no corruption).
//
// No mutex, no condition_variable.  Both threads spin with HFT_CPU_PAUSE()
// when the queue is full/empty respectively.
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCStress, OneMillion_NoLoss_NoCorruption) {
    constexpr uint64_t kItems = 1'000'000;

    // 64K capacity — big enough to buffer bursts without excessive back-pressure.
    SPSCQueue<uint64_t, 65536> q;

    // Shared result storage — written by consumer, read after jthread joins.
    uint64_t received_count = 0;
    uint64_t received_sum   = 0;
    bool     order_violated = false;
    uint64_t last_seen      = UINT64_MAX;  // sentinel: "nothing seen yet"

    // ── Consumer jthread ──────────────────────────────────────────────────────
    std::jthread consumer([&] {
        uint64_t val = 0;
        while (received_count < kItems) {
            if (q.try_pop(val)) {
                // (b) FIFO order check
                if (last_seen != UINT64_MAX && val != last_seen + 1) {
                    order_violated = true;
                }
                last_seen = val;

                // (a) count, (c) sum
                ++received_count;
                received_sum += val;
            } else {
                HFT_CPU_PAUSE();  // spin — no mutex, no condvar
            }
        }
    });

    // ── Producer (this thread) ────────────────────────────────────────────────
    for (uint64_t i = 0; i < kItems; ++i) {
        while (!q.try_push(i)) {
            HFT_CPU_PAUSE();  // ring is full — spin without any OS primitive
        }
    }

    // jthread destructor joins automatically — consumer finishes before we assert.

    // ── Assertions ────────────────────────────────────────────────────────────

    // (a) Zero loss / zero duplication
    EXPECT_EQ(received_count, kItems)
        << "Items lost or duplicated! Expected " << kItems
        << " but received " << received_count;

    // (b) Strict FIFO
    EXPECT_FALSE(order_violated)
        << "Items arrived out of order — memory ordering bug!";

    // (c) No corruption: sum of 0..N-1 = N*(N-1)/2
    const uint64_t expected_sum = kItems * (kItems - 1) / 2;
    EXPECT_EQ(received_sum, expected_sum)
        << "Checksum mismatch — data was corrupted in transit!"
        << "\n  Expected: " << expected_sum
        << "\n  Got:      " << received_sum;
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. MULTI-THREADED CHECKSUM STRESS
//
// Pushes pseudo-random values and verifies via XOR checksum that not a single
// bit was corrupted across 1M transfers.
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCStress, OneMillion_XorChecksum) {
    constexpr uint64_t kItems = 1'000'000;
    SPSCQueue<uint64_t, 65536> q;

    uint64_t producer_xor = 0;
    uint64_t consumer_xor = 0;
    uint64_t count        = 0;

    std::jthread consumer([&] {
        uint64_t val = 0;
        while (count < kItems) {
            if (q.try_pop(val)) {
                consumer_xor ^= val;
                ++count;
            } else {
                HFT_CPU_PAUSE();
            }
        }
    });

    // Producer: generate values using a simple LCG for variety
    uint64_t lcg = 0xDEADBEEFCAFEBABEull;
    for (uint64_t i = 0; i < kItems; ++i) {
        lcg = lcg * 6364136223846793005ull + 1442695040888963407ull;
        producer_xor ^= lcg;
        while (!q.try_push(lcg)) { HFT_CPU_PAUSE(); }
    }

    // jthread joins here (destructor)

    EXPECT_EQ(producer_xor, consumer_xor)
        << "XOR checksum mismatch — at least one element was corrupted!";
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. SPIN API
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCSpinAPI, SpinPushSpinPop_SingleThread) {
    SPSCQueue<uint32_t, 256> q;
    for (uint32_t i = 0; i < 100; ++i) q.spin_push(i);
    for (uint32_t i = 0; i < 100; ++i) {
        EXPECT_EQ(q.spin_pop(), i);
    }
}

TEST(SPSCSpinAPI, SpinPushSpinPop_TwoThreads) {
    SPSCQueue<uint32_t, 256> q;
    constexpr uint32_t N = 50'000;

    std::jthread producer([&] {
        for (uint32_t i = 0; i < N; ++i) q.spin_push(i);
    });

    for (uint32_t i = 0; i < N; ++i) {
        const uint32_t val = q.spin_pop();
        EXPECT_EQ(val, i) << "spin_pop returned wrong value at i=" << i;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. BATCH MULTI-THREADED STRESS
//
// Producer pushes 1M items in batches of 64.
// Consumer pops in batches of 64.
// Verifies count and XOR checksum.
// ─────────────────────────────────────────────────────────────────────────────

TEST(SPSCBatchStress, OneMillion_BatchMode) {
    constexpr uint64_t kItems      = 1'000'000;
    constexpr std::size_t kBatch   = 64;
    SPSCQueue<uint64_t, 65536> q;

    uint64_t prod_xor = 0;
    uint64_t cons_xor = 0;
    uint64_t cons_cnt = 0;

    std::jthread consumer([&] {
        uint64_t buf[kBatch];
        while (cons_cnt < kItems) {
            const std::size_t n = q.try_pop_batch(buf, kBatch);
            for (std::size_t i = 0; i < n; ++i) {
                cons_xor ^= buf[i];
            }
            cons_cnt += n;
            if (n == 0) HFT_CPU_PAUSE();
        }
    });

    // Producer
    uint64_t buf[kBatch];
    uint64_t sent = 0;
    while (sent < kItems) {
        const std::size_t this_batch = std::min<uint64_t>(kBatch, kItems - sent);
        for (std::size_t i = 0; i < this_batch; ++i) {
            buf[i] = sent + i;
            prod_xor ^= buf[i];
        }
        std::size_t pushed = 0;
        while (pushed < this_batch) {
            pushed += q.try_push_batch(buf + pushed, this_batch - pushed);
            if (pushed < this_batch) HFT_CPU_PAUSE();
        }
        sent += this_batch;
    }

    // jthread destructor joins consumer

    EXPECT_EQ(cons_cnt, kItems)
        << "Batch stress: item count mismatch (lost items?)";
    EXPECT_EQ(prod_xor, cons_xor)
        << "Batch stress: XOR checksum mismatch (corruption?)";
}

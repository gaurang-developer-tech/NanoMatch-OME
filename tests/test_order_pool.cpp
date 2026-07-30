// =============================================================================
// test_order_pool.cpp
// Unit tests for hft::core::Order layout and hft::core::OrderPool mechanics.
//
// Test Groups:
//   1. OrderLayout      — size, alignment, field offsets, trivial-dtor guarantee
//   2. OrderPoolBasic   — acquire/release round-trip, O(1) freelist mechanics
//   3. OrderPoolCounts  — capacity/used/free counters, exhaustion guard
//   4. OrderPoolBounds  — is_from_this_pool sentinel, pointer provenance
//   5. OrderPoolHugePage— huge-page flag reported correctly (informational)
//   6. OrderPoolStress  — acquire all, release all, re-acquire all (regression)
// =============================================================================

#include "core/order.hpp"
#include "core/order_pool.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <vector>
#include <algorithm>

using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// 1. ORDER LAYOUT
// ─────────────────────────────────────────────────────────────────────────────

TEST(OrderLayout, SizeIsExactlyOneCacheLine) {
    // The architecture mandates exactly 64 bytes so one Order == one cache line.
    // If this fires, a field was added / padding changed without updating the
    // layout comment. Fix the struct, not this test.
    EXPECT_EQ(sizeof(Order), 64u)
        << "Order must be exactly 64 bytes (one cache line). "
           "Adjust struct layout or padding.";
}

TEST(OrderLayout, AlignmentIs64Bytes) {
    EXPECT_EQ(alignof(Order), 64u)
        << "Order must be 64-byte aligned to prevent false sharing.";
}

TEST(OrderLayout, TriviallyDestructible) {
    // Pool reuses slots without calling destructors.
    // A non-trivial dtor would cause silent resource leaks.
    EXPECT_TRUE(std::is_trivially_destructible_v<Order>)
        << "Order must be trivially destructible for pool reuse safety.";
}

TEST(OrderLayout, TriviallyCopyable) {
    // Needed for safe memcpy-based operations in the decoder path.
    EXPECT_TRUE(std::is_trivially_copyable_v<Order>);
}

TEST(OrderLayout, FieldOffsets) {
    // Verify hot fields are in the first 32 bytes (first half of cache line).
    // Matching engine reads these on every comparison.
    EXPECT_EQ(offsetof(Order, order_id),      0u);
    EXPECT_EQ(offsetof(Order, price),         8u);
    EXPECT_EQ(offsetof(Order, timestamp_ns), 16u);
    EXPECT_EQ(offsetof(Order, quantity),     24u);
    EXPECT_EQ(offsetof(Order, orig_quantity),28u);
    EXPECT_EQ(offsetof(Order, instrument_id),32u);
    EXPECT_EQ(offsetof(Order, side),         36u);
    EXPECT_EQ(offsetof(Order, type),         37u);
    EXPECT_EQ(offsetof(Order, status),       38u);

    // Intrusive pointers in second half — only touched when walking the queue.
    EXPECT_GE(offsetof(Order, next), 40u);
    EXPECT_GE(offsetof(Order, prev), 48u);
}

TEST(OrderLayout, ResetClearsAllFields) {
    Order o{};
    // Set some non-zero values
    o.order_id      = 999;
    o.price         = 12345;
    o.quantity      = 100;
    o.side          = Side::Sell;
    o.status        = OrderStatus::Partial;
    o.next          = reinterpret_cast<Order*>(0xDEADBEEF);
    o.prev          = reinterpret_cast<Order*>(0xCAFEBABE);

    o.reset();

    EXPECT_EQ(o.order_id,      kInvalidOrderId);
    EXPECT_EQ(o.price,         kInvalidPrice);
    EXPECT_EQ(o.quantity,      0u);
    EXPECT_EQ(o.orig_quantity, 0u);
    EXPECT_EQ(o.side,          Side::Buy);        // default enum value
    EXPECT_EQ(o.status,        OrderStatus::New);
    EXPECT_EQ(o.next,          nullptr);
    EXPECT_EQ(o.prev,          nullptr);
}

TEST(OrderLayout, HelperPredicates) {
    Order o{};
    o.side   = Side::Buy;
    o.status = OrderStatus::New;
    EXPECT_TRUE(o.is_buy());
    EXPECT_FALSE(o.is_sell());
    EXPECT_TRUE(o.is_active());
    EXPECT_FALSE(o.is_filled());
    EXPECT_FALSE(o.is_cancelled());

    o.side   = Side::Sell;
    o.status = OrderStatus::Filled;
    EXPECT_FALSE(o.is_buy());
    EXPECT_TRUE(o.is_sell());
    EXPECT_FALSE(o.is_active());
    EXPECT_TRUE(o.is_filled());
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. ORDER POOL BASIC MECHANICS
// ─────────────────────────────────────────────────────────────────────────────

// Use a small capacity for fast tests (no huge-page dependency).
static constexpr std::size_t kTestCapacity = 1024;
using TestPool = OrderPool<kTestCapacity>;

TEST(OrderPoolBasic, ConstructionDoesNotThrow) {
    EXPECT_NO_THROW({ TestPool pool; });
}

TEST(OrderPoolBasic, InitialCountsCorrect) {
    TestPool pool;
    EXPECT_EQ(pool.capacity(),   kTestCapacity);
    EXPECT_EQ(pool.free_count(), kTestCapacity);
    EXPECT_EQ(pool.used_count(), 0u);
    EXPECT_FALSE(pool.exhausted());
}

TEST(OrderPoolBasic, AcquireReturnsNonNull) {
    TestPool pool;
    Order* o = pool.acquire();
    ASSERT_NE(o, nullptr);
}

TEST(OrderPoolBasic, AcquiredOrderIsReset) {
    // reset() is called inside acquire() — caller gets a clean Order.
    TestPool pool;
    Order* o = pool.acquire();
    ASSERT_NE(o, nullptr);

    EXPECT_EQ(o->order_id,      kInvalidOrderId);
    EXPECT_EQ(o->price,         kInvalidPrice);
    EXPECT_EQ(o->quantity,      0u);
    EXPECT_EQ(o->next,          nullptr);
    EXPECT_EQ(o->prev,          nullptr);
    EXPECT_EQ(o->status,        OrderStatus::New);
}

TEST(OrderPoolBasic, AcquireDecrementsFreelist) {
    TestPool pool;
    pool.acquire();
    EXPECT_EQ(pool.free_count(), kTestCapacity - 1);
    EXPECT_EQ(pool.used_count(), 1u);
}

TEST(OrderPoolBasic, ReleaseIncrementsFreelist) {
    TestPool pool;
    Order* o = pool.acquire();
    ASSERT_NE(o, nullptr);
    pool.release(o);
    EXPECT_EQ(pool.free_count(), kTestCapacity);
    EXPECT_EQ(pool.used_count(), 0u);
}

TEST(OrderPoolBasic, AcquireReleaseRoundTrip) {
    TestPool pool;

    // Acquire and release 10 times — counts must return to initial state each time.
    for (int i = 0; i < 10; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr) << "Iteration " << i;
        EXPECT_EQ(pool.used_count(), 1u);
        pool.release(o);
        EXPECT_EQ(pool.used_count(), 0u);
    }
}

TEST(OrderPoolBasic, AcquiredPointerIsAligned) {
    TestPool pool;
    for (int i = 0; i < 16; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr);
        // Each Order must be 64-byte aligned (one cache line).
        EXPECT_EQ(reinterpret_cast<uintptr_t>(o) % 64u, 0u)
            << "Order* is not 64-byte aligned at iteration " << i;
        pool.release(o);
    }
}

TEST(OrderPoolBasic, LIFOBehavior) {
    // LIFO ensures recently-released (cache-warm) slots are reused first.
    TestPool pool;

    Order* first  = pool.acquire();
    Order* second = pool.acquire();
    ASSERT_NE(first,  nullptr);
    ASSERT_NE(second, nullptr);

    // Release in LIFO order: second then first
    pool.release(second);
    pool.release(first);

    // Re-acquire should give back 'first' (top of stack), then 'second'
    Order* re1 = pool.acquire();
    Order* re2 = pool.acquire();
    EXPECT_EQ(re1, first);
    EXPECT_EQ(re2, second);

    pool.release(re2);
    pool.release(re1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. POOL CAPACITY AND EXHAUSTION
// ─────────────────────────────────────────────────────────────────────────────

TEST(OrderPoolCounts, AcquireAllSlots) {
    TestPool pool;
    std::vector<Order*> acquired;
    acquired.reserve(kTestCapacity);

    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr) << "Pool exhausted prematurely at slot " << i;
        acquired.push_back(o);
    }

    EXPECT_EQ(pool.free_count(), 0u);
    EXPECT_EQ(pool.used_count(), kTestCapacity);
    EXPECT_TRUE(pool.exhausted());

    // Release all back
    for (Order* o : acquired) pool.release(o);

    EXPECT_EQ(pool.free_count(), kTestCapacity);
    EXPECT_FALSE(pool.exhausted());
}

TEST(OrderPoolCounts, ExhaustionReturnsNullptr) {
    TestPool pool;
    std::vector<Order*> acquired;
    acquired.reserve(kTestCapacity);

    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        acquired.push_back(pool.acquire());
    }

    // Pool is now empty — next acquire must return nullptr, not crash.
    Order* overflow = pool.acquire();
    EXPECT_EQ(overflow, nullptr)
        << "Pool must return nullptr when exhausted, not UB.";

    for (Order* o : acquired) pool.release(o);
}

TEST(OrderPoolCounts, AllPointersAreUnique) {
    // Each slot must be a distinct address — no double-issue of the same slot.
    TestPool pool;
    std::unordered_set<Order*> seen;
    seen.reserve(kTestCapacity);

    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr);
        auto [_, inserted] = seen.insert(o);
        EXPECT_TRUE(inserted) << "Duplicate pointer issued by pool at slot " << i;
    }

    for (Order* o : seen) pool.release(o);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. POOL BOUNDARY / PROVENANCE CHECKS
// ─────────────────────────────────────────────────────────────────────────────

TEST(OrderPoolBounds, AcquiredPointerBelongsToPool) {
    TestPool pool;
    Order* o = pool.acquire();
    ASSERT_NE(o, nullptr);
    EXPECT_TRUE(pool.is_from_this_pool(o));
    pool.release(o);
}

TEST(OrderPoolBounds, ExternalPointerNotFromPool) {
    TestPool pool;
    Order external_order{};
    EXPECT_FALSE(pool.is_from_this_pool(&external_order));
}

TEST(OrderPoolBounds, AllAcquiredPointersAreFromPool) {
    TestPool pool;
    std::vector<Order*> acquired;
    acquired.reserve(kTestCapacity);

    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr);
        EXPECT_TRUE(pool.is_from_this_pool(o))
            << "Slot " << i << " is outside pool slab — memory corruption?";
        acquired.push_back(o);
    }

    for (Order* o : acquired) pool.release(o);
}

TEST(OrderPoolBounds, PoolContiguousMemory) {
    // All slots must be contiguous — array-of-structs layout.
    // Verifies that mmap gave us a contiguous region.
    TestPool pool;
    std::vector<Order*> acquired;
    acquired.reserve(kTestCapacity);

    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        acquired.push_back(pool.acquire());
    }

    // Sort by address
    std::sort(acquired.begin(), acquired.end());

    for (std::size_t i = 1; i < acquired.size(); ++i) {
        const auto diff = reinterpret_cast<uintptr_t>(acquired[i])
                        - reinterpret_cast<uintptr_t>(acquired[i-1]);
        EXPECT_EQ(diff, sizeof(Order))
            << "Non-contiguous allocation between slot "
            << (i-1) << " and " << i << " (diff=" << diff << ")";
    }

    for (Order* o : acquired) pool.release(o);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. HUGE PAGE STATUS (INFORMATIONAL)
// ─────────────────────────────────────────────────────────────────────────────

TEST(OrderPoolHugePage, ReportsHugePageStatus) {
    // We don't assert huge pages are ON (kernel may not have them configured)
    // but we assert the flag is valid and the pool constructed successfully.
    TestPool pool;
    const bool hp = pool.huge_pages();
    // Just log — not a failure if false (CI environments rarely have huge pages).
    std::cout << "[INFO] OrderPool huge pages: " << (hp ? "YES" : "NO (fallback to 4KB)") << "\n";
    SUCCEED(); // informational only
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. STRESS / REGRESSION
// ─────────────────────────────────────────────────────────────────────────────

TEST(OrderPoolStress, AcquireAllReleaseAllReacquireAll) {
    // Full cycle: fill pool → drain pool → fill pool again.
    // Verifies freelist is rebuilt correctly after a full release sweep.
    TestPool pool;
    std::vector<Order*> batch;
    batch.reserve(kTestCapacity);

    // === ROUND 1 ===
    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr) << "Round 1 exhausted early at " << i;
        // Write a sentinel value to verify reset on next acquire
        o->order_id = static_cast<OrderId>(i + 1);
        batch.push_back(o);
    }
    EXPECT_TRUE(pool.exhausted());

    // Release in scrambled order (simulates real workload)
    for (std::size_t i = 0; i < kTestCapacity; i += 2) pool.release(batch[i]);
    for (std::size_t i = 1; i < kTestCapacity; i += 2) pool.release(batch[i]);
    batch.clear();
    EXPECT_EQ(pool.free_count(), kTestCapacity);

    // === ROUND 2 — all slots must be clean (reset by acquire) ===
    for (std::size_t i = 0; i < kTestCapacity; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr) << "Round 2 exhausted early at " << i;
        // Sentinel from Round 1 must be gone
        EXPECT_EQ(o->order_id, kInvalidOrderId)
            << "Slot " << i << " not cleaned by acquire() — reset() missing?";
        batch.push_back(o);
    }

    for (Order* o : batch) pool.release(o);
}

TEST(OrderPoolStress, InterleavedAcquireRelease) {
    // Simulate the matching engine hot loop:
    // acquire → use → release in tight alternating pattern.
    TestPool pool;
    constexpr int kIterations = 100'000;

    for (int i = 0; i < kIterations; ++i) {
        Order* o = pool.acquire();
        ASSERT_NE(o, nullptr) << "Exhausted at iteration " << i;
        o->order_id = static_cast<OrderId>(i);
        o->quantity = 100;
        // Simulate a fill — update quantity
        o->quantity = 0;
        o->status   = OrderStatus::Filled;
        pool.release(o);
    }

    // Pool must be fully returned to initial state
    EXPECT_EQ(pool.free_count(), kTestCapacity);
    EXPECT_EQ(pool.used_count(), 0u);
}

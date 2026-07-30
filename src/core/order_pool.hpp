#pragma once

#include "order.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cassert>
#include <stdexcept>
#include <sys/mman.h>   // mmap, munmap, MAP_HUGETLB, MAP_POPULATE
#include <new>          // std::hardware_destructive_interference_size

namespace hft::core {

// ─── OrderPool ────────────────────────────────────────────────────────────────
//
// A fixed-capacity, slab-style memory pool for Order objects.
//
// Design Principles:
//   • All memory is pre-faulted at construction via MAP_POPULATE — zero
//     runtime page faults on the hot path.
//   • Backing store uses MAP_HUGETLB (2 MB pages) when available, falling
//     back to standard 4 KB pages if the kernel cannot satisfy the request.
//   • The freelist is an intrusive singly-linked stack embedded in Order::next,
//     requiring no external metadata and touching no extra cache lines.
//   • acquire() / release() are O(1) and contain no locks.
//
// Threading:
//   The pool is designed to be owned by a SINGLE thread (the Matching Engine).
//   All acquire/release calls must come from that thread. No atomics needed.
//
// Template Parameters:
//   Capacity — maximum number of concurrent live orders. Must be >= 1.
//
template<std::size_t Capacity>
class OrderPool {
    static_assert(Capacity >= 1, "OrderPool capacity must be at least 1");

public:
    // ── Construction / Destruction ────────────────────────────────────────────

    OrderPool() {
        const std::size_t bytes = Capacity * sizeof(Order);

        // Attempt huge-page backed allocation first (2 MB pages).
        // Requires kernel huge page availability (/proc/sys/vm/nr_hugepages).
        storage_ = static_cast<Order*>(
            ::mmap(nullptr, bytes,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB,
                   -1, 0)
        );

        if (storage_ == MAP_FAILED) {
            // Fallback: standard 4 KB pages, still pre-faulted via MAP_POPULATE.
            storage_ = static_cast<Order*>(
                ::mmap(nullptr, bytes,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                       -1, 0)
            );

            if (storage_ == MAP_FAILED) {
                throw std::runtime_error(
                    "OrderPool: mmap failed — out of virtual memory");
            }
            huge_pages_active_ = false;
        } else {
            huge_pages_active_ = true;
        }

        bytes_allocated_ = bytes;
        build_freelist();
    }

    ~OrderPool() noexcept {
        if (storage_ != nullptr && storage_ != MAP_FAILED) {
            ::munmap(storage_, bytes_allocated_);
        }
    }

    // Non-copyable, non-movable (owns mmap region)
    OrderPool(const OrderPool&)            = delete;
    OrderPool& operator=(const OrderPool&) = delete;
    OrderPool(OrderPool&&)                 = delete;
    OrderPool& operator=(OrderPool&&)      = delete;

    // ── acquire() ─────────────────────────────────────────────────────────────
    //
    // Pop a free Order from the freelist. O(1), no allocation.
    // Returns nullptr if the pool is exhausted (caller must handle gracefully).
    //
    [[nodiscard]] Order* acquire() noexcept {
        if (__builtin_expect(freelist_head_ == nullptr, 0)) {
            return nullptr;  // pool exhausted
        }
        Order* order       = freelist_head_;
        freelist_head_     = freelist_head_->next;
        order->reset();    // clear all fields before handing to caller
        --free_count_;
        return order;
    }

    // ── release() ─────────────────────────────────────────────────────────────
    //
    // Return an Order back to the freelist. O(1), no deallocation.
    // The pointer must have been obtained from this pool's acquire().
    //
    void release(Order* order) noexcept {
        assert(order != nullptr);
        assert(is_from_this_pool(order) && "Releasing a pointer not from this pool");
        order->next    = freelist_head_;
        order->prev    = nullptr;
        freelist_head_ = order;
        ++free_count_;
    }

    // ── reset() ───────────────────────────────────────────────────────────────
    //
    // Reset all order slots back to the freelist. O(Capacity), zero heap allocation.
    // Useful for resetting benchmarks or clearing state between trading sessions.
    //
    void reset() noexcept {
        build_freelist();
    }

    // ── Diagnostics ───────────────────────────────────────────────────────────

    [[nodiscard]] std::size_t capacity()   const noexcept { return Capacity;    }
    [[nodiscard]] std::size_t free_count() const noexcept { return free_count_; }
    [[nodiscard]] std::size_t used_count() const noexcept { return Capacity - free_count_; }
    [[nodiscard]] bool        huge_pages() const noexcept { return huge_pages_active_; }
    [[nodiscard]] bool        exhausted()  const noexcept { return freelist_head_ == nullptr; }

    // Returns true if `ptr` was allocated from this pool's storage slab.
    [[nodiscard]] bool is_from_this_pool(const Order* ptr) const noexcept {
        return ptr >= storage_ && ptr < storage_ + Capacity;
    }

private:
    // ── Private helpers ───────────────────────────────────────────────────────

    // Link all Order slots into the freelist at pool construction time.
    // Runs once; afterwards acquire/release are purely pointer-manipulation.
    void build_freelist() noexcept {
        freelist_head_ = nullptr;
        free_count_    = Capacity;

        // Build in reverse so slot 0 is at the front (LIFO, cache warm)
        for (std::size_t i = Capacity; i-- > 0; ) {
            storage_[i].next = freelist_head_;
            storage_[i].prev = nullptr;
            freelist_head_   = &storage_[i];
        }
    }

    // ── Data members ──────────────────────────────────────────────────────────

    Order*      storage_           = nullptr;
    Order*      freelist_head_     = nullptr;
    std::size_t free_count_        = 0;
    std::size_t bytes_allocated_   = 0;
    bool        huge_pages_active_ = false;
};

// ─── Convenience type alias ───────────────────────────────────────────────────
// Default pool: 1 million concurrent orders (~64 MB, fits in typical L3 + huge pages).
inline constexpr std::size_t kDefaultPoolCapacity = 1'000'000;
using DefaultOrderPool = OrderPool<kDefaultPoolCapacity>;

} // namespace hft::core

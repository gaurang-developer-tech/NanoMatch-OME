#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>

namespace hft::core {

// ─── Enumerations ─────────────────────────────────────────────────────────────

enum class Side : uint8_t {
    Buy  = 0,
    Sell = 1,
};

enum class OrderType : uint8_t {
    Limit  = 0,
    Market = 1,
    IOC    = 2,   // Immediate-or-Cancel
    FOK    = 3,   // Fill-or-Kill
};

enum class OrderStatus : uint8_t {
    New       = 0,
    Partial   = 1,
    Filled    = 2,
    Cancelled = 3,
    Rejected  = 4,
};

// ─── Type Aliases ─────────────────────────────────────────────────────────────

using OrderId      = uint64_t;
using InstrumentId = uint32_t;
using Quantity     = uint32_t;

// Price is stored in fixed-point integer ticks (multiply by tick_size for real price).
// E.g., price 123.45 with tick_size=0.01 → stored as 12345.
using Price = int64_t;

// Sentinel for invalid/null values
inline constexpr OrderId      kInvalidOrderId      = 0;
inline constexpr InstrumentId kInvalidInstrumentId = 0xFFFF'FFFFu;
inline constexpr Price        kInvalidPrice        = INT64_MIN;

// ─── Order Struct — exactly 64 bytes, cache-line aligned ──────────────────────
//
// Layout is carefully chosen so all hot fields fit within the first 32 bytes
// (accessed during matching). Intrusive list pointers occupy the second 32B.
//
// The `next` field is dual-purpose:
//   • When the order is live in the LOB  → points to next order in queue
//   • When the order is free in the pool → points to next free slot (freelist)
//
struct alignas(64) Order {
    // ── Hot fields (first cache line half) ────────────────────────────────────
    OrderId      order_id;        //  8B  unique monotonic order ID
    Price        price;           //  8B  fixed-point price in ticks
    uint64_t     timestamp_ns;    //  8B  ingestion timestamp (CLOCK_MONOTONIC_RAW)
    Quantity     quantity;        //  4B  remaining quantity (decremented on fills)
    Quantity     orig_quantity;   //  4B  original quantity (for reporting)
    InstrumentId instrument_id;   //  4B  which instrument this order belongs to
    Side         side;            //  1B  Buy or Sell
    OrderType    type;            //  1B  Limit / Market / IOC / FOK
    OrderStatus  status;          //  1B  lifecycle status
    uint8_t      _pad0;           //  1B  explicit padding

    // ── Intrusive list pointers (second cache line half) ──────────────────────
    Order*       next;            //  8B  next in queue (or freelist when free)
    Order*       prev;            //  8B  prev in queue (nullptr when free)

    // ── Reserved / future fields ──────────────────────────────────────────────
    uint64_t     client_id;       //  8B  originating client/session ID
    uint64_t     _reserved;       //  8B  pad to exactly 64 bytes

    // ── Helpers ───────────────────────────────────────────────────────────────
    [[nodiscard]] bool is_buy()  const noexcept { return side == Side::Buy;  }
    [[nodiscard]] bool is_sell() const noexcept { return side == Side::Sell; }
    [[nodiscard]] bool is_filled()    const noexcept { return status == OrderStatus::Filled;    }
    [[nodiscard]] bool is_cancelled() const noexcept { return status == OrderStatus::Cancelled; }
    [[nodiscard]] bool is_active()    const noexcept {
        return status == OrderStatus::New || status == OrderStatus::Partial;
    }

    // Zero out all fields (used by OrderPool on acquire)
    void reset() noexcept {
        order_id      = kInvalidOrderId;
        price         = kInvalidPrice;
        timestamp_ns  = 0;
        quantity      = 0;
        orig_quantity = 0;
        instrument_id = kInvalidInstrumentId;
        side          = Side::Buy;
        type          = OrderType::Limit;
        status        = OrderStatus::New;
        _pad0         = 0;
        next          = nullptr;
        prev          = nullptr;
        client_id     = 0;
        _reserved     = 0;
    }
};

// ─── Compile-Time Layout Assertions ───────────────────────────────────────────
static_assert(sizeof(Order) == 128,
    "Order must be exactly 128 bytes (two cache lines)");
static_assert(alignof(Order) == 64,
    "Order must be 64-byte aligned to avoid false sharing");
static_assert(std::is_trivially_destructible_v<Order>,
    "Order must be trivially destructible for pool reuse without calling dtors");

} // namespace hft::core

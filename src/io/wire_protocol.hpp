#pragma once

// =============================================================================
// wire_protocol.hpp — Binary Wire Protocol for HFT Order Ingestion
//
// Design goals:
//   • Fixed-size, tag-free messages — no parsing ambiguity, O(1) decode
//   • All fields naturally aligned — safe reinterpret_cast / memcpy
//   • Little-endian (native x86-64)
//   • Message type + length in first 4 bytes of every message (common header)
//     → decoder can safely skip unknown message types
//
// Inbound message types  (client → exchange):
//   0x0001  WireNewOrder    (40 bytes)
//   0x0002  WireCancelOrder (16 bytes)
//   0x0003  WireModifyOrder (20 bytes)  [future]
//
// Outbound message types (exchange → client):
//   0x0010  WireExecReport  (48 bytes)
//
// A single TCP recv() may contain one or more concatenated messages.
// The decoder must iterate using hdr.msg_len to advance through the span.
// =============================================================================

#include <cstdint>
#include <cstddef>
#include <type_traits>

namespace hft::io {

// ─────────────────────────────────────────────────────────────────────────────
// Message type registry
// ─────────────────────────────────────────────────────────────────────────────
enum class WireMsgType : uint16_t {
    NewOrder     = 0x0001,
    CancelOrder  = 0x0002,
    ModifyOrder  = 0x0003,  // reserved for Phase 4 Modify
    ExecReport   = 0x0010,  // outbound only
};

// ─────────────────────────────────────────────────────────────────────────────
// Common header — present at offset 0 of every message
// ─────────────────────────────────────────────────────────────────────────────
struct WireHeader {
    uint16_t msg_type;   // WireMsgType cast to uint16_t
    uint16_t msg_len;    // total message length in bytes (including this header)
};
static_assert(sizeof(WireHeader) == 4);
static_assert(std::is_trivially_copyable_v<WireHeader>);

// ─────────────────────────────────────────────────────────────────────────────
// WireNewOrder — 0x0001 (40 bytes)
//
// Carries all fields needed to construct an Order.
// Market orders set price = 0 (ignored by matching logic).
// ─────────────────────────────────────────────────────────────────────────────
struct WireNewOrder {
    // ── Header (4 bytes) ──────────────────────────────────────────────────────
    uint16_t msg_type;        // = 0x0001
    uint16_t msg_len;         // = 40

    // ── Identification (12 bytes) ─────────────────────────────────────────────
    uint32_t instrument_id;   // which instrument
    uint64_t order_id;        // client-assigned order ID (must be unique per session)

    // ── Order parameters (24 bytes) ───────────────────────────────────────────
    uint64_t client_id;       // session / trader identifier
    int64_t  price;           // fixed-point ticks; 0 for Market/IOC/FOK at market
    uint32_t quantity;        // shares / contracts

    // ── Type flags (4 bytes) ──────────────────────────────────────────────────
    uint8_t  side;            // 0 = Buy, 1 = Sell
    uint8_t  order_type;      // 0 = Limit, 1 = Market, 2 = IOC, 3 = FOK
    uint8_t  _pad[2];         // explicit padding to 40 bytes
    // TOTAL: 4 + 4 + 4 + 8 + 8 + 8 + 4 + 1 + 1 + 2 = 40 bytes ✓ (wait let me recount)
    // 2+2+4+8+8+8+4+1+1+2 = 40 bytes ✓
};
static_assert(sizeof(WireNewOrder) == 40,
    "WireNewOrder must be 40 bytes");
static_assert(std::is_trivially_copyable_v<WireNewOrder>);

// ─────────────────────────────────────────────────────────────────────────────
// WireCancelOrder — 0x0002 (16 bytes)
// ─────────────────────────────────────────────────────────────────────────────
struct WireCancelOrder {
    uint16_t msg_type;        // = 0x0002
    uint16_t msg_len;         // = 16
    uint32_t instrument_id;
    uint64_t order_id;        // which order to cancel
    // TOTAL: 2+2+4+8 = 16 bytes ✓
};
static_assert(sizeof(WireCancelOrder) == 16);
static_assert(std::is_trivially_copyable_v<WireCancelOrder>);

// ─────────────────────────────────────────────────────────────────────────────
// WireExecReport — 0x0010 (48 bytes) — outbound
// ─────────────────────────────────────────────────────────────────────────────
struct WireExecReport {
    uint16_t msg_type;                // = 0x0010
    uint16_t msg_len;                 // = 48
    uint8_t  kind;                    // ExecutionReport::Kind cast to uint8_t
    uint8_t  _pad0[3];               // alignment
    uint64_t order_id;
    uint64_t counterpart_order_id;
    int64_t  exec_price;
    uint32_t exec_quantity;
    uint32_t leaves_quantity;
    uint64_t timestamp_ns;
    // TOTAL: 2+2+1+3+8+8+8+4+4+8 = 48 bytes ✓
};
static_assert(sizeof(WireExecReport) == 48);
static_assert(std::is_trivially_copyable_v<WireExecReport>);

// ─────────────────────────────────────────────────────────────────────────────
// Builder helpers — construct outbound wire messages without manual field setting
// ─────────────────────────────────────────────────────────────────────────────

[[nodiscard]] inline WireExecReport
build_exec_report(const struct ExecutionReportFields& r) noexcept;

// Convenience: minimum message length for each known type
[[nodiscard]] inline constexpr std::size_t
min_msg_len(WireMsgType t) noexcept {
    switch (t) {
        case WireMsgType::NewOrder:    return sizeof(WireNewOrder);
        case WireMsgType::CancelOrder: return sizeof(WireCancelOrder);
        case WireMsgType::ExecReport:  return sizeof(WireExecReport);
        default:                       return sizeof(WireHeader);
    }
}

} // namespace hft::io

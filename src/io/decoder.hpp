#pragma once

// =============================================================================
// decoder.hpp — Binary Protocol Decoder
//
// Responsibility:
//   Runs on the Decoder thread.
//   Drains RawByteSpan messages from the RawByteQueue SPSC (posted by the
//   UringReceiver), parses the binary wire protocol (see wire_protocol.hpp),
//   and pushes InboundOrderMsg structs to the InboundQueue for the Matching
//   Engine thread.
//
// After processing each RawByteSpan, the decoder pushes the span's buf_id to
// the BufReturnQueue so the UringReceiver can re-arm that fixed buffer.
//
// Buffer lifecycle (zero-copy hot path):
//
//   [Network] ──DMA──► [Fixed Buffer buf_id]
//      ↓
//   UringReceiver posts RawByteSpan{data=fixed_buf_ptr, len, buf_id}
//      ↓  (via RawByteQueue SPSC)
//   Decoder reads bytes, parses, pushes InboundOrderMsg
//      ↓  (via BufReturnQueue SPSC)
//   UringReceiver re-arms buf_id with new RECV SQE
//
// Thread safety:
//   Decoder methods are NOT thread-safe.  Call only from the Decoder thread.
//   Queue interaction uses the SPSC lock-free ordering guarantees.
// =============================================================================

#include "io/wire_protocol.hpp"
#include "spsc/spsc_queue.hpp"
#include "core/order.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>    // std::memcpy
#include <cassert>

namespace hft::io {

using namespace hft::spsc;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// Buffer-return queue type alias
//
// The decoder pushes freed buffer IDs here; the UringReceiver drains them
// and re-arms the corresponding fixed buffers.
// ─────────────────────────────────────────────────────────────────────────────
using BufReturnQueue = SPSCQueue<uint32_t, 1024>;

// ─────────────────────────────────────────────────────────────────────────────
// Decoder
// ─────────────────────────────────────────────────────────────────────────────
class Decoder {
public:

    Decoder() = default;

    // Non-copyable, non-movable.
    Decoder(const Decoder&)            = delete;
    Decoder& operator=(const Decoder&) = delete;

    // ── poll() — main decoder loop ────────────────────────────────────────────
    //
    // Drains up to max_batch RawByteSpans from `raw`.
    // For each span:
    //   1. Iterates through concatenated wire messages in the byte buffer.
    //   2. Parses each message into an InboundOrderMsg.
    //   3. Pushes each parsed message to `inbound`.
    //   4. Pushes span.buf_id to `returns` so UringReceiver can re-arm it.
    //
    // Returns number of spans processed.
    //
    int poll(RawByteQueue&    raw,
             InboundQueue&    inbound,
             BufReturnQueue&  returns,
             std::size_t      max_batch = 64) noexcept
    {
        RawByteSpan span{};
        int spans_processed = 0;

        while (spans_processed < static_cast<int>(max_batch)
               && raw.try_pop(span))
        {
            decode_span(span, inbound);

            // Signal the receiver that this buffer slot is free.
            // Spin if the return queue is momentarily full (extremely rare).
            while (!returns.try_push(span.buf_id)) { /* spin */ }

            ++spans_processed;
        }

        return spans_processed;
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    [[nodiscard]] uint64_t decoded_count() const noexcept { return decoded_count_; }
    [[nodiscard]] uint64_t error_count()   const noexcept { return error_count_;   }
    [[nodiscard]] uint64_t spans_seen()    const noexcept { return spans_seen_;    }

    // ── decode_span() — exposed for unit testing ───────────────────────────────
    //
    // Parses all wire messages in `span` and pushes InboundOrderMsgs to `inbound`.
    // Returns the number of valid messages decoded.
    //
    // This method is public so tests can feed arbitrary byte arrays without
    // needing the full SPSC machinery or io_uring buffers.
    //
    int decode_span(const RawByteSpan& span, InboundQueue& inbound) noexcept {
        ++spans_seen_;
        if (span.data == nullptr || span.len == 0) return 0;

        const uint8_t* ptr = span.data;
        const uint8_t* end = ptr + span.len;
        int count = 0;

        while (ptr + sizeof(WireHeader) <= end) {
            // Safe read of header (may be unaligned in stream — use memcpy).
            WireHeader hdr{};
            std::memcpy(&hdr, ptr, sizeof(WireHeader));

            // Sanity: message must be at least header-sized and fit in span.
            if (hdr.msg_len < sizeof(WireHeader)
                || static_cast<std::size_t>(hdr.msg_len) > static_cast<std::size_t>(end - ptr))
            {
                ++error_count_;
                break;  // remainder of span is corrupt; abandon
            }

            InboundOrderMsg msg{};
            bool ok = false;

            switch (hdr.msg_type) {
                case static_cast<uint16_t>(WireMsgType::NewOrder):
                    ok = parse_new_order(ptr, hdr.msg_len, msg);
                    break;

                case static_cast<uint16_t>(WireMsgType::CancelOrder):
                    ok = parse_cancel_order(ptr, hdr.msg_len, msg);
                    break;

                default:
                    // Unknown type — skip over it using msg_len.
                    ++error_count_;
                    break;
            }

            if (ok) {
                // Spin-push: the inbound queue is consumed by the ME thread.
                // In steady state it should never be full.
                while (!inbound.try_push(msg)) { /* spin */ }
                ++decoded_count_;
                ++count;
            }

            ptr += hdr.msg_len;
        }

        return count;
    }

private:

    // ── parse_new_order() ─────────────────────────────────────────────────────
    //
    // Deserialise a WireNewOrder at `data[0..len)` into `out`.
    // Returns false if `len` is too small (truncated message — protocol error).
    //
    [[nodiscard]] static bool
    parse_new_order(const uint8_t* data, uint16_t len, InboundOrderMsg& out) noexcept
    {
        if (len < sizeof(WireNewOrder)) return false;

        WireNewOrder wire{};
        std::memcpy(&wire, data, sizeof(WireNewOrder));

        out.kind          = InboundOrderMsg::Kind::NewOrder;
        out.order_id      = wire.order_id;
        out.client_id     = wire.client_id;
        out.price         = wire.price;
        out.quantity      = wire.quantity;
        out.instrument_id = wire.instrument_id;
        out.side          = wire.side;
        out.type          = wire.order_type;
        // Padding fields zeroed by default construction
        return true;
    }

    // ── parse_cancel_order() ──────────────────────────────────────────────────
    [[nodiscard]] static bool
    parse_cancel_order(const uint8_t* data, uint16_t len, InboundOrderMsg& out) noexcept
    {
        if (len < sizeof(WireCancelOrder)) return false;

        WireCancelOrder wire{};
        std::memcpy(&wire, data, sizeof(WireCancelOrder));

        out.kind          = InboundOrderMsg::Kind::Cancel;
        out.order_id      = wire.order_id;
        out.instrument_id = wire.instrument_id;
        out.price         = 0;
        out.quantity      = 0;
        out.side          = 0;
        out.type          = 0;
        return true;
    }

    // ── Data members ──────────────────────────────────────────────────────────

    uint64_t decoded_count_ = 0;
    uint64_t error_count_   = 0;
    uint64_t spans_seen_    = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// Encoder — serialise ExecutionReports to WireExecReport for the UringSender
// ─────────────────────────────────────────────────────────────────────────────
class Encoder {
public:

    // Serialise `report` into `buf` (must be at least sizeof(WireExecReport) bytes).
    // Returns number of bytes written.
    //
    static std::size_t
    encode_exec_report(const ExecutionReport& report, uint8_t* buf) noexcept
    {
        WireExecReport wire{};
        wire.msg_type              = static_cast<uint16_t>(WireMsgType::ExecReport);
        wire.msg_len               = static_cast<uint16_t>(sizeof(WireExecReport));
        wire.kind                  = static_cast<uint8_t>(report.kind);
        wire._pad0[0]              = 0;
        wire._pad0[1]              = 0;
        wire._pad0[2]              = 0;
        wire.order_id              = report.order_id;
        wire.counterpart_order_id  = report.counterpart_order_id;
        wire.exec_price            = report.exec_price;
        wire.exec_quantity         = report.exec_quantity;
        wire.leaves_quantity       = report.leaves_quantity;
        wire.timestamp_ns          = report.timestamp_ns;

        std::memcpy(buf, &wire, sizeof(WireExecReport));
        return sizeof(WireExecReport);
    }

    // Decode a WireExecReport byte buffer back into an ExecutionReport.
    // Used by the client-side receiver and for roundtrip tests.
    //
    [[nodiscard]] static bool
    decode_exec_report(const uint8_t* data, std::size_t len,
                       ExecutionReport& out) noexcept
    {
        if (len < sizeof(WireExecReport)) return false;

        WireExecReport wire{};
        std::memcpy(&wire, data, sizeof(WireExecReport));

        if (wire.msg_type != static_cast<uint16_t>(WireMsgType::ExecReport)) return false;

        out.kind                  = static_cast<ExecutionReport::Kind>(wire.kind);
        out.order_id              = wire.order_id;
        out.counterpart_order_id  = wire.counterpart_order_id;
        out.exec_price            = wire.exec_price;
        out.exec_quantity         = wire.exec_quantity;
        out.leaves_quantity       = wire.leaves_quantity;
        out.timestamp_ns          = wire.timestamp_ns;
        return true;
    }
};

} // namespace hft::io

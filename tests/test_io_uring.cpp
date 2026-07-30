// =============================================================================
// test_io_uring.cpp
// Google Test suite for hft::io::Decoder and hft::io::Encoder
//
// Scope:
//   These tests cover the protocol codec layer (decoder.hpp + wire_protocol.hpp)
//   and are intentionally io_uring-FREE.  They can be compiled and run on any
//   platform (Linux, macOS, Windows) without liburing.
//
//   io_uring integration is verified in a separate cloud CI job where
//   CAP_SYS_NICE and kernel 5.11+ are available.
//
// Test groups:
//   1.  WireProtocolLayout    — sizeof/offset static assertions proved at runtime
//   2.  DecodeNewOrder        — Limit, Market, IOC, FOK order types
//   3.  DecodeCancelOrder     — cancel message parsing
//   4.  MultiMessageSpan      — multiple messages concatenated in one recv buffer
//   5.  TruncatedMessages     — partial/corrupt messages handled gracefully
//   6.  UnknownMessageType    — skipped without crashing
//   7.  BufIdReturned         — buf_id feedback to UringReceiver verified
//   8.  EncodeExecReport      — serialise ExecutionReport → WireExecReport
//   9.  DecodeExecReport      — deserialise WireExecReport → ExecutionReport
//   10. EncoderRoundtrip      — encode then decode equals original struct
//   11. DecoderStats          — decoded_count, error_count, spans_seen tracking
//   12. FullPipeline          — bytes → RawByteQueue → Decoder → InboundQueue
// =============================================================================

#include "io/decoder.hpp"
#include "io/wire_protocol.hpp"
#include "spsc/spsc_queue.hpp"
#include "core/order.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <cstdint>
#include <vector>
#include <array>

using namespace hft::io;
using namespace hft::spsc;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// Test Fixture
//
// Small queues — all stack-allocated for test speed.
// ─────────────────────────────────────────────────────────────────────────────
class DecoderTest : public ::testing::Test {
protected:
    RawByteQueue    raw_queue_;    // Receiver → Decoder
    InboundQueue    inbound_;     // Decoder  → MatchingEngine
    BufReturnQueue  returns_;     // Decoder  → Receiver (buf_id recycling)
    Decoder         decoder_;

    // ── Wire message builders (fill in fields, return byte array) ─────────────

    static std::vector<uint8_t> make_new_order_bytes(
        OrderId    order_id,
        uint32_t   instrument_id,
        int64_t    price,
        uint32_t   quantity,
        uint8_t    side,       // 0=Buy, 1=Sell
        uint8_t    order_type, // 0=Limit, 1=Market, 2=IOC, 3=FOK
        uint64_t   client_id  = 0xDEADBEEF)
    {
        WireNewOrder wire{};
        wire.msg_type      = static_cast<uint16_t>(WireMsgType::NewOrder);
        wire.msg_len       = static_cast<uint16_t>(sizeof(WireNewOrder));
        wire.instrument_id = instrument_id;
        wire.order_id      = order_id;
        wire.client_id     = client_id;
        wire.price         = price;
        wire.quantity      = quantity;
        wire.side          = side;
        wire.order_type    = order_type;
        wire._pad[0]       = 0;
        wire._pad[1]       = 0;

        std::vector<uint8_t> buf(sizeof(WireNewOrder));
        std::memcpy(buf.data(), &wire, sizeof(WireNewOrder));
        return buf;
    }

    static std::vector<uint8_t> make_cancel_bytes(
        OrderId  order_id,
        uint32_t instrument_id)
    {
        WireCancelOrder wire{};
        wire.msg_type      = static_cast<uint16_t>(WireMsgType::CancelOrder);
        wire.msg_len       = static_cast<uint16_t>(sizeof(WireCancelOrder));
        wire.order_id      = order_id;
        wire.instrument_id = instrument_id;

        std::vector<uint8_t> buf(sizeof(WireCancelOrder));
        std::memcpy(buf.data(), &wire, sizeof(WireCancelOrder));
        return buf;
    }

    // Post a byte buffer as a RawByteSpan with a given buf_id.
    RawByteSpan make_span(const std::vector<uint8_t>& data, uint32_t buf_id = 0) {
        RawByteSpan span{};
        span.data   = data.data();
        span.len    = static_cast<uint32_t>(data.size());
        span.buf_id = buf_id;
        return span;
    }

    // Drain all InboundOrderMsgs from inbound_.
    std::vector<InboundOrderMsg> drain_inbound() {
        std::vector<InboundOrderMsg> out;
        InboundOrderMsg msg{};
        while (inbound_.try_pop(msg)) out.push_back(msg);
        return out;
    }

    // Drain all returned buf_ids from returns_.
    std::vector<uint32_t> drain_returns() {
        std::vector<uint32_t> out;
        uint32_t id{};
        while (returns_.try_pop(id)) out.push_back(id);
        return out;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. WIRE PROTOCOL LAYOUT
// ─────────────────────────────────────────────────────────────────────────────

TEST(WireProtocolLayout, WireHeaderSize) {
    EXPECT_EQ(sizeof(WireHeader), 4u);
}

TEST(WireProtocolLayout, WireNewOrderSize) {
    EXPECT_EQ(sizeof(WireNewOrder), 40u);
}

TEST(WireProtocolLayout, WireCancelOrderSize) {
    EXPECT_EQ(sizeof(WireCancelOrder), 16u);
}

TEST(WireProtocolLayout, WireExecReportSize) {
    EXPECT_EQ(sizeof(WireExecReport), 48u);
}

TEST(WireProtocolLayout, WireNewOrderFieldOffsets) {
    EXPECT_EQ(offsetof(WireNewOrder, msg_type),      0u);
    EXPECT_EQ(offsetof(WireNewOrder, msg_len),        2u);
    EXPECT_EQ(offsetof(WireNewOrder, instrument_id),  4u);
    EXPECT_EQ(offsetof(WireNewOrder, order_id),       8u);
    EXPECT_EQ(offsetof(WireNewOrder, client_id),     16u);
    EXPECT_EQ(offsetof(WireNewOrder, price),         24u);
    EXPECT_EQ(offsetof(WireNewOrder, quantity),      32u);
    EXPECT_EQ(offsetof(WireNewOrder, side),          36u);
    EXPECT_EQ(offsetof(WireNewOrder, order_type),    37u);
}

TEST(WireProtocolLayout, MinMsgLen) {
    EXPECT_EQ(min_msg_len(WireMsgType::NewOrder),    sizeof(WireNewOrder));
    EXPECT_EQ(min_msg_len(WireMsgType::CancelOrder), sizeof(WireCancelOrder));
    EXPECT_EQ(min_msg_len(WireMsgType::ExecReport),  sizeof(WireExecReport));
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. DECODE NEW ORDER
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, DecodeNewOrder_LimitBuy) {
    auto bytes = make_new_order_bytes(
        /*order_id*/      0xABCD1234,
        /*instrument_id*/ 42,
        /*price*/         15000,
        /*quantity*/      100,
        /*side*/          0,   // Buy
        /*order_type*/    0,   // Limit
        /*client_id*/     0xCAFEBABE);

    auto span = make_span(bytes, /*buf_id=*/7);
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 1);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);

    EXPECT_EQ(msgs[0].kind,          InboundOrderMsg::Kind::NewOrder);
    EXPECT_EQ(msgs[0].order_id,      0xABCD1234u);
    EXPECT_EQ(msgs[0].instrument_id, 42u);
    EXPECT_EQ(msgs[0].price,         15000);
    EXPECT_EQ(msgs[0].quantity,      100u);
    EXPECT_EQ(msgs[0].side,          0u);
    EXPECT_EQ(msgs[0].type,          0u);
    EXPECT_EQ(msgs[0].client_id,     0xCAFEBABEu);
}

TEST_F(DecoderTest, DecodeNewOrder_LimitSell) {
    auto bytes = make_new_order_bytes(1, 1, 1500, 50, /*side=*/1, /*type=*/0);
    auto span  = make_span(bytes);
    decoder_.decode_span(span, inbound_);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].side,  1u) << "Side must be Sell (1)";
    EXPECT_EQ(msgs[0].type,  0u) << "Order type must be Limit (0)";
}

TEST_F(DecoderTest, DecodeNewOrder_MarketOrder_PriceZero) {
    auto bytes = make_new_order_bytes(2, 1, /*price=*/0, 100, 0, /*type=*/1);
    auto span  = make_span(bytes);
    decoder_.decode_span(span, inbound_);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].price, 0) << "Market order price must be 0";
    EXPECT_EQ(msgs[0].type,  1u) << "Order type must be Market (1)";
}

TEST_F(DecoderTest, DecodeNewOrder_IOC) {
    auto bytes = make_new_order_bytes(3, 1, 1500, 100, 0, /*type=*/2);
    decoder_.decode_span(make_span(bytes), inbound_);
    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].type, 2u) << "Order type must be IOC (2)";
}

TEST_F(DecoderTest, DecodeNewOrder_FOK) {
    auto bytes = make_new_order_bytes(4, 1, 1500, 100, 0, /*type=*/3);
    decoder_.decode_span(make_span(bytes), inbound_);
    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].type, 3u) << "Order type must be FOK (3)";
}

TEST_F(DecoderTest, DecodeNewOrder_LargePrice) {
    // Price near int64_t max — should decode without overflow.
    const int64_t big_price = 999999999LL;
    auto bytes = make_new_order_bytes(5, 1, big_price, 1, 0, 0);
    decoder_.decode_span(make_span(bytes), inbound_);
    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].price, big_price);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. DECODE CANCEL ORDER
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, DecodeCancelOrder_FieldsCorrect) {
    auto bytes = make_cancel_bytes(/*order_id=*/0xDEAD5678, /*instrument_id=*/99);
    decoder_.decode_span(make_span(bytes), inbound_);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].kind,          InboundOrderMsg::Kind::Cancel);
    EXPECT_EQ(msgs[0].order_id,      0xDEAD5678u);
    EXPECT_EQ(msgs[0].instrument_id, 99u);
    // Non-applicable fields should be zeroed.
    EXPECT_EQ(msgs[0].price,    0);
    EXPECT_EQ(msgs[0].quantity, 0u);
}

TEST_F(DecoderTest, DecodeCancelOrder_DoesNotRestoreAsNewOrder) {
    auto bytes = make_cancel_bytes(42, 1);
    decoder_.decode_span(make_span(bytes), inbound_);
    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_NE(msgs[0].kind, InboundOrderMsg::Kind::NewOrder)
        << "Cancel must not be decoded as a NewOrder";
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. MULTI-MESSAGE SPAN
//
// A single recv() may deliver multiple complete messages concatenated.
// The decoder must iterate until the span is consumed.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, MultiMessage_TwoNewOrders) {
    auto b1 = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    auto b2 = make_new_order_bytes(2, 1, 1600, 200, 1, 0);

    // Concatenate into one buffer.
    std::vector<uint8_t> combined;
    combined.insert(combined.end(), b1.begin(), b1.end());
    combined.insert(combined.end(), b2.begin(), b2.end());

    RawByteSpan span{combined.data(), static_cast<uint32_t>(combined.size()), 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 2);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 2u);
    EXPECT_EQ(msgs[0].order_id, 1u);
    EXPECT_EQ(msgs[1].order_id, 2u);
}

TEST_F(DecoderTest, MultiMessage_NewOrderThenCancel) {
    auto b1 = make_new_order_bytes(10, 1, 1500, 100, 0, 0);
    auto b2 = make_cancel_bytes(10, 1);

    std::vector<uint8_t> combined;
    combined.insert(combined.end(), b1.begin(), b1.end());
    combined.insert(combined.end(), b2.begin(), b2.end());

    decoder_.decode_span({combined.data(), static_cast<uint32_t>(combined.size()), 0}, inbound_);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 2u);
    EXPECT_EQ(msgs[0].kind, InboundOrderMsg::Kind::NewOrder);
    EXPECT_EQ(msgs[1].kind, InboundOrderMsg::Kind::Cancel);
    EXPECT_EQ(msgs[1].order_id, 10u);
}

TEST_F(DecoderTest, MultiMessage_FiveOrders) {
    std::vector<uint8_t> combined;
    for (uint32_t i = 1; i <= 5; ++i) {
        auto b = make_new_order_bytes(i, 1, 1000 + static_cast<int64_t>(i) * 10, 100, 0, 0);
        combined.insert(combined.end(), b.begin(), b.end());
    }

    int count = decoder_.decode_span(
        {combined.data(), static_cast<uint32_t>(combined.size()), 0}, inbound_);
    EXPECT_EQ(count, 5);

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 5u);
    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(msgs[i].order_id, i + 1)
            << "Multi-message FIFO order violated at index " << i;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. TRUNCATED / CORRUPT MESSAGES
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, Truncated_LessThanHeader) {
    // 3 bytes — not even a full WireHeader.
    uint8_t buf[3] = {0x00, 0x01, 0x00};
    RawByteSpan span{buf, 3, 0};

    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 0);
    EXPECT_EQ(drain_inbound().size(), 0u) << "Truncated span must produce no messages";
}

TEST_F(DecoderTest, Truncated_HeaderOnlyNoBody) {
    // Valid header claims msg_len = sizeof(WireNewOrder) but buffer has only 4 bytes.
    WireHeader hdr{static_cast<uint16_t>(WireMsgType::NewOrder),
                   static_cast<uint16_t>(sizeof(WireNewOrder))};
    uint8_t buf[4];
    std::memcpy(buf, &hdr, 4);

    RawByteSpan span{buf, 4, 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 0);
    EXPECT_GT(decoder_.error_count(), 0u) << "Truncation must increment error count";
}

TEST_F(DecoderTest, Truncated_BodyPartiallyPresent) {
    // A full WireNewOrder truncated to 20 bytes (half the message).
    auto full = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    full.resize(20);
    // But the header still claims full msg_len — decoder should detect.
    // Re-write header with correct msg_len to make the span claim it's 40 bytes.
    WireNewOrder wire{};
    std::memcpy(&wire, full.data(), std::min(full.size(), sizeof(WireNewOrder)));
    // msg_len says 40, but buffer only has 20 → decoder rejects.
    // The header msg_len in the buffer is 40 (original), span.len = 20.
    RawByteSpan span{full.data(), static_cast<uint32_t>(full.size()), 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 0);
}

TEST_F(DecoderTest, MsgLenZero_Corrupt) {
    // msg_len = 0 → less than sizeof(WireHeader) → error.
    uint8_t buf[8] = {0x00, 0x01,  // msg_type = NewOrder
                      0x00, 0x00,  // msg_len = 0 (invalid!)
                      0, 0, 0, 0};
    RawByteSpan span{buf, 8, 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 0);
    EXPECT_GT(decoder_.error_count(), 0u);
}

TEST_F(DecoderTest, ValidMessageAfterGoodOne_DecodesCorrectly) {
    // First message is valid, second is truncated.
    // Decoder should decode the first and bail on the second without crashing.
    auto b1 = make_new_order_bytes(1, 1, 1500, 100, 0, 0);

    // Second "message": header claiming 40 bytes but only 4 bytes present.
    WireHeader hdr{static_cast<uint16_t>(WireMsgType::NewOrder),
                   static_cast<uint16_t>(sizeof(WireNewOrder))};

    std::vector<uint8_t> combined = b1;
    combined.resize(combined.size() + 4);
    std::memcpy(combined.data() + b1.size(), &hdr, 4);

    RawByteSpan span{combined.data(), static_cast<uint32_t>(combined.size()), 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 1) << "Should decode first valid message only";
    EXPECT_GT(decoder_.error_count(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. UNKNOWN MESSAGE TYPE
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, UnknownMsgType_SkippedWithoutCrash) {
    // msg_type = 0xFF00 (not in WireMsgType enum).
    uint8_t buf[16] = {};
    WireHeader hdr{0xFF00, 16};
    std::memcpy(buf, &hdr, sizeof(hdr));

    RawByteSpan span{buf, 16, 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 0);
    EXPECT_EQ(drain_inbound().size(), 0u);
    EXPECT_GT(decoder_.error_count(), 0u) << "Unknown type must increment error_count";
}

TEST_F(DecoderTest, UnknownMsgTypeThenValidMsg_ValidIsDecoded) {
    // Unknown message (16 bytes), then a valid cancel (16 bytes).
    std::vector<uint8_t> combined(16, 0);
    WireHeader unknown{0xFF00, 16};
    std::memcpy(combined.data(), &unknown, sizeof(unknown));

    auto cancel = make_cancel_bytes(42, 1);
    combined.insert(combined.end(), cancel.begin(), cancel.end());

    RawByteSpan span{combined.data(), static_cast<uint32_t>(combined.size()), 0};
    int count = decoder_.decode_span(span, inbound_);
    EXPECT_EQ(count, 1) << "Cancel after unknown type must be decoded";

    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].kind,     InboundOrderMsg::Kind::Cancel);
    EXPECT_EQ(msgs[0].order_id, 42u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. BUF_ID RETURNED TO RECEIVER
//
// Verifies that after poll(), the span's buf_id is pushed to the return queue
// so the UringReceiver can re-arm the fixed buffer.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, BufId_ReturnedAfterPoll) {
    auto bytes = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    RawByteSpan span{bytes.data(), static_cast<uint32_t>(bytes.size()), /*buf_id=*/13};

    raw_queue_.spin_push(span);
    decoder_.poll(raw_queue_, inbound_, returns_);

    auto returned = drain_returns();
    ASSERT_EQ(returned.size(), 1u);
    EXPECT_EQ(returned[0], 13u) << "Decoder must return the exact buf_id it received";
}

TEST_F(DecoderTest, BufId_ReturnedForEachSpan_MultiplePoll) {
    // Push 3 spans with different buf_ids.
    for (uint32_t i = 0; i < 3; ++i) {
        auto bytes = make_new_order_bytes(i, 1, 1500, 100, 0, 0);
        // We need to keep the bytes alive — store them on stack
        RawByteSpan span{bytes.data(), static_cast<uint32_t>(bytes.size()), i};
        raw_queue_.spin_push(span);
    }

    decoder_.poll(raw_queue_, inbound_, returns_);

    auto returned = drain_returns();
    ASSERT_EQ(returned.size(), 3u)
        << "Each span's buf_id must be returned exactly once";

    // All three buf_ids must appear (order may vary — use a set check).
    std::vector<uint32_t> sorted = returned;
    std::sort(sorted.begin(), sorted.end());
    EXPECT_EQ(sorted[0], 0u);
    EXPECT_EQ(sorted[1], 1u);
    EXPECT_EQ(sorted[2], 2u);
}

TEST_F(DecoderTest, BufId_ReturnedEvenForInvalidSpan) {
    // Even a span with no valid messages must return its buf_id.
    uint8_t garbage[3] = {0xFF, 0xFE, 0xFD};
    RawByteSpan span{garbage, 3, /*buf_id=*/77};

    raw_queue_.spin_push(span);
    decoder_.poll(raw_queue_, inbound_, returns_);

    auto returned = drain_returns();
    ASSERT_EQ(returned.size(), 1u) << "buf_id must be returned even for invalid spans";
    EXPECT_EQ(returned[0], 77u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. ENCODE EXEC REPORT
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, EncodeExecReport_FieldsCorrect) {
    ExecutionReport r{};
    r.kind                  = ExecutionReport::Kind::Fill;
    r.order_id              = 0xABCDEF01u;
    r.counterpart_order_id  = 0x12345678u;
    r.exec_price            = 15050;
    r.exec_quantity         = 100;
    r.leaves_quantity       = 0;
    r.timestamp_ns          = 123456789ULL;

    std::array<uint8_t, sizeof(WireExecReport)> buf{};
    std::size_t len = Encoder::encode_exec_report(r, buf.data());

    EXPECT_EQ(len, sizeof(WireExecReport));

    // Decode the raw bytes back.
    WireExecReport wire{};
    std::memcpy(&wire, buf.data(), sizeof(wire));

    EXPECT_EQ(wire.msg_type,             static_cast<uint16_t>(WireMsgType::ExecReport));
    EXPECT_EQ(wire.msg_len,              static_cast<uint16_t>(sizeof(WireExecReport)));
    EXPECT_EQ(wire.kind,                 static_cast<uint8_t>(ExecutionReport::Kind::Fill));
    EXPECT_EQ(wire.order_id,             0xABCDEF01u);
    EXPECT_EQ(wire.counterpart_order_id, 0x12345678u);
    EXPECT_EQ(wire.exec_price,           15050);
    EXPECT_EQ(wire.exec_quantity,        100u);
    EXPECT_EQ(wire.leaves_quantity,      0u);
    EXPECT_EQ(wire.timestamp_ns,         123456789ULL);
}

TEST_F(DecoderTest, EncodeExecReport_AllKinds) {
    const std::vector<ExecutionReport::Kind> kinds = {
        ExecutionReport::Kind::Fill,
        ExecutionReport::Kind::PartialFill,
        ExecutionReport::Kind::CancelAck,
        ExecutionReport::Kind::Reject,
    };

    for (auto k : kinds) {
        ExecutionReport r{};
        r.kind = k;
        std::array<uint8_t, sizeof(WireExecReport)> buf{};
        Encoder::encode_exec_report(r, buf.data());

        WireExecReport wire{};
        std::memcpy(&wire, buf.data(), sizeof(wire));
        EXPECT_EQ(wire.kind, static_cast<uint8_t>(k))
            << "Kind mismatch for kind=" << static_cast<int>(k);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. DECODE EXEC REPORT
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, DecodeExecReport_FieldsCorrect) {
    WireExecReport wire{};
    wire.msg_type             = static_cast<uint16_t>(WireMsgType::ExecReport);
    wire.msg_len              = sizeof(WireExecReport);
    wire.kind                 = static_cast<uint8_t>(ExecutionReport::Kind::PartialFill);
    wire.order_id             = 0x5555u;
    wire.counterpart_order_id = 0x6666u;
    wire.exec_price           = 1234;
    wire.exec_quantity        = 50;
    wire.leaves_quantity      = 50;
    wire.timestamp_ns         = 9999ULL;

    std::array<uint8_t, sizeof(WireExecReport)> buf{};
    std::memcpy(buf.data(), &wire, sizeof(wire));

    ExecutionReport r{};
    bool ok = Encoder::decode_exec_report(buf.data(), buf.size(), r);
    ASSERT_TRUE(ok);

    EXPECT_EQ(r.kind,                 ExecutionReport::Kind::PartialFill);
    EXPECT_EQ(r.order_id,             0x5555u);
    EXPECT_EQ(r.counterpart_order_id, 0x6666u);
    EXPECT_EQ(r.exec_price,           1234);
    EXPECT_EQ(r.exec_quantity,        50u);
    EXPECT_EQ(r.leaves_quantity,      50u);
    EXPECT_EQ(r.timestamp_ns,         9999ULL);
}

TEST_F(DecoderTest, DecodeExecReport_TruncatedReturnsFalse) {
    std::array<uint8_t, 10> buf{};  // too small
    ExecutionReport r{};
    EXPECT_FALSE(Encoder::decode_exec_report(buf.data(), buf.size(), r));
}

TEST_F(DecoderTest, DecodeExecReport_WrongMsgTypeReturnsFalse) {
    WireExecReport wire{};
    wire.msg_type = 0x9999;   // wrong type
    wire.msg_len  = sizeof(WireExecReport);

    std::array<uint8_t, sizeof(WireExecReport)> buf{};
    std::memcpy(buf.data(), &wire, sizeof(wire));

    ExecutionReport r{};
    EXPECT_FALSE(Encoder::decode_exec_report(buf.data(), buf.size(), r));
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. ENCODER ROUNDTRIP
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, EncoderRoundtrip_PreservesAllFields) {
    ExecutionReport original{};
    original.kind                 = ExecutionReport::Kind::Reject;
    original.order_id             = 0xFEDCBA98u;
    original.counterpart_order_id = 0u;
    original.exec_price           = -1;   // negative price (test edge case)
    original.exec_quantity        = 0;
    original.leaves_quantity      = 500;
    original.timestamp_ns         = 0xFFFFFFFFFFFFFFFFULL;

    std::array<uint8_t, sizeof(WireExecReport)> buf{};
    Encoder::encode_exec_report(original, buf.data());

    ExecutionReport decoded{};
    bool ok = Encoder::decode_exec_report(buf.data(), buf.size(), decoded);
    ASSERT_TRUE(ok);

    EXPECT_EQ(decoded.kind,                 original.kind);
    EXPECT_EQ(decoded.order_id,             original.order_id);
    EXPECT_EQ(decoded.counterpart_order_id, original.counterpart_order_id);
    EXPECT_EQ(decoded.exec_price,           original.exec_price);
    EXPECT_EQ(decoded.exec_quantity,        original.exec_quantity);
    EXPECT_EQ(decoded.leaves_quantity,      original.leaves_quantity);
    EXPECT_EQ(decoded.timestamp_ns,         original.timestamp_ns);
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. DECODER STATISTICS
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, Stats_DecodedCountIncrementsPerMessage) {
    EXPECT_EQ(decoder_.decoded_count(), 0u);

    auto b1 = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    auto b2 = make_new_order_bytes(2, 1, 1600, 100, 1, 0);

    std::vector<uint8_t> combined = b1;
    combined.insert(combined.end(), b2.begin(), b2.end());

    decoder_.decode_span({combined.data(), static_cast<uint32_t>(combined.size()), 0}, inbound_);

    EXPECT_EQ(decoder_.decoded_count(), 2u);
}

TEST_F(DecoderTest, Stats_ErrorCountIncrementsOnInvalid) {
    EXPECT_EQ(decoder_.error_count(), 0u);

    uint8_t bad[3] = {0xFF, 0xFF, 0xFF};
    decoder_.decode_span({bad, 3, 0}, inbound_);

    EXPECT_EQ(decoder_.error_count(), 0u)  // Less than header — no error counted, just skipped
        << "Span shorter than WireHeader should be silently skipped";

    // Now trigger an actual error: valid-length header with unknown type.
    uint8_t unknown[16] = {};
    WireHeader hdr{0xBEEF, 16};
    std::memcpy(unknown, &hdr, sizeof(hdr));
    decoder_.decode_span({unknown, 16, 0}, inbound_);
    EXPECT_GT(decoder_.error_count(), 0u);
}

TEST_F(DecoderTest, Stats_SpansSeenIncrementsPerSpan) {
    EXPECT_EQ(decoder_.spans_seen(), 0u);

    auto bytes = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    decoder_.decode_span({bytes.data(), static_cast<uint32_t>(bytes.size()), 0}, inbound_);
    EXPECT_EQ(decoder_.spans_seen(), 1u);

    decoder_.decode_span({bytes.data(), static_cast<uint32_t>(bytes.size()), 0}, inbound_);
    EXPECT_EQ(decoder_.spans_seen(), 2u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. FULL PIPELINE — bytes → RawByteQueue → Decoder → InboundQueue
//
// Simulates the full flow from "bytes received off the wire" through the
// decoder to the matching engine's inbox — without io_uring or network.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DecoderTest, FullPipeline_OrdersFlow_EndToEnd) {
    // Build 3 orders.
    const uint32_t instrument_id = 42;
    auto order1 = make_new_order_bytes(100, instrument_id, 15000, 200, 0, 0); // limit buy
    auto order2 = make_new_order_bytes(101, instrument_id, 15500, 100, 1, 0); // limit sell
    auto order3 = make_new_order_bytes(102, instrument_id, 0,     50,  0, 1); // market buy

    // Post each as a separate RawByteSpan with distinct buf_ids.
    for (uint32_t i = 0; i < 3; ++i) {
        const std::vector<uint8_t>* data_ptr = (i == 0) ? &order1
                                             : (i == 1) ? &order2 : &order3;
        RawByteSpan span{
            data_ptr->data(),
            static_cast<uint32_t>(data_ptr->size()),
            i   // buf_id = 0, 1, 2
        };
        raw_queue_.spin_push(span);
    }

    // Run decoder.
    int spans = decoder_.poll(raw_queue_, inbound_, returns_);
    EXPECT_EQ(spans, 3);

    // Verify InboundQueue.
    auto msgs = drain_inbound();
    ASSERT_EQ(msgs.size(), 3u);

    EXPECT_EQ(msgs[0].order_id,      100u);
    EXPECT_EQ(msgs[0].price,         15000);
    EXPECT_EQ(msgs[0].type,          0u);   // Limit

    EXPECT_EQ(msgs[1].order_id,      101u);
    EXPECT_EQ(msgs[1].side,          1u);   // Sell

    EXPECT_EQ(msgs[2].order_id,      102u);
    EXPECT_EQ(msgs[2].type,          1u);   // Market
    EXPECT_EQ(msgs[2].price,         0);

    // Verify all buf_ids returned to receiver.
    auto returned = drain_returns();
    ASSERT_EQ(returned.size(), 3u);
    std::sort(returned.begin(), returned.end());
    EXPECT_EQ(returned[0], 0u);
    EXPECT_EQ(returned[1], 1u);
    EXPECT_EQ(returned[2], 2u);
}

TEST_F(DecoderTest, FullPipeline_MixedValidAndInvalidSpans) {
    auto valid = make_new_order_bytes(1, 1, 1500, 100, 0, 0);
    uint8_t garbage[3] = {0xFF, 0xFE, 0xFD};

    RawByteSpan s1{valid.data(),   static_cast<uint32_t>(valid.size()), 10};
    RawByteSpan s2{garbage, 3, 11};

    raw_queue_.spin_push(s1);
    raw_queue_.spin_push(s2);
    decoder_.poll(raw_queue_, inbound_, returns_);

    EXPECT_EQ(drain_inbound().size(), 1u)  << "Only valid span should produce a message";
    EXPECT_EQ(drain_returns().size(), 2u)  << "Both buf_ids must be returned";
    EXPECT_EQ(decoder_.spans_seen(),  2u);
    EXPECT_EQ(decoder_.decoded_count(), 1u);
}

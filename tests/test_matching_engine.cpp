// =============================================================================
// test_matching_engine.cpp
// Google Test suite for hft::matching::MatchingEngine
//
// Test groups:
//   1. PriceTimePriority — FIFO execution at the same price level
//   2. FullFill          — single resting order exactly filled
//   3. PartialFill       — resting partial, aggressive full; and vice versa
//   4. MultiLevelSweep   — aggressive order sweeps multiple price levels
//   5. FOK               — reject on insufficient liquidity; execute on sufficiency
//   6. IOC               — partial match then CancelAck for residual
//   7. MarketOrder       — sweeps regardless of price; discards residual
//   8. Cancel            — cancel resting order; verify book and report
//   9. NoMatch           — order rests when no crossable opposite side
//   10. MixedWorkload    — interleaved adds, fills, cancels
// =============================================================================

#include "matching/matching_engine.hpp"

#include <gtest/gtest.h>

#include <vector>
#include <cstdint>

using namespace hft::matching;
using namespace hft::core;
using namespace hft::spsc;

// ─────────────────────────────────────────────────────────────────────────────
// Test Fixture
//
// Instrument: id=1, prices 1000–2000 (ticks), tick_size=1.
// All helpers use submit() to bypass the SPSC queue for determinism.
// ─────────────────────────────────────────────────────────────────────────────
class METest : public ::testing::Test {
protected:
    static constexpr InstrumentId kInst     = 1;
    static constexpr Price        kMinPrice = 1000;
    static constexpr Price        kMaxPrice = 2000;

    // Stack-allocate the queues — they are large (65 KB each) but the test
    // process has a multi-MB stack on Linux.
    InboundQueue  inbound_;
    OutboundQueue outbound_;
    MatchingEngine engine_{inbound_, outbound_};

    void SetUp() override {
        engine_.add_instrument(kInst, kMinPrice, kMaxPrice, /*tick_size=*/1);
    }

    // ── Message builders ──────────────────────────────────────────────────────

    static InboundOrderMsg make_new_order(
        OrderId id, Price price, Quantity qty, Side side, OrderType type)
    {
        InboundOrderMsg msg{};
        msg.kind          = InboundOrderMsg::Kind::NewOrder;
        msg.order_id      = id;
        msg.price         = price;
        msg.quantity      = qty;
        msg.instrument_id = kInst;
        msg.side          = static_cast<uint8_t>(side);
        msg.type          = static_cast<uint8_t>(type);
        return msg;
    }

    // Submit a resting bid (limit buy at exactly this price).
    void post_bid(OrderId id, Price price, Quantity qty = 100) {
        engine_.submit(make_new_order(id, price, qty, Side::Buy, OrderType::Limit));
    }

    // Submit a resting ask (limit sell at exactly this price).
    void post_ask(OrderId id, Price price, Quantity qty = 100) {
        engine_.submit(make_new_order(id, price, qty, Side::Sell, OrderType::Limit));
    }

    // Submit an aggressive limit buy (price high enough to cross).
    void post_limit_buy(OrderId id, Price price, Quantity qty) {
        engine_.submit(make_new_order(id, price, qty, Side::Buy, OrderType::Limit));
    }

    // Submit an aggressive limit sell (price low enough to cross).
    void post_limit_sell(OrderId id, Price price, Quantity qty) {
        engine_.submit(make_new_order(id, price, qty, Side::Sell, OrderType::Limit));
    }

    void post_market_buy(OrderId id, Quantity qty) {
        // Market orders use price = 0 (ignored by matching logic).
        engine_.submit(make_new_order(id, 0, qty, Side::Buy, OrderType::Market));
    }

    void post_market_sell(OrderId id, Quantity qty) {
        engine_.submit(make_new_order(id, 0, qty, Side::Sell, OrderType::Market));
    }

    void post_ioc_buy(OrderId id, Price price, Quantity qty) {
        engine_.submit(make_new_order(id, price, qty, Side::Buy, OrderType::IOC));
    }

    void post_fok_buy(OrderId id, Price price, Quantity qty) {
        engine_.submit(make_new_order(id, price, qty, Side::Buy, OrderType::FOK));
    }

    void post_cancel(OrderId id) {
        InboundOrderMsg msg{};
        msg.kind          = InboundOrderMsg::Kind::Cancel;
        msg.order_id      = id;
        msg.instrument_id = kInst;
        engine_.submit(msg);
    }

    // ── Report drain ──────────────────────────────────────────────────────────

    std::vector<ExecutionReport> drain_reports() {
        std::vector<ExecutionReport> out;
        ExecutionReport r{};
        while (outbound_.try_pop(r)) {
            out.push_back(r);
        }
        return out;
    }

    // Drain and find the first report for a specific order_id.
    std::vector<ExecutionReport> reports_for(OrderId id) {
        auto all = drain_reports();
        std::vector<ExecutionReport> filtered;
        for (auto& r : all) {
            if (r.order_id == id) filtered.push_back(r);
        }
        return filtered;
    }

    // ── Book introspection helpers ────────────────────────────────────────────

    hft::lob::OrderBook* book() {
        return engine_.book_for(kInst);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. PRICE-TIME PRIORITY (FIFO)
//
// Three resting asks at the same price; an aggressive bid sweeps them in order.
// Verifies that order 1 is filled before 2, and 2 before 3.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, PriceTimePriority_FIFO_ExecutionOrder) {
    // Resting asks at 1500, each 50 units, submitted in order 1→2→3.
    post_ask(1, 1500, 50);
    post_ask(2, 1500, 50);
    post_ask(3, 1500, 50);

    // Aggressive bid that crosses and fills all three.
    post_limit_buy(10, 1500, 150);

    auto reports = drain_reports();
    // Expect 6 reports: for each of 3 fills → (resting report, agg report).
    ASSERT_EQ(reports.size(), 6u) << "Expected 3 resting fills + 3 agg partials/fill";

    // Reports come in pairs: [resting, aggressive] per fill.
    // First fill must be against order 1 (earliest arrival = head of queue).
    EXPECT_EQ(reports[0].order_id, 1u)  << "First fill must be against order 1 (oldest)";
    EXPECT_EQ(reports[0].kind, ExKind::Fill);
    EXPECT_EQ(reports[0].exec_quantity, 50u);

    EXPECT_EQ(reports[1].order_id, 10u) << "Aggressive order report after first fill";
    EXPECT_EQ(reports[1].kind, ExKind::PartialFill);
    EXPECT_EQ(reports[1].leaves_quantity, 100u);

    // Second fill: order 2
    EXPECT_EQ(reports[2].order_id, 2u);
    EXPECT_EQ(reports[2].kind, ExKind::Fill);

    // Third fill: order 3 — aggressive is now fully filled
    EXPECT_EQ(reports[4].order_id, 3u);
    EXPECT_EQ(reports[4].kind, ExKind::Fill);
    EXPECT_EQ(reports[5].order_id, 10u);
    EXPECT_EQ(reports[5].kind, ExKind::Fill);
    EXPECT_EQ(reports[5].leaves_quantity, 0u);
}

TEST_F(METest, PriceTimePriority_TwoPriceLevels_BestFirst) {
    // Two ask levels: 1500 and 1510. Aggressive bid at 1510 should fill 1500 first.
    post_ask(1, 1500, 100);
    post_ask(2, 1510, 100);

    post_limit_buy(10, 1510, 200);   // crosses both levels

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 4u);

    // First fill is at the better price (1500), not 1510.
    EXPECT_EQ(reports[0].order_id,     1u);
    EXPECT_EQ(reports[0].exec_price, 1500);
    EXPECT_EQ(reports[2].order_id,     2u);
    EXPECT_EQ(reports[2].exec_price, 1510);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. FULL FILL — exact quantity match
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, FullFill_SingleRestingOrder) {
    post_ask(1, 1500, 100);
    post_limit_buy(10, 1510, 100);   // exact match

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 2u);

    // Resting ask report
    EXPECT_EQ(reports[0].order_id,      1u);
    EXPECT_EQ(reports[0].kind,          ExKind::Fill);
    EXPECT_EQ(reports[0].exec_price,    1500);
    EXPECT_EQ(reports[0].exec_quantity, 100u);
    EXPECT_EQ(reports[0].leaves_quantity, 0u);

    // Aggressive bid report
    EXPECT_EQ(reports[1].order_id,      10u);
    EXPECT_EQ(reports[1].kind,          ExKind::Fill);
    EXPECT_EQ(reports[1].exec_price,    1500);
    EXPECT_EQ(reports[1].exec_quantity, 100u);
    EXPECT_EQ(reports[1].leaves_quantity, 0u);

    // Book must be empty
    EXPECT_FALSE(book()->has_bids());
    EXPECT_FALSE(book()->has_asks());
    EXPECT_EQ(book()->live_order_count(), 0u);
}

TEST_F(METest, FullFill_ExecutionPriceIsMakersPrice) {
    // Resting ask at 1500; aggressive bid at 1600 (crosses with room to spare).
    // Execution price must be 1500 (maker's price), not 1600.
    post_ask(1, 1500, 50);
    post_limit_buy(10, 1600, 50);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 2u);
    EXPECT_EQ(reports[0].exec_price, 1500) << "Execution must occur at maker's price";
    EXPECT_EQ(reports[1].exec_price, 1500) << "Both parties report the same exec price";
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. PARTIAL FILLS
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, PartialFill_RestingPartial_AggFull) {
    // Resting ask of 200; aggressive bid of 100.
    // Resting: PartialFill (100 filled, 100 remains in book).
    // Aggressive: Fill (100 filled, 0 remains — done).
    post_ask(1, 1500, 200);
    post_limit_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 2u);

    // Resting order: partial fill, 100 remain
    EXPECT_EQ(reports[0].order_id,        1u);
    EXPECT_EQ(reports[0].kind,            ExKind::PartialFill);
    EXPECT_EQ(reports[0].exec_quantity,   100u);
    EXPECT_EQ(reports[0].leaves_quantity, 100u) << "Resting order still has 100 qty";

    // Aggressive: fully filled
    EXPECT_EQ(reports[1].order_id,        10u);
    EXPECT_EQ(reports[1].kind,            ExKind::Fill);
    EXPECT_EQ(reports[1].leaves_quantity,  0u);

    // Resting order must still be in the book with qty=100
    EXPECT_TRUE(book()->has_asks());
    EXPECT_EQ(book()->best_ask_price(),   1500);
    EXPECT_EQ(book()->ask_level_at(1500).total_qty, 100u);
    EXPECT_EQ(book()->live_order_count(), 1u);
}

TEST_F(METest, PartialFill_AggPartial_RestsFull_ThenRests) {
    // Resting ask of 50; aggressive bid of 100.
    // Resting: Fill (50/50).
    // Aggressive: PartialFill (50 filled), then rests remaining 50 in bid book.
    post_ask(1, 1500, 50);
    post_limit_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 2u);

    EXPECT_EQ(reports[0].order_id, 1u);
    EXPECT_EQ(reports[0].kind, ExKind::Fill);

    EXPECT_EQ(reports[1].order_id,        10u);
    EXPECT_EQ(reports[1].kind,            ExKind::PartialFill);
    EXPECT_EQ(reports[1].exec_quantity,    50u);
    EXPECT_EQ(reports[1].leaves_quantity,  50u);

    // Aggressive bid must now be resting in the bid book with qty=50
    EXPECT_TRUE(book()->has_bids());
    EXPECT_EQ(book()->best_bid_price(), 1500);
    EXPECT_EQ(book()->bid_level_at(1500).total_qty, 50u);
    EXPECT_EQ(book()->live_order_count(), 1u);
}

TEST_F(METest, PartialFill_TwoRestingOrders_OnePartial) {
    // Resting asks: 1@60, 2@60. Aggressive bid of 80.
    // Order 1 fills fully (60). Order 2 fills partially (20 of 60).
    post_ask(1, 1500, 60);
    post_ask(2, 1500, 60);
    post_limit_buy(10, 1500, 80);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 4u);

    // Fill of order 1 (full)
    EXPECT_EQ(reports[0].order_id, 1u);
    EXPECT_EQ(reports[0].kind, ExKind::Fill);
    EXPECT_EQ(reports[0].exec_quantity, 60u);

    // Agg after first fill: partial, 20 remains
    EXPECT_EQ(reports[1].order_id, 10u);
    EXPECT_EQ(reports[1].kind, ExKind::PartialFill);
    EXPECT_EQ(reports[1].leaves_quantity, 20u);

    // Partial fill of order 2 (20 of 60)
    EXPECT_EQ(reports[2].order_id, 2u);
    EXPECT_EQ(reports[2].kind, ExKind::PartialFill);
    EXPECT_EQ(reports[2].exec_quantity, 20u);
    EXPECT_EQ(reports[2].leaves_quantity, 40u);

    // Agg fully filled
    EXPECT_EQ(reports[3].order_id, 10u);
    EXPECT_EQ(reports[3].kind, ExKind::Fill);
    EXPECT_EQ(reports[3].leaves_quantity, 0u);

    // Order 2 must still be in the book with 40 qty
    EXPECT_EQ(book()->ask_level_at(1500).total_qty, 40u);
    EXPECT_TRUE(book()->has_order(2));
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. MULTI-LEVEL SWEEP
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, MultiLevelSweep_ThreeLevels) {
    // Three ask levels: 1500 (qty 30), 1505 (qty 40), 1510 (qty 30) = 100 total.
    // Aggressive bid of 100 at limit 1510 should sweep all three.
    post_ask(1, 1500, 30);
    post_ask(2, 1505, 40);
    post_ask(3, 1510, 30);

    post_limit_buy(10, 1510, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 6u) << "3 fills → 6 reports";

    // Execution prices in ascending order (best ask first).
    EXPECT_EQ(reports[0].exec_price, 1500);
    EXPECT_EQ(reports[2].exec_price, 1505);
    EXPECT_EQ(reports[4].exec_price, 1510);

    // All three resting asks fully filled.
    EXPECT_EQ(reports[0].kind, ExKind::Fill);
    EXPECT_EQ(reports[2].kind, ExKind::Fill);
    EXPECT_EQ(reports[4].kind, ExKind::Fill);

    // Book must be fully cleared.
    EXPECT_FALSE(book()->has_asks());
    EXPECT_EQ(book()->live_order_count(), 0u);
}

TEST_F(METest, MultiLevelSweep_StopsAtLimitPrice) {
    // Ask at 1500 (crossable) and 1520 (not crossable for bid at 1510).
    post_ask(1, 1500, 100);
    post_ask(2, 1520, 100);

    post_limit_buy(10, 1510, 200);   // limit 1510 only crosses 1500, not 1520

    auto reports = drain_reports();
    // One fill (against ask@1500), then aggressive rests remaining 100 at 1510.
    ASSERT_EQ(reports.size(), 2u) << "Should only fill at 1500, not 1520";

    EXPECT_EQ(reports[0].exec_price, 1500);
    EXPECT_EQ(reports[0].kind, ExKind::Fill);
    EXPECT_EQ(reports[1].kind, ExKind::PartialFill);
    EXPECT_EQ(reports[1].leaves_quantity, 100u);

    // Ask at 1520 must still be in the book untouched.
    EXPECT_TRUE(book()->has_asks());
    EXPECT_EQ(book()->best_ask_price(), 1520);
    EXPECT_TRUE(book()->has_order(2));

    // Aggressive bid residual must be resting at 1510.
    EXPECT_TRUE(book()->has_bids());
    EXPECT_EQ(book()->best_bid_price(), 1510);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. FOK — FILL-OR-KILL
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, FOK_Reject_InsufficientLiquidity) {
    // 50 shares available; FOK needs 100 → reject.
    post_ask(1, 1500, 50);

    post_fok_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u) << "FOK reject must produce exactly one report";

    EXPECT_EQ(reports[0].order_id,       10u);
    EXPECT_EQ(reports[0].kind,           ExKind::Reject);
    EXPECT_EQ(reports[0].exec_quantity,  0u) << "No execution on rejected FOK";
    EXPECT_EQ(reports[0].leaves_quantity,100u);

    // Book must be untouched — resting order still there with full qty.
    EXPECT_EQ(book()->ask_level_at(1500).total_qty, 50u);
    EXPECT_TRUE(book()->has_order(1));
    EXPECT_EQ(book()->live_order_count(), 1u);
}

TEST_F(METest, FOK_Reject_EmptyBook) {
    post_fok_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].kind, ExKind::Reject);
}

TEST_F(METest, FOK_Success_ExactLiquidity) {
    // Exactly 100 shares available at valid prices → FOK executes.
    post_ask(1, 1500, 60);
    post_ask(2, 1505, 40);   // 60 + 40 = 100

    post_fok_buy(10, 1510, 100);   // limit 1510 crosses both

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 4u) << "Two fills → 4 reports";

    // No reject report — everything filled.
    for (auto& r : reports) {
        EXPECT_NE(r.kind, ExKind::Reject);
    }

    EXPECT_EQ(reports[3].order_id, 10u);
    EXPECT_EQ(reports[3].kind,     ExKind::Fill);
    EXPECT_EQ(reports[3].leaves_quantity, 0u);

    EXPECT_FALSE(book()->has_asks());
}

TEST_F(METest, FOK_Reject_LiquidityAtWrongPrice) {
    // 100 shares at 1600, but FOK limit is 1500 — not crossable.
    post_ask(1, 1600, 100);

    post_fok_buy(10, 1500, 100);   // limit too low to cross ask@1600

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].kind, ExKind::Reject);

    // Resting ask at 1600 must be untouched.
    EXPECT_EQ(book()->ask_level_at(1600).total_qty, 100u);
    EXPECT_TRUE(book()->has_order(1));
}

TEST_F(METest, FOK_Success_MultiLevel) {
    post_ask(1, 1500, 33);
    post_ask(2, 1505, 33);
    post_ask(3, 1510, 34);   // 33+33+34 = 100

    post_fok_buy(10, 1510, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 6u);

    for (auto& r : reports) {
        EXPECT_NE(r.kind, ExKind::Reject) << "FOK should not reject when qty is sufficient";
    }
    EXPECT_FALSE(book()->has_asks());
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. IOC — IMMEDIATE-OR-CANCEL
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, IOC_PartialMatch_ResidualCancelled) {
    // 50 shares at 1500; IOC buy of 100 at 1500.
    // Fills 50, then CancelAck for remaining 50. Never rests.
    post_ask(1, 1500, 50);

    post_ioc_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 3u);

    // Fill for resting ask
    EXPECT_EQ(reports[0].order_id, 1u);
    EXPECT_EQ(reports[0].kind, ExKind::Fill);

    // Partial fill for aggressive IOC
    EXPECT_EQ(reports[1].order_id, 10u);
    EXPECT_EQ(reports[1].kind,     ExKind::PartialFill);
    EXPECT_EQ(reports[1].exec_quantity, 50u);
    EXPECT_EQ(reports[1].leaves_quantity, 50u);

    // CancelAck for the unfilled residual
    EXPECT_EQ(reports[2].order_id,        10u);
    EXPECT_EQ(reports[2].kind,            ExKind::CancelAck);
    EXPECT_EQ(reports[2].leaves_quantity,  50u);

    // IOC never rests in the book.
    EXPECT_FALSE(book()->has_bids());
    EXPECT_EQ(book()->live_order_count(), 0u);
}

TEST_F(METest, IOC_ZeroMatch_FullCancel) {
    // No asks on the book; IOC buy gets immediate full cancel.
    post_ioc_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].kind,            ExKind::CancelAck);
    EXPECT_EQ(reports[0].leaves_quantity, 100u);
    EXPECT_FALSE(book()->has_bids());
}

TEST_F(METest, IOC_FullMatch_NoCancelAck) {
    // Exactly enough liquidity — IOC fills fully with no cancel needed.
    post_ask(1, 1500, 100);
    post_ioc_buy(10, 1500, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 2u);   // fill + fill, no CancelAck

    for (auto& r : reports) {
        EXPECT_NE(r.kind, ExKind::CancelAck) << "Full IOC match must not generate CancelAck";
    }
    EXPECT_EQ(reports[1].kind, ExKind::Fill);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. MARKET ORDER
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, MarketOrder_SweepsAllLevels) {
    post_ask(1, 1500, 30);
    post_ask(2, 1550, 30);
    post_ask(3, 1600, 40);   // total 100

    post_market_buy(10, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 6u);

    EXPECT_EQ(reports[0].exec_price, 1500);
    EXPECT_EQ(reports[2].exec_price, 1550);
    EXPECT_EQ(reports[4].exec_price, 1600);

    EXPECT_FALSE(book()->has_asks());
}

TEST_F(METest, MarketOrder_InsufficientLiquidity_PartialThenCancel) {
    // Only 50 shares; market buy of 100 fills 50 then gets CancelAck for 50.
    post_ask(1, 1500, 50);

    post_market_buy(10, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 3u);

    EXPECT_EQ(reports[0].kind, ExKind::Fill);            // resting ask
    EXPECT_EQ(reports[1].kind, ExKind::PartialFill);     // agg market
    EXPECT_EQ(reports[2].kind, ExKind::CancelAck);       // residual discarded
    EXPECT_EQ(reports[2].leaves_quantity, 50u);

    EXPECT_FALSE(book()->has_bids()) << "Market order must never rest in book";
}

TEST_F(METest, MarketOrder_EmptyBook_ImmediateCancel) {
    post_market_sell(10, 100);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].kind, ExKind::CancelAck);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. CANCEL
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, Cancel_RestingOrder_BookUpdated) {
    post_ask(1, 1500, 100);
    EXPECT_EQ(book()->live_order_count(), 1u);

    post_cancel(1);

    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].order_id, 1u);
    EXPECT_EQ(reports[0].kind, ExKind::CancelAck);
    EXPECT_EQ(reports[0].leaves_quantity, 100u);

    EXPECT_FALSE(book()->has_asks());
    EXPECT_FALSE(book()->has_order(1));
    EXPECT_EQ(book()->live_order_count(), 0u);
}

TEST_F(METest, Cancel_PartiallyFilledOrder) {
    // Partially fill resting ask (200 qty → 100 remain), then cancel it.
    post_ask(1, 1500, 200);
    post_limit_buy(10, 1500, 100);
    drain_reports();   // discard fill reports

    post_cancel(1);
    auto reports = drain_reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].kind, ExKind::CancelAck);
    EXPECT_EQ(reports[0].leaves_quantity, 100u);

    EXPECT_FALSE(book()->has_asks());
}

TEST_F(METest, Cancel_UnknownOrder_Ignored) {
    // Cancelling a non-existent order ID must be a no-op.
    post_cancel(9999);

    auto reports = drain_reports();
    EXPECT_EQ(reports.size(), 0u) << "Cancel of unknown ID must produce no report";
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. NO MATCH — ORDER RESTS
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, NoMatch_BidRests_WhenNoAsks) {
    post_bid(1, 1500, 100);

    auto reports = drain_reports();
    EXPECT_EQ(reports.size(), 0u) << "No fill reports when no opposite side";
    EXPECT_TRUE(book()->has_bids());
    EXPECT_EQ(book()->best_bid_price(), 1500);
    EXPECT_EQ(book()->live_order_count(), 1u);
}

TEST_F(METest, NoMatch_BidRests_WhenPriceDoesNotCross) {
    post_ask(1, 1600, 100);   // ask at 1600
    post_bid(10, 1500, 100);  // bid at 1500 — does NOT cross ask at 1600

    auto reports = drain_reports();
    EXPECT_EQ(reports.size(), 0u);
    EXPECT_EQ(book()->live_order_count(), 2u);
    EXPECT_EQ(book()->best_bid_price(), 1500);
    EXPECT_EQ(book()->best_ask_price(), 1600);
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. MIXED WORKLOAD
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(METest, Mixed_InterleavedOperations) {
    // Simulate a realistic sequence: add resting orders, partial fills, cancels.
    post_ask(1, 1500, 100);
    post_ask(2, 1505, 100);
    post_ask(3, 1510, 100);
    post_bid(4, 1490, 50);    // resting bid — does not cross any ask

    // Aggressive buy fills first two levels completely, partially fills third.
    post_limit_buy(10, 1510, 250);

    auto reports = drain_reports();
    // Level 1500: full fill (2 reports) + Level 1505: full fill (2 reports)
    // + Level 1510: partial fill (2 reports) + agg partial report = 6 reports
    // Actually: 100+100+50=250 fills three reports pairs
    // Order 1: 100 filled, order 2: 100 filled, order 3: 50 filled (partial)
    ASSERT_EQ(reports.size(), 6u);

    // Order 3 should be partially filled with 50 remaining.
    EXPECT_TRUE(book()->has_asks());
    EXPECT_EQ(book()->best_ask_price(), 1510);
    EXPECT_EQ(book()->ask_level_at(1510).total_qty, 50u);

    // Resting bid at 1490 must be untouched.
    EXPECT_TRUE(book()->has_bids());
    EXPECT_TRUE(book()->has_order(4));

    // Cancel the partially filled order 3
    post_cancel(3);
    auto cancel_reports = drain_reports();
    ASSERT_EQ(cancel_reports.size(), 1u);
    EXPECT_EQ(cancel_reports[0].leaves_quantity, 50u);
    EXPECT_FALSE(book()->has_asks());

    // Book state: only bid order 4 remains
    EXPECT_EQ(book()->live_order_count(), 1u);
    EXPECT_TRUE(book()->has_order(4));
}

TEST_F(METest, Mixed_StatisticsTracked) {
    post_ask(1, 1500, 100);
    post_limit_buy(10, 1500, 100);

    EXPECT_EQ(engine_.fills_executed(), 1u);
    EXPECT_GE(engine_.orders_processed(), 2u);
}

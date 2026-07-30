// =============================================================================
// test_order_book.cpp
// Google Test suite for hft::lob::PriceLevel and hft::lob::OrderBook
//
// Test groups:
//   1.  PriceLevelLayout     — sizeof/alignment, static_assert proof
//   2.  PriceLevelQueue      — enqueue_tail, dequeue_head, remove_any
//   3.  TickToIndex          — O(1) arithmetic mapping, boundary values
//   4.  AddOrder             — tail insertion, level counters, best cursor
//   5.  CancelOrderById      — O(1) hash map lookup + splice-out
//   6.  CancelOrderMidQueue  — prev/next pointer integrity in 3-node list
//   7.  CancelOrderHead      — head pointer updated on head cancel
//   8.  CancelOrderTail      — tail pointer updated on tail cancel
//   9.  CancelSingleOrder    — head == tail == nullptr after last cancel
//   10. BestCursorBid        — cursor advances on bid level drain
//   11. BestCursorAsk        — cursor advances on ask level drain
//   12. BestCursorMultiLevel — cursor jumps over multiple empty levels
//   13. EmptyBook            — best_bid/best_ask return nullptr on empty book
//   14. OrderIndex           — find, contains, erase semantics
//   15. LiveOrderCount       — add/cancel tracks index size correctly
//   16. PopBestHead          — pop_best_bid/ask_head dequeues + advances cursor
//   17. Mixed                — interleaved adds and cancels stress test
// =============================================================================

#include "lob/order_book.hpp"
#include "core/order_pool.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>
#include <algorithm>

using namespace hft::lob;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// Test fixture — provides a small OrderPool and helper to build Orders.
//
// Book configuration: prices 100–200 (inclusive), tick_size = 1.
// ─────────────────────────────────────────────────────────────────────────────
class OrderBookTest : public ::testing::Test {
protected:
    static constexpr Price kMinPrice = 100;
    static constexpr Price kMaxPrice = 200;
    static constexpr Price kTickSize = 1;

    // Small pool — 1 024 orders is more than enough for unit tests.
    OrderPool<1024> pool_;
    OrderBook book_{1u, kMinPrice, kMaxPrice, kTickSize};

    // Build a fresh Order from the pool with the given fields.
    Order* make_order(OrderId id, Price price, Quantity qty, Side side) {
        Order* o = pool_.acquire();
        EXPECT_NE(o, nullptr) << "Pool exhausted in test";
        o->order_id     = id;
        o->price        = price;
        o->quantity     = qty;
        o->orig_quantity= qty;
        o->side         = side;
        o->type         = OrderType::Limit;
        o->status       = OrderStatus::New;
        o->instrument_id= 1u;
        return o;
    }

    Order* make_bid(OrderId id, Price price, Quantity qty = 100) {
        return make_order(id, price, qty, Side::Buy);
    }
    Order* make_ask(OrderId id, Price price, Quantity qty = 100) {
        return make_order(id, price, qty, Side::Sell);
    }

    void TearDown() override {
        // Nothing — pool destructor frees the mmap region.
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. PRICELEVEL LAYOUT
// ─────────────────────────────────────────────────────────────────────────────

TEST(PriceLevelLayout, SizeIsExactly32Bytes) {
    EXPECT_EQ(sizeof(PriceLevel), 32u)
        << "PriceLevel must be 32 bytes (half a cache line). "
           "Two levels must fit in one 64-byte cache line.";
}

TEST(PriceLevelLayout, FieldOffsets) {
    EXPECT_EQ(offsetof(PriceLevel, head),        0u);
    EXPECT_EQ(offsetof(PriceLevel, tail),        8u);
    EXPECT_EQ(offsetof(PriceLevel, price),      16u);
    EXPECT_EQ(offsetof(PriceLevel, total_qty),  24u);
    EXPECT_EQ(offsetof(PriceLevel, order_count),28u);
}

TEST(PriceLevelLayout, TwoPriceLevelsInOneCacheLine) {
    // Because sizeof(PriceLevel) == 32, two adjacent levels in a
    // std::vector<PriceLevel> always share one 64-byte cache line —
    // a sequential sweep loads two levels per cache miss.
    EXPECT_EQ(2 * sizeof(PriceLevel), 64u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. PRICELEVEL QUEUE OPERATIONS (unit tests on the free functions)
// ─────────────────────────────────────────────────────────────────────────────

TEST(PriceLevelQueue, InitiallyEmpty) {
    PriceLevel lvl{};
    EXPECT_TRUE(level_empty(lvl));
    EXPECT_EQ(lvl.head,        nullptr);
    EXPECT_EQ(lvl.tail,        nullptr);
    EXPECT_EQ(lvl.total_qty,   0u);
    EXPECT_EQ(lvl.order_count, 0u);
}

TEST(PriceLevelQueue, EnqueueSetsHeadAndTail) {
    OrderPool<16> pool;
    PriceLevel lvl{};

    Order* o1 = pool.acquire();
    o1->quantity = 50;

    level_enqueue_tail(lvl, o1);

    EXPECT_EQ(lvl.head,        o1);
    EXPECT_EQ(lvl.tail,        o1);
    EXPECT_EQ(lvl.total_qty,   50u);
    EXPECT_EQ(lvl.order_count, 1u);
    EXPECT_EQ(o1->next, nullptr);
    EXPECT_EQ(o1->prev, nullptr);

    pool.release(o1);
}

TEST(PriceLevelQueue, EnqueueTwoOrders_FIFOLinks) {
    OrderPool<16> pool;
    PriceLevel lvl{};

    Order* o1 = pool.acquire(); o1->quantity = 10;
    Order* o2 = pool.acquire(); o2->quantity = 20;

    level_enqueue_tail(lvl, o1);
    level_enqueue_tail(lvl, o2);

    // o1 is head (first arrived), o2 is tail (last arrived)
    EXPECT_EQ(lvl.head, o1);
    EXPECT_EQ(lvl.tail, o2);
    EXPECT_EQ(o1->next, o2);   // forward link
    EXPECT_EQ(o2->prev, o1);   // backward link
    EXPECT_EQ(o1->prev, nullptr);
    EXPECT_EQ(o2->next, nullptr);
    EXPECT_EQ(lvl.total_qty,   30u);
    EXPECT_EQ(lvl.order_count, 2u);

    pool.release(o1);
    pool.release(o2);
}

TEST(PriceLevelQueue, DequeueHead_SingleOrder) {
    OrderPool<16> pool;
    PriceLevel lvl{};

    Order* o = pool.acquire(); o->quantity = 100;
    level_enqueue_tail(lvl, o);

    Order* got = level_dequeue_head(lvl);
    EXPECT_EQ(got, o);
    EXPECT_TRUE(level_empty(lvl));
    EXPECT_EQ(lvl.head,        nullptr);
    EXPECT_EQ(lvl.tail,        nullptr);
    EXPECT_EQ(lvl.total_qty,   0u);
    EXPECT_EQ(lvl.order_count, 0u);
    EXPECT_EQ(o->next, nullptr);
    EXPECT_EQ(o->prev, nullptr);

    pool.release(o);
}

TEST(PriceLevelQueue, DequeueHead_TwoOrders_HeadAdvances) {
    OrderPool<16> pool;
    PriceLevel lvl{};

    Order* o1 = pool.acquire(); o1->quantity = 30;
    Order* o2 = pool.acquire(); o2->quantity = 40;
    level_enqueue_tail(lvl, o1);
    level_enqueue_tail(lvl, o2);

    Order* got = level_dequeue_head(lvl);
    EXPECT_EQ(got, o1);
    // o2 should now be both head and tail
    EXPECT_EQ(lvl.head, o2);
    EXPECT_EQ(lvl.tail, o2);
    EXPECT_EQ(o2->prev, nullptr) << "New head must have prev == nullptr";
    EXPECT_EQ(o2->next, nullptr) << "Sole remaining order must have next == nullptr";
    EXPECT_EQ(lvl.total_qty,   40u);
    EXPECT_EQ(lvl.order_count, 1u);

    pool.release(o1);
    pool.release(o2);
}

TEST(PriceLevelQueue, DequeueHead_EmptyReturnsNull) {
    PriceLevel lvl{};
    EXPECT_EQ(level_dequeue_head(lvl), nullptr);
}

TEST(PriceLevelQueue, RemoveMiddleNode) {
    OrderPool<16> pool;
    PriceLevel lvl{};

    Order* o1 = pool.acquire(); o1->quantity = 10;
    Order* o2 = pool.acquire(); o2->quantity = 20;
    Order* o3 = pool.acquire(); o3->quantity = 30;
    level_enqueue_tail(lvl, o1);
    level_enqueue_tail(lvl, o2);
    level_enqueue_tail(lvl, o3);

    // Remove middle node (o2)
    level_remove(lvl, o2);

    // Head and tail must be unchanged
    EXPECT_EQ(lvl.head, o1);
    EXPECT_EQ(lvl.tail, o3);
    // o1 <-> o3 must be linked directly
    EXPECT_EQ(o1->next, o3) << "o1->next must point to o3 after o2 removal";
    EXPECT_EQ(o3->prev, o1) << "o3->prev must point to o1 after o2 removal";
    // o2's pointers must be nulled out
    EXPECT_EQ(o2->next, nullptr) << "Removed node's next must be cleared";
    EXPECT_EQ(o2->prev, nullptr) << "Removed node's prev must be cleared";
    EXPECT_EQ(lvl.total_qty,   40u);
    EXPECT_EQ(lvl.order_count, 2u);

    pool.release(o1);
    pool.release(o2);
    pool.release(o3);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. TICK-TO-INDEX MAPPING
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, TickToIndex_MinPrice) {
    EXPECT_EQ(book_.tick_to_index(kMinPrice), 0u);
}

TEST_F(OrderBookTest, TickToIndex_MaxPrice) {
    EXPECT_EQ(book_.tick_to_index(kMaxPrice), 100u);   // (200-100)/1 = 100
}

TEST_F(OrderBookTest, TickToIndex_MidPrice) {
    EXPECT_EQ(book_.tick_to_index(150), 50u);
}

TEST_F(OrderBookTest, TickToIndex_RoundTrip) {
    for (Price p = kMinPrice; p <= kMaxPrice; ++p) {
        std::size_t idx  = book_.tick_to_index(p);
        Price       back = book_.index_to_price(idx);
        EXPECT_EQ(back, p) << "Round-trip failed for price " << p;
    }
}

TEST_F(OrderBookTest, TickToIndex_WithTickSize) {
    // Larger tick size — each level spans 5 ticks
    OrderBook wide{2u, 1000, 2000, 5};
    EXPECT_EQ(wide.tick_to_index(1000), 0u);
    EXPECT_EQ(wide.tick_to_index(1005), 1u);
    EXPECT_EQ(wide.tick_to_index(1500), 100u);
    EXPECT_EQ(wide.tick_to_index(2000), 200u);
}

TEST_F(OrderBookTest, NumLevels) {
    // (200 - 100) / 1 + 1 = 101 levels
    EXPECT_EQ(book_.num_levels(), 101u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. ADD ORDER — TAIL INSERTION
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, AddBidOrder_LevelCountersUpdated) {
    Order* o = make_bid(1, 150, 200);
    book_.add_order(o);

    const PriceLevel& lvl = book_.bid_level_at(150);
    EXPECT_EQ(lvl.head,        o);
    EXPECT_EQ(lvl.tail,        o);
    EXPECT_EQ(lvl.total_qty,   200u);
    EXPECT_EQ(lvl.order_count, 1u);
    EXPECT_EQ(book_.live_order_count(), 1u);
}

TEST_F(OrderBookTest, AddBidOrders_FIFOOrderAtTail) {
    Order* o1 = make_bid(1, 150, 100);
    Order* o2 = make_bid(2, 150, 200);
    Order* o3 = make_bid(3, 150, 300);
    book_.add_order(o1);
    book_.add_order(o2);
    book_.add_order(o3);

    const PriceLevel& lvl = book_.bid_level_at(150);
    EXPECT_EQ(lvl.head,        o1) << "Oldest order must be at head";
    EXPECT_EQ(lvl.tail,        o3) << "Newest order must be at tail";
    EXPECT_EQ(lvl.order_count, 3u);
    EXPECT_EQ(lvl.total_qty,   600u);

    // Verify full chain linkage
    EXPECT_EQ(o1->next, o2);
    EXPECT_EQ(o2->next, o3);
    EXPECT_EQ(o3->next, nullptr);
    EXPECT_EQ(o3->prev, o2);
    EXPECT_EQ(o2->prev, o1);
    EXPECT_EQ(o1->prev, nullptr);
}

TEST_F(OrderBookTest, AddOrders_DifferentPriceLevels_IndependentQueues) {
    Order* b1 = make_bid(1, 150);
    Order* b2 = make_bid(2, 160);
    Order* b3 = make_bid(3, 160);
    book_.add_order(b1);
    book_.add_order(b2);
    book_.add_order(b3);

    EXPECT_EQ(book_.bid_level_at(150).order_count, 1u);
    EXPECT_EQ(book_.bid_level_at(160).order_count, 2u);
    EXPECT_EQ(book_.live_order_count(), 3u);
}

TEST_F(OrderBookTest, AddAskOrder_Registered) {
    Order* a = make_ask(10, 155, 500);
    book_.add_order(a);

    EXPECT_TRUE(book_.has_asks());
    EXPECT_EQ(book_.best_ask_price(), 155);
    EXPECT_EQ(book_.ask_level_at(155).total_qty, 500u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. CANCEL BY ORDER ID — O(1) HASH MAP + SPLICE-OUT
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, CancelById_Found) {
    Order* o = make_bid(99, 140, 300);
    book_.add_order(o);

    EXPECT_TRUE(book_.cancel_order(99u));
    EXPECT_FALSE(book_.has_order(99u));
    EXPECT_EQ(o->status, OrderStatus::Cancelled);
    EXPECT_EQ(book_.bid_level_at(140).order_count, 0u);
    EXPECT_EQ(book_.live_order_count(), 0u);
}

TEST_F(OrderBookTest, CancelById_NotFound_ReturnsFalse) {
    EXPECT_FALSE(book_.cancel_order(42u))
        << "Cancel of unknown ID must return false without crashing";
}

TEST_F(OrderBookTest, CancelById_AlreadyCancelled_ReturnsFalse) {
    Order* o = make_bid(7, 120);
    book_.add_order(o);
    EXPECT_TRUE(book_.cancel_order(7u));
    EXPECT_FALSE(book_.cancel_order(7u)) << "Double-cancel must return false";
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. CANCEL MID-QUEUE — INTRUSIVE SPLICE-OUT POINTER INTEGRITY
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, CancelMidQueue_PointerIntegrity) {
    // Three orders at the same price level; cancel the middle one.
    Order* o1 = make_bid(1, 130, 10);
    Order* o2 = make_bid(2, 130, 20);
    Order* o3 = make_bid(3, 130, 30);
    book_.add_order(o1);
    book_.add_order(o2);
    book_.add_order(o3);

    // Cancel o2 (middle)
    book_.cancel_order(o2);

    const PriceLevel& lvl = book_.bid_level_at(130);

    // Head / tail unchanged
    EXPECT_EQ(lvl.head, o1) << "Head must still be o1";
    EXPECT_EQ(lvl.tail, o3) << "Tail must still be o3";

    // o1 <-> o3 directly linked
    EXPECT_EQ(o1->next, o3) << "o1->next must jump to o3";
    EXPECT_EQ(o3->prev, o1) << "o3->prev must point back to o1";

    // o2's pointers must be nulled
    EXPECT_EQ(o2->next, nullptr) << "Cancelled order's next must be null";
    EXPECT_EQ(o2->prev, nullptr) << "Cancelled order's prev must be null";

    // Aggregate counters
    EXPECT_EQ(lvl.order_count, 2u);
    EXPECT_EQ(lvl.total_qty,   40u);  // 10 + 30
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. CANCEL HEAD ORDER — HEAD POINTER UPDATED
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, CancelHead_HeadPointerAdvances) {
    Order* o1 = make_bid(1, 110, 100);
    Order* o2 = make_bid(2, 110, 200);
    Order* o3 = make_bid(3, 110, 300);
    book_.add_order(o1);
    book_.add_order(o2);
    book_.add_order(o3);

    book_.cancel_order(o1);   // cancel the head

    const PriceLevel& lvl = book_.bid_level_at(110);
    EXPECT_EQ(lvl.head, o2) << "After head cancel, o2 must be the new head";
    EXPECT_EQ(lvl.tail, o3) << "Tail must be unchanged";
    EXPECT_EQ(o2->prev, nullptr) << "New head must have prev == nullptr";
    EXPECT_EQ(o1->next, nullptr) << "Cancelled head's next must be null";
    EXPECT_EQ(o1->prev, nullptr) << "Cancelled head's prev must be null";
    EXPECT_EQ(lvl.order_count, 2u);
    EXPECT_EQ(lvl.total_qty,   500u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. CANCEL TAIL ORDER — TAIL POINTER UPDATED
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, CancelTail_TailPointerRetreats) {
    Order* o1 = make_bid(1, 115, 100);
    Order* o2 = make_bid(2, 115, 200);
    Order* o3 = make_bid(3, 115, 300);
    book_.add_order(o1);
    book_.add_order(o2);
    book_.add_order(o3);

    book_.cancel_order(o3);   // cancel the tail

    const PriceLevel& lvl = book_.bid_level_at(115);
    EXPECT_EQ(lvl.head, o1) << "Head must be unchanged";
    EXPECT_EQ(lvl.tail, o2) << "After tail cancel, o2 must be the new tail";
    EXPECT_EQ(o2->next, nullptr) << "New tail must have next == nullptr";
    EXPECT_EQ(o3->next, nullptr) << "Cancelled tail's next must be null";
    EXPECT_EQ(o3->prev, nullptr) << "Cancelled tail's prev must be null";
    EXPECT_EQ(lvl.order_count, 2u);
    EXPECT_EQ(lvl.total_qty,   300u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. CANCEL SINGLE ORDER — LEVEL GOES FULLY EMPTY
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, CancelSingleOrder_LevelBecomesEmpty) {
    Order* o = make_bid(1, 120, 100);
    book_.add_order(o);
    book_.cancel_order(o);

    const PriceLevel& lvl = book_.bid_level_at(120);
    EXPECT_TRUE(level_empty(lvl));
    EXPECT_EQ(lvl.head,        nullptr) << "head must be null after last cancel";
    EXPECT_EQ(lvl.tail,        nullptr) << "tail must be null after last cancel";
    EXPECT_EQ(lvl.total_qty,   0u);
    EXPECT_EQ(lvl.order_count, 0u);
    EXPECT_EQ(o->next, nullptr);
    EXPECT_EQ(o->prev, nullptr);
    EXPECT_EQ(o->status, OrderStatus::Cancelled);
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. BEST BID CURSOR — ADVANCES WHEN BID LEVEL IS DRAINED
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, BestBidCursor_SingleLevel) {
    Order* o = make_bid(1, 150);
    book_.add_order(o);

    EXPECT_EQ(book_.best_bid_price(), 150);
    book_.cancel_order(o);
    EXPECT_FALSE(book_.has_bids()) << "best_bid must be null after only bid cancelled";
}

TEST_F(OrderBookTest, BestBidCursor_AdvancesDown_OnDrain) {
    // Two bid levels: 150 and 155.  Best bid is 155.
    // After 155 is cancelled, best bid should retreat to 150.
    Order* b1 = make_bid(1, 150, 100);
    Order* b2 = make_bid(2, 155, 200);
    book_.add_order(b1);
    book_.add_order(b2);

    EXPECT_EQ(book_.best_bid_price(), 155) << "Before cancel: best bid should be 155";
    book_.cancel_order(b2);
    EXPECT_EQ(book_.best_bid_price(), 150) << "After 155 cancel: best bid should retreat to 150";
}

TEST_F(OrderBookTest, BestBidCursor_UnchangedWhenNonBestCancelled) {
    Order* b1 = make_bid(1, 140);
    Order* b2 = make_bid(2, 155);
    book_.add_order(b1);
    book_.add_order(b2);

    EXPECT_EQ(book_.best_bid_price(), 155);
    book_.cancel_order(b1);   // cancel the inferior bid
    EXPECT_EQ(book_.best_bid_price(), 155) << "Best bid must not change when inferior bid cancelled";
}

TEST_F(OrderBookTest, BestBidCursor_MultipleCancels_StillCorrect) {
    // Add 5 orders at 5 different prices; cancel top 4; cursor must end at bottom.
    std::vector<Order*> orders;
    for (int p = 150; p <= 154; ++p) {
        orders.push_back(make_bid(static_cast<OrderId>(p), static_cast<Price>(p)));
        book_.add_order(orders.back());
    }

    EXPECT_EQ(book_.best_bid_price(), 154);
    for (int i = 4; i >= 1; --i) {   // cancel 154, 153, 152, 151
        book_.cancel_order(orders[static_cast<std::size_t>(i)]);
        EXPECT_EQ(book_.best_bid_price(), orders[static_cast<std::size_t>(i-1)]->price)
            << "Cursor wrong after cancel at index " << i;
    }
    book_.cancel_order(orders[0]);
    EXPECT_FALSE(book_.has_bids());
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. BEST ASK CURSOR — ADVANCES WHEN ASK LEVEL IS DRAINED
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, BestAskCursor_SingleLevel) {
    Order* o = make_ask(10, 155);
    book_.add_order(o);

    EXPECT_EQ(book_.best_ask_price(), 155);
    book_.cancel_order(o);
    EXPECT_FALSE(book_.has_asks());
}

TEST_F(OrderBookTest, BestAskCursor_AdvancesUp_OnDrain) {
    // Two ask levels: 155 and 160.  Best ask is 155.
    // After 155 is cancelled, best ask should advance to 160.
    Order* a1 = make_ask(10, 155);
    Order* a2 = make_ask(11, 160);
    book_.add_order(a1);
    book_.add_order(a2);

    EXPECT_EQ(book_.best_ask_price(), 155);
    book_.cancel_order(a1);
    EXPECT_EQ(book_.best_ask_price(), 160) << "Best ask must advance to 160 after 155 drained";
}

TEST_F(OrderBookTest, BestAskCursor_UnchangedWhenNonBestCancelled) {
    Order* a1 = make_ask(10, 155);
    Order* a2 = make_ask(11, 170);
    book_.add_order(a1);
    book_.add_order(a2);

    EXPECT_EQ(book_.best_ask_price(), 155);
    book_.cancel_order(a2);   // cancel the inferior ask
    EXPECT_EQ(book_.best_ask_price(), 155);
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. BEST CURSOR — JUMPS OVER MULTIPLE EMPTY LEVELS
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, BestBidCursor_JumpsOverGaps) {
    // Add bids at 100, 120, 150.  Cancel 120 and 150; cursor must land at 100.
    Order* b100 = make_bid(1, 100);
    Order* b120 = make_bid(2, 120);
    Order* b150 = make_bid(3, 150);
    book_.add_order(b100);
    book_.add_order(b120);
    book_.add_order(b150);

    EXPECT_EQ(book_.best_bid_price(), 150);
    book_.cancel_order(b150);
    EXPECT_EQ(book_.best_bid_price(), 120);
    book_.cancel_order(b120);
    EXPECT_EQ(book_.best_bid_price(), 100);
    book_.cancel_order(b100);
    EXPECT_FALSE(book_.has_bids());
}

TEST_F(OrderBookTest, BestAskCursor_JumpsOverGaps) {
    Order* a155 = make_ask(10, 155);
    Order* a170 = make_ask(11, 170);
    Order* a190 = make_ask(12, 190);
    book_.add_order(a155);
    book_.add_order(a170);
    book_.add_order(a190);

    EXPECT_EQ(book_.best_ask_price(), 155);
    book_.cancel_order(a155);
    EXPECT_EQ(book_.best_ask_price(), 170);
    book_.cancel_order(a170);
    EXPECT_EQ(book_.best_ask_price(), 190);
    book_.cancel_order(a190);
    EXPECT_FALSE(book_.has_asks());
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. EMPTY BOOK
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, EmptyBook_BestBidIsNull) {
    EXPECT_EQ(book_.best_bid(),       nullptr);
    EXPECT_EQ(book_.best_bid_price(), kInvalidPrice);
    EXPECT_FALSE(book_.has_bids());
}

TEST_F(OrderBookTest, EmptyBook_BestAskIsNull) {
    EXPECT_EQ(book_.best_ask(),       nullptr);
    EXPECT_EQ(book_.best_ask_price(), kInvalidPrice);
    EXPECT_FALSE(book_.has_asks());
}

TEST_F(OrderBookTest, EmptyBook_IsEmpty) {
    EXPECT_TRUE(book_.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. ORDER INDEX
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, OrderIndex_InsertAndFind) {
    Order* o = make_bid(555, 130);
    book_.add_order(o);

    Order* found = book_.find_order(555);
    EXPECT_EQ(found, o);
    EXPECT_TRUE(book_.has_order(555));
}

TEST_F(OrderBookTest, OrderIndex_FindAfterCancel_ReturnsNull) {
    Order* o = make_bid(777, 140);
    book_.add_order(o);
    book_.cancel_order(o);

    EXPECT_EQ(book_.find_order(777), nullptr);
    EXPECT_FALSE(book_.has_order(777));
}

TEST_F(OrderBookTest, OrderIndex_MissingId_ReturnsNull) {
    EXPECT_EQ(book_.find_order(99999), nullptr);
}

// ─────────────────────────────────────────────────────────────────────────────
// 15. LIVE ORDER COUNT
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, LiveOrderCount_TracksCancels) {
    EXPECT_EQ(book_.live_order_count(), 0u);

    Order* o1 = make_bid(1, 130);
    Order* o2 = make_ask(2, 140);
    book_.add_order(o1);
    EXPECT_EQ(book_.live_order_count(), 1u);
    book_.add_order(o2);
    EXPECT_EQ(book_.live_order_count(), 2u);
    book_.cancel_order(o1);
    EXPECT_EQ(book_.live_order_count(), 1u);
    book_.cancel_order(o2);
    EXPECT_EQ(book_.live_order_count(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// 16. POP BEST BID / ASK HEAD — DEQUEUE + CURSOR ADVANCE
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, PopBestBidHead_ReturnsFrontOrder) {
    Order* o1 = make_bid(1, 150, 100);
    Order* o2 = make_bid(2, 150, 200);
    book_.add_order(o1);
    book_.add_order(o2);

    Order* popped = book_.pop_best_bid_head();
    EXPECT_EQ(popped, o1) << "Should pop oldest (head) order first";
    EXPECT_EQ(book_.bid_level_at(150).head, o2);
    EXPECT_EQ(book_.live_order_count(), 1u);  // o2 still resting
}

TEST_F(OrderBookTest, PopBestBidHead_AdvancesCursorOnDrain) {
    Order* b150_a = make_bid(1, 150, 100);
    Order* b155   = make_bid(2, 155, 200);  // better bid
    book_.add_order(b150_a);
    book_.add_order(b155);

    EXPECT_EQ(book_.best_bid_price(), 155);

    Order* popped = book_.pop_best_bid_head();   // drains level 155
    EXPECT_EQ(popped, b155);
    EXPECT_EQ(book_.best_bid_price(), 150)
        << "Cursor must advance to 150 after 155 level is drained";
}

TEST_F(OrderBookTest, PopBestBidHead_EmptyBookReturnsNull) {
    EXPECT_EQ(book_.pop_best_bid_head(), nullptr);
}

TEST_F(OrderBookTest, PopBestAskHead_ReturnsFrontOrder) {
    Order* a1 = make_ask(10, 155, 300);
    Order* a2 = make_ask(11, 155, 400);
    book_.add_order(a1);
    book_.add_order(a2);

    Order* popped = book_.pop_best_ask_head();
    EXPECT_EQ(popped, a1);
    EXPECT_EQ(book_.ask_level_at(155).head, a2);
}

TEST_F(OrderBookTest, PopBestAskHead_AdvancesCursorOnDrain) {
    Order* a155 = make_ask(10, 155);
    Order* a160 = make_ask(11, 160);
    book_.add_order(a155);
    book_.add_order(a160);

    EXPECT_EQ(book_.best_ask_price(), 155);
    book_.pop_best_ask_head();   // drains 155
    EXPECT_EQ(book_.best_ask_price(), 160)
        << "Cursor must advance to 160 after 155 level drained by pop";
}

// ─────────────────────────────────────────────────────────────────────────────
// 17. MIXED STRESS — INTERLEAVED ADDS AND CANCELS
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(OrderBookTest, Mixed_InterleavedAddsAndCancels) {
    // Simulate 200 rounds: add bid + ask at random prices, cancel every other one.
    std::vector<Order*> live_bids, live_asks;
    OrderId next_id = 1;

    const Price bid_prices[] = {100, 110, 120, 130, 140};
    const Price ask_prices[] = {160, 170, 180, 190, 200};

    for (int round = 0; round < 20; ++round) {
        // Add one bid and one ask at cycling prices
        Price bp = bid_prices[round % 5];
        Price ap = ask_prices[round % 5];

        Order* b = make_bid(next_id++, bp);
        Order* a = make_ask(next_id++, ap);
        book_.add_order(b);
        book_.add_order(a);
        live_bids.push_back(b);
        live_asks.push_back(a);
    }

    const std::size_t total = live_bids.size() + live_asks.size();
    EXPECT_EQ(book_.live_order_count(), total);
    EXPECT_TRUE(book_.has_bids());
    EXPECT_TRUE(book_.has_asks());

    // Cancel every even-indexed order
    for (std::size_t i = 0; i < live_bids.size(); i += 2) book_.cancel_order(live_bids[i]);
    for (std::size_t i = 0; i < live_asks.size(); i += 2) book_.cancel_order(live_asks[i]);

    // Remaining orders must still be findable
    for (std::size_t i = 1; i < live_bids.size(); i += 2) {
        EXPECT_TRUE(book_.has_order(live_bids[i]->order_id))
            << "Bid order " << live_bids[i]->order_id << " should still be live";
    }
    for (std::size_t i = 1; i < live_asks.size(); i += 2) {
        EXPECT_TRUE(book_.has_order(live_asks[i]->order_id))
            << "Ask order " << live_asks[i]->order_id << " should still be live";
    }
}

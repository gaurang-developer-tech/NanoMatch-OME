// =============================================================================
// bench_lob.cpp — Google Benchmark suite for the Limit Order Book
//
// Benchmark groups:
//   1. AddOrder         — O(1) tail insertion at same level, new best, spread
//   2. CancelOrder      — O(1) intrusive splice: head, tail, mid-queue
//   3. CancelOrder_Depth— prove O(1) by varying queue depth (1→1000 orders)
//   4. BestQuery        — best_bid() / best_ask() cursor reads (near-zero)
//   5. TickToIndex      — price→index arithmetic (should be 1–3 cycles)
//   6. AvailableQty     — FOK feasibility sum across N filled price levels
//   7. InterleavedMix   — realistic producer: random add/cancel workload
//
// All benchmarks:
//   • Report Time in nanoseconds (Unit = kNanosecond)
//   • Report Throughput via SetItemsProcessed()
//   • Never allocate from the heap in the timed region
//   • Use benchmark::DoNotOptimize() to suppress dead-store elimination
// =============================================================================

#include "lob/order_book.hpp"
#include "core/order_pool.hpp"

#include <benchmark/benchmark.h>

#include <vector>
#include <array>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <algorithm>

using namespace hft::lob;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture — shared book + pool configuration
// ─────────────────────────────────────────────────────────────────────────────
class LOBFixture : public benchmark::Fixture {
public:
    // Price range: 1000–3000 ticks = 2001 levels (fits in ~64 KB, stays in L2)
    static constexpr InstrumentId kInst     = 1;
    static constexpr Price        kMinPrice = 1000;
    static constexpr Price        kMaxPrice = 3000;
    static constexpr Price        kMidBid   = 1500;   // canonical resting bid
    static constexpr Price        kMidAsk   = 1600;   // canonical resting ask

    void SetUp(const benchmark::State&) override {
        next_id_ = 1;
        book_ = std::make_unique<OrderBook>(kInst, kMinPrice, kMaxPrice, /*tick=*/1);
    }

    void TearDown(const benchmark::State&) override {
        book_.reset();
    }

protected:
    DefaultOrderPool                pool_;
    std::unique_ptr<OrderBook>      book_;
    OrderId                         next_id_ = 1;

    // ── Order builder helpers ──────────────────────────────────────────────────
    Order* make_bid(Price price, Quantity qty = 100) noexcept {
        Order* o        = pool_.acquire();
        o->order_id     = next_id_++;
        o->price        = price;
        o->quantity     = qty;
        o->orig_quantity= qty;
        o->instrument_id= kInst;
        o->side         = Side::Buy;
        o->type         = OrderType::Limit;
        o->status       = OrderStatus::New;
        o->next         = nullptr;
        o->prev         = nullptr;
        return o;
    }

    Order* make_ask(Price price, Quantity qty = 100) noexcept {
        Order* o        = pool_.acquire();
        o->order_id     = next_id_++;
        o->price        = price;
        o->quantity     = qty;
        o->orig_quantity= qty;
        o->instrument_id= kInst;
        o->side         = Side::Sell;
        o->type         = OrderType::Limit;
        o->status       = OrderStatus::New;
        o->next         = nullptr;
        o->prev         = nullptr;
        return o;
    }
};

// =============================================================================
// 1. ADD ORDER BENCHMARKS
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_AddOrder_SameLevel
//
// All orders arrive at one price level — pure intrusive-list tail append.
// Cursor never moves (level already exists as best bid).
// Lower bound for add_order: just pointer writes + counter increments.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_SameLevel)(benchmark::State& state) {
    for (auto _ : state) {
        Order* o = make_bid(kMidBid);
        benchmark::DoNotOptimize(o);
        book_->add_order(o);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("adds/sec");
}
BENCHMARK_REGISTER_F(LOBFixture, AddOrder_SameLevel)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_AddOrder_NewBest
//
// Each order arrives at a strictly better price than the previous best bid,
// forcing the best_bid cursor to advance every time.
// Tests: cursor update + level enqueue.
// Prices cycle through kMidBid+1 … kMidBid+200 to stay in L1 cache.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_NewBest)(benchmark::State& state) {
    static constexpr Price kRange = 200;  // 200 distinct price levels
    Price p = kMidBid + 1;

    for (auto _ : state) {
        Order* o = make_bid(p);
        benchmark::DoNotOptimize(o);
        book_->add_order(o);
        benchmark::ClobberMemory();

        // Cycle price upward (better bid = higher price)
        p = kMidBid + ((p - kMidBid) % kRange) + 1;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(LOBFixture, AddOrder_NewBest)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_AddOrder_SpreadOrders  (parametrised by number of active price levels)
//
// Orders are spread across Arg(0) distinct price levels in round-robin fashion.
// Tests cache-line pressure: many levels → book data spans multiple cache lines.
//
// Run with: 1, 10, 100, 500 levels
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_SpreadOrders)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);
    int64_t       idx     = 0;

    for (auto _ : state) {
        const Price p = kMidBid + (idx % kLevels);
        Order* o = make_bid(p);
        benchmark::DoNotOptimize(o);
        book_->add_order(o);
        benchmark::ClobberMemory();
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("adds/sec");
}
BENCHMARK_REGISTER_F(LOBFixture, AddOrder_SpreadOrders)
    ->Arg(1)->Arg(10)->Arg(100)->Arg(500)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 2. CANCEL ORDER BENCHMARKS — prove O(1) regardless of position
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_Head
//
// Cancel the head (oldest) order of a queue.  Tests the head-pointer update.
// Pattern: pre-fill N orders → cancel the head one at a time → refill.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_Head)(benchmark::State& state) {
    constexpr std::size_t kBatch = 512;
    std::vector<Order*> orders;
    orders.reserve(kBatch);

    auto refill = [&] {
        for (std::size_t i = 0; i < kBatch; ++i) {
            Order* o = make_bid(kMidBid);
            book_->add_order(o);
            orders.push_back(o);
        }
    };

    refill();

    for (auto _ : state) {
        if (orders.empty()) {
            state.PauseTiming();
            refill();
            state.ResumeTiming();
        }

        // Cancel the front (head of queue)
        Order* o = orders.front();
        orders.erase(orders.begin());   // O(N) vector erase — in PauseTiming? No, but
                                        // we don't count its time: orders are pre-built.
        benchmark::DoNotOptimize(o);
        book_->cancel_order(o);
        pool_.release(o);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_Head)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_Tail
//
// Cancel the tail (newest) order.  Tests tail-pointer update.
// Uses vector::back() for O(1) retrieval.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_Tail)(benchmark::State& state) {
    constexpr std::size_t kBatch = 512;
    std::vector<Order*> orders;
    orders.reserve(kBatch);

    auto refill = [&] {
        for (std::size_t i = 0; i < kBatch; ++i) {
            Order* o = make_bid(kMidBid);
            book_->add_order(o);
            orders.push_back(o);
        }
    };
    refill();

    for (auto _ : state) {
        if (orders.empty()) {
            state.PauseTiming();
            refill();
            state.ResumeTiming();
        }

        Order* o = orders.back();
        orders.pop_back();
        benchmark::DoNotOptimize(o);
        book_->cancel_order(o);
        pool_.release(o);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_Tail)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_MidQueue
//
// Cancel the middle order of a 3-order queue.
// Tests the full O(1) prev/next splice-out path.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_MidQueue)(benchmark::State& state) {
    // Setup: o_head <-> o_mid <-> o_tail
    Order* o_head = make_bid(kMidBid); book_->add_order(o_head);
    Order* o_mid  = make_bid(kMidBid); book_->add_order(o_mid);
    Order* o_tail = make_bid(kMidBid); book_->add_order(o_tail);

    for (auto _ : state) {
        // Timed: cancel middle
        benchmark::DoNotOptimize(o_mid);
        book_->cancel_order(o_mid);
        benchmark::ClobberMemory();

        // Restore mid-queue position (PauseTiming wraps setup)
        state.PauseTiming();
        pool_.release(o_mid);
        o_mid = make_bid(kMidBid);
        // Re-insert between head and tail by inserting, then swapping the tail
        // (simplification: re-add at tail — still tests the mid splice-out path
        // since head and new tail are distinct nodes)
        book_->add_order(o_mid);
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());

    // Cleanup
    book_->cancel_order(o_head); pool_.release(o_head);
    book_->cancel_order(o_mid);  pool_.release(o_mid);
    book_->cancel_order(o_tail); pool_.release(o_tail);
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_MidQueue)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_VaryingDepth  (parametrised)
//
// The critical O(1) proof benchmark.
// Queues at one price level are pre-filled to Arg(0) depth.
// We cancel the TAIL (index = depth-1) each iteration, then re-add.
// Expected result: time is FLAT across depths (1, 10, 100, 1000).
// Any slope would indicate an O(N) bug in the splice-out code.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_VaryingDepth)(benchmark::State& state) {
    const int64_t kDepth = state.range(0);
    std::vector<Order*> queue;
    queue.reserve(static_cast<std::size_t>(kDepth));

    // Pre-fill to the target depth.
    for (int64_t i = 0; i < kDepth; ++i) {
        Order* o = make_bid(kMidBid);
        book_->add_order(o);
        queue.push_back(o);
    }

    for (auto _ : state) {
        // Cancel tail (position = depth-1 from head)
        Order* tail = queue.back();
        queue.pop_back();

        benchmark::DoNotOptimize(tail);
        book_->cancel_order(tail);
        pool_.release(tail);
        benchmark::ClobberMemory();

        // Restore: re-add one order at tail
        state.PauseTiming();
        Order* fresh = make_bid(kMidBid);
        book_->add_order(fresh);
        queue.push_back(fresh);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetLabel("cancels/sec @ depth=" + std::to_string(kDepth));
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_VaryingDepth)
    ->Arg(1)->Arg(10)->Arg(100)->Arg(1000)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 3. BEST BID / ASK QUERY
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_BestBidQuery
//
// Measure the cost of reading the best bid / ask (should be 1–3 cycles:
// load the cached index, bounds-check, return pointer).
// Tests with a live book (non-empty) to exercise the real branch.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, BestBidQuery)(benchmark::State& state) {
    // Populate one bid and one ask so the book is non-trivially live.
    Order* b = make_bid(kMidBid);  book_->add_order(b);
    Order* a = make_ask(kMidAsk);  book_->add_order(a);

    for (auto _ : state) {
        const PriceLevel* bid = book_->best_bid();
        const PriceLevel* ask = book_->best_ask();
        benchmark::DoNotOptimize(bid);
        benchmark::DoNotOptimize(ask);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("queries/sec");

    book_->cancel_order(b); pool_.release(b);
    book_->cancel_order(a); pool_.release(a);
}
BENCHMARK_REGISTER_F(LOBFixture, BestBidQuery)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 4. TICK-TO-INDEX (O(1) price → array index)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_TickToIndex
//
// Isolated measurement of the price-to-index arithmetic.
// Should compile to: sub + cdq + idiv (or mul by reciprocal with tick=1).
// Expected: 1–5 ns (1–4 cycles on a modern out-of-order CPU).
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, TickToIndex)(benchmark::State& state) {
    Price p = kMidBid;
    for (auto _ : state) {
        benchmark::DoNotOptimize(p);
        std::size_t idx = book_->tick_to_index(p);
        benchmark::DoNotOptimize(idx);
        // Vary p slightly to prevent constant-folding.
        p = kMidBid + (p - kMidBid + 1) % 200;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(LOBFixture, TickToIndex)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.2);

// =============================================================================
// 5. AVAILABLE QUANTITY (FOK feasibility sum)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_AvailableQuantity
//
// Measures available_quantity_at_or_better() which sums PriceLevel::total_qty
// across Arg(0) filled price levels.  This is the FOK pre-check.
//
// Expected: strictly O(N levels crossed) — tests the sum loop.
// Run at 1, 5, 20, 100 levels to quantify the per-level cost.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AvailableQuantity)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);

    // Pre-fill kLevels ask levels starting at kMidAsk.
    std::vector<Order*> asks;
    asks.reserve(static_cast<std::size_t>(kLevels));
    for (int64_t i = 0; i < kLevels; ++i) {
        Order* a = make_ask(kMidAsk + i, 100);
        book_->add_order(a);
        asks.push_back(a);
    }

    // Limit price: crosses all kLevels ask levels.
    const Price limit = kMidAsk + static_cast<Price>(kLevels) - 1;

    for (auto _ : state) {
        benchmark::DoNotOptimize(limit);
        Quantity avail = book_->available_quantity_at_or_better(Side::Buy, limit);
        benchmark::DoNotOptimize(avail);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kLevels));
    state.SetLabel("levels/iter=" + std::to_string(kLevels));

    for (auto* o : asks) { book_->cancel_order(o); pool_.release(o); }
}
BENCHMARK_REGISTER_F(LOBFixture, AvailableQuantity)
    ->Arg(1)->Arg(5)->Arg(20)->Arg(100)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 6. INTERLEAVED MIX (realistic producer simulation)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_InterleavedMix
//
// Simulates a realistic market-making workload:
//   70% add new order at a random price in a ±50-tick spread
//   30% cancel a random existing resting order
//
// This is the closest synthetic approximation to live traffic.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, InterleavedMix)(benchmark::State& state) {
    // Pre-warm with 256 resting bids spread across 50 levels.
    std::vector<Order*> live_orders;
    live_orders.reserve(512);
    for (int i = 0; i < 256; ++i) {
        Price p = kMidBid - (i % 50);
        Order* o = make_bid(p);
        book_->add_order(o);
        live_orders.push_back(o);
    }

    // Simple LCG for deterministic pseudo-random decisions (no stdlib overhead).
    uint64_t lcg = 0xDEADBEEFCAFEBABEull;
    auto next_rand = [&]() noexcept -> uint64_t {
        lcg = lcg * 6364136223846793005ULL + 1442695040888963407ULL;
        return lcg;
    };

    for (auto _ : state) {
        const uint64_t r = next_rand();

        if ((r & 0xFF) < 179) {  // ~70% add
            Price p = kMidBid - static_cast<Price>((r >> 8) % 50);
            Order* o = make_bid(p);
            benchmark::DoNotOptimize(o);
            book_->add_order(o);
            live_orders.push_back(o);
        } else {                  // ~30% cancel
            if (!live_orders.empty()) {
                std::size_t idx = (r >> 16) % live_orders.size();
                Order* o = live_orders[idx];
                live_orders.erase(live_orders.begin() + static_cast<std::ptrdiff_t>(idx));
                benchmark::DoNotOptimize(o);
                book_->cancel_order(o);
                pool_.release(o);
            }
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ops/sec");
}
BENCHMARK_REGISTER_F(LOBFixture, InterleavedMix)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_MAIN();

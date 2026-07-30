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
        if (__builtin_expect(o == nullptr, 0)) return nullptr;
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
        if (__builtin_expect(o == nullptr, 0)) return nullptr;
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
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_SameLevel)(benchmark::State& state) {
    for (auto _ : state) {
        if (__builtin_expect(pool_.free_count() < 100, 0)) {
            state.PauseTiming();
            book_->clear();
            pool_.reset();
            state.ResumeTiming();
        }
        Order* o = make_bid(kMidBid);
        if (!o) { state.SkipWithError("Pool exhausted"); break; }
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
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_NewBest)(benchmark::State& state) {
    static constexpr Price kRange = 200;  // 200 distinct price levels
    Price p = kMidBid + 1;

    for (auto _ : state) {
        if (__builtin_expect(pool_.free_count() < 100, 0)) {
            state.PauseTiming();
            book_->clear();
            pool_.reset();
            state.ResumeTiming();
        }
        Order* o = make_bid(p);
        if (!o) { state.SkipWithError("Pool exhausted"); break; }
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
// BM_LOB_AddOrder_SpreadOrders
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AddOrder_SpreadOrders)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);
    int64_t       idx     = 0;

    for (auto _ : state) {
        if (__builtin_expect(pool_.free_count() < 100, 0)) {
            state.PauseTiming();
            book_->clear();
            pool_.reset();
            state.ResumeTiming();
        }
        const Price p = kMidBid + (idx % kLevels);
        Order* o = make_bid(p);
        if (!o) { state.SkipWithError("Pool exhausted"); break; }
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
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_Head)(benchmark::State& state) {
    constexpr std::size_t kBatch = 512;
    std::vector<Order*> orders;
    orders.reserve(kBatch);

    auto refill = [&] {
        orders.clear();
        book_->clear();
        pool_.reset();
        for (std::size_t i = 0; i < kBatch; ++i) {
            Order* o = make_bid(kMidBid);
            if (!o) break;
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
        if (orders.empty()) { state.SkipWithError("Pool exhausted"); break; }

        Order* o = orders.front();
        orders.erase(orders.begin());
        if (!o) { state.SkipWithError("Pool exhausted"); break; }
        benchmark::DoNotOptimize(o);
        book_->cancel_order(o);
        pool_.release(o);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    for (auto* o : orders) { if (o) { book_->cancel_order(o); pool_.release(o); } }
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_Head)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_Tail
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_Tail)(benchmark::State& state) {
    constexpr std::size_t kBatch = 512;
    std::vector<Order*> orders;
    orders.reserve(kBatch);

    auto refill = [&] {
        orders.clear();
        book_->clear();
        pool_.reset();
        for (std::size_t i = 0; i < kBatch; ++i) {
            Order* o = make_bid(kMidBid);
            if (!o) break;
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
        if (orders.empty()) { state.SkipWithError("Pool exhausted"); break; }

        Order* o = orders.back();
        orders.pop_back();
        if (!o) { state.SkipWithError("Pool exhausted"); break; }
        benchmark::DoNotOptimize(o);
        book_->cancel_order(o);
        pool_.release(o);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    for (auto* o : orders) { if (o) { book_->cancel_order(o); pool_.release(o); } }
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_Tail)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_MidQueue
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_MidQueue)(benchmark::State& state) {
    Order* o_head = make_bid(kMidBid);
    Order* o_mid  = make_bid(kMidBid);
    Order* o_tail = make_bid(kMidBid);
    if (!o_head || !o_mid || !o_tail) {
        state.SkipWithError("Pool exhausted on setup");
        return;
    }
    book_->add_order(o_head);
    book_->add_order(o_mid);
    book_->add_order(o_tail);

    for (auto _ : state) {
        if (!o_mid) { state.SkipWithError("Pool exhausted"); break; }
        benchmark::DoNotOptimize(o_mid);
        book_->cancel_order(o_mid);
        benchmark::ClobberMemory();

        state.PauseTiming();
        pool_.release(o_mid);
        o_mid = make_bid(kMidBid);
        if (!o_mid) {
            state.SkipWithError("Pool exhausted");
            state.ResumeTiming();
            break;
        }
        book_->add_order(o_mid);
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());

    if (o_head) { book_->cancel_order(o_head); pool_.release(o_head); }
    if (o_mid)  { book_->cancel_order(o_mid);  pool_.release(o_mid); }
    if (o_tail) { book_->cancel_order(o_tail); pool_.release(o_tail); }
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_MidQueue)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_CancelOrder_VaryingDepth
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, CancelOrder_VaryingDepth)(benchmark::State& state) {
    const int64_t kDepth = state.range(0);
    std::vector<Order*> queue;
    queue.reserve(static_cast<std::size_t>(kDepth));

    for (int64_t i = 0; i < kDepth; ++i) {
        Order* o = make_bid(kMidBid);
        if (!o) { state.SkipWithError("Pool exhausted in setup"); return; }
        book_->add_order(o);
        queue.push_back(o);
    }

    for (auto _ : state) {
        if (queue.empty()) { state.SkipWithError("Queue empty"); break; }
        Order* tail = queue.back();
        queue.pop_back();
        if (!tail) { state.SkipWithError("Null order in queue"); break; }

        benchmark::DoNotOptimize(tail);
        book_->cancel_order(tail);
        pool_.release(tail);
        benchmark::ClobberMemory();

        state.PauseTiming();
        Order* fresh = make_bid(kMidBid);
        if (!fresh) {
            state.SkipWithError("Pool exhausted");
            state.ResumeTiming();
            break;
        }
        book_->add_order(fresh);
        queue.push_back(fresh);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetLabel("cancels/sec @ depth=" + std::to_string(kDepth));

    for (auto* o : queue) {
        if (o) { book_->cancel_order(o); pool_.release(o); }
    }
}
BENCHMARK_REGISTER_F(LOBFixture, CancelOrder_VaryingDepth)
    ->Arg(1)->Arg(10)->Arg(100)->Arg(1000)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 3. BEST BID / ASK QUERY
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_BestBidQuery
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, BestBidQuery)(benchmark::State& state) {
    Order* b = make_bid(kMidBid);
    Order* a = make_ask(kMidAsk);
    if (!b || !a) { state.SkipWithError("Pool exhausted on setup"); return; }
    book_->add_order(b);
    book_->add_order(a);

    for (auto _ : state) {
        const PriceLevel* bid = book_->best_bid();
        const PriceLevel* ask = book_->best_ask();
        benchmark::DoNotOptimize(bid);
        benchmark::DoNotOptimize(ask);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("queries/sec");

    if (b) { book_->cancel_order(b); pool_.release(b); }
    if (a) { book_->cancel_order(a); pool_.release(a); }
}
BENCHMARK_REGISTER_F(LOBFixture, BestBidQuery)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 4. TICK-TO-INDEX (O(1) price → array index)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_TickToIndex
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, TickToIndex)(benchmark::State& state) {
    Price p = kMidBid;
    for (auto _ : state) {
        benchmark::DoNotOptimize(p);
        std::size_t idx = book_->tick_to_index(p);
        benchmark::DoNotOptimize(idx);
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
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, AvailableQuantity)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);
    std::vector<Order*> asks;
    asks.reserve(static_cast<std::size_t>(kLevels));
    for (int64_t i = 0; i < kLevels; ++i) {
        Order* a = make_ask(kMidAsk + i, 100);
        if (!a) { state.SkipWithError("Pool exhausted on setup"); return; }
        book_->add_order(a);
        asks.push_back(a);
    }

    const Price limit = kMidAsk + static_cast<Price>(kLevels) - 1;

    for (auto _ : state) {
        benchmark::DoNotOptimize(limit);
        Quantity avail = book_->available_quantity_at_or_better(Side::Buy, limit);
        benchmark::DoNotOptimize(avail);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kLevels));
    state.SetLabel("levels/iter=" + std::to_string(kLevels));

    for (auto* o : asks) { if (o) { book_->cancel_order(o); pool_.release(o); } }
}
BENCHMARK_REGISTER_F(LOBFixture, AvailableQuantity)
    ->Arg(1)->Arg(5)->Arg(20)->Arg(100)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 6. INTERLEAVED MIX (realistic producer simulation)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_LOB_InterleavedMix
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(LOBFixture, InterleavedMix)(benchmark::State& state) {
    std::vector<Order*> live_orders;
    live_orders.reserve(512);

    auto warm = [&] {
        live_orders.clear();
        book_->clear();
        pool_.reset();
        for (int i = 0; i < 256; ++i) {
            Price p = kMidBid - (i % 50);
            Order* o = make_bid(p);
            if (!o) break;
            book_->add_order(o);
            live_orders.push_back(o);
        }
    };
    warm();

    uint64_t lcg = 0xDEADBEEFCAFEBABEull;
    auto next_rand = [&]() noexcept -> uint64_t {
        lcg = lcg * 6364136223846793005ULL + 1442695040888963407ULL;
        return lcg;
    };

    for (auto _ : state) {
        if (__builtin_expect(pool_.free_count() < 100, 0)) {
            state.PauseTiming();
            warm();
            state.ResumeTiming();
        }
        const uint64_t r = next_rand();

        if ((r & 0xFF) < 179) {  // ~70% add
            Price p = kMidBid - static_cast<Price>((r >> 8) % 50);
            Order* o = make_bid(p);
            if (!o) { state.SkipWithError("Pool exhausted"); break; }
            benchmark::DoNotOptimize(o);
            book_->add_order(o);
            live_orders.push_back(o);
        } else {                  // ~30% cancel
            if (!live_orders.empty()) {
                std::size_t idx = (r >> 16) % live_orders.size();
                Order* o = live_orders[idx];
                live_orders.erase(live_orders.begin() + static_cast<std::ptrdiff_t>(idx));
                if (o) {
                    benchmark::DoNotOptimize(o);
                    book_->cancel_order(o);
                    pool_.release(o);
                }
            }
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ops/sec");
    for (auto* o : live_orders) { if (o) { book_->cancel_order(o); pool_.release(o); } }
}
BENCHMARK_REGISTER_F(LOBFixture, InterleavedMix)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_MAIN();

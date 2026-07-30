// =============================================================================
// bench_matching_engine.cpp — Google Benchmark suite for the Matching Engine
//
// Benchmark groups:
//   1. SingleOrder       — latency for one order (no match, limit rest)
//   2. FullMatch         — latency: one aggressive order ↔ one resting order
//   3. MultiLevelSweep   — sweep N resting levels in one aggressive order
//   4. OrderTypes        — Market, IOC, FOK-reject, FOK-accept latency
//   5. CancelOrder       — cancel-by-id latency (unordered_map + splice)
//   6. BurstThroughput   — amortised ns/match across a burst of N crossings
//   7. EndToEnd_Pipeline — full InboundOrderMsg → ExecutionReport round-trip
//
// Latency methodology:
//   Each benchmark measures the time from engine.submit(msg) returning to
//   when the caller can access the first ExecutionReport in the outbound queue.
//   Since submit() is synchronous (single-threaded ME), this equals the
//   true processing latency including all book operations and report posting.
//
// Throughput methodology:
//   BurstThroughput pre-loads N resting orders, then submits N aggressive
//   orders in the timed region.  SetItemsProcessed(N) gives Google Benchmark
//   the correct denominator for the "items/s" counter.
//
// All benchmarks:
//   • Report Time in nanoseconds
//   • Never allocate from the heap in the timed region
//   • Use benchmark::DoNotOptimize() on the outbound queue result
//   • Drain the outbound queue in PauseTiming (or after the loop) to prevent
//     it from filling up and artificially inflating spin-push time
// =============================================================================

#include "matching/matching_engine.hpp"
#include "spsc/spsc_queue.hpp"
#include "core/order.hpp"

#include <benchmark/benchmark.h>

#include <vector>
#include <cstdint>
#include <memory>
#include <array>

using namespace hft::matching;
using namespace hft::spsc;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark fixture
//
// Instrument config: id=1, prices 1000–3000 (tick=1 → 2001 levels)
// ─────────────────────────────────────────────────────────────────────────────
class MEFixture : public benchmark::Fixture {
public:
    static constexpr InstrumentId kInst     = 1;
    static constexpr Price        kMinPrice = 1000;
    static constexpr Price        kMaxPrice = 3000;
    static constexpr Price        kRestAsk  = 1500;   // canonical resting ask price
    static constexpr Price        kRestBid  = 1400;   // canonical resting bid price
    static constexpr Price        kAggBid   = 1600;   // aggressive bid (crosses ask)
    static constexpr Price        kAggAsk   = 1300;   // aggressive ask (crosses bid)

    void SetUp(const benchmark::State&) override {
        next_id_  = 1;
        engine_   = std::make_unique<MatchingEngine>(inbound_, outbound_);
        engine_->add_instrument(kInst, kMinPrice, kMaxPrice, /*tick=*/1);
    }

    void TearDown(const benchmark::State&) override {
        drain_outbound();
        engine_.reset();
    }

protected:
    InboundQueue                       inbound_;
    OutboundQueue                      outbound_;
    std::unique_ptr<MatchingEngine>    engine_;
    OrderId                            next_id_ = 1;

    // ── Message builders ──────────────────────────────────────────────────────

    InboundOrderMsg make_msg(InboundOrderMsg::Kind kind,
                             OrderId id,
                             Price   price,
                             Quantity qty,
                             Side    side,
                             OrderType type = OrderType::Limit) const noexcept
    {
        InboundOrderMsg msg{};
        msg.kind          = kind;
        msg.order_id      = id;
        msg.price         = price;
        msg.quantity      = qty;
        msg.instrument_id = kInst;
        msg.side          = static_cast<uint8_t>(side);
        msg.type          = static_cast<uint8_t>(type);
        return msg;
    }

    InboundOrderMsg new_limit_bid(Price p, Quantity q = 100) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, p, q,
                        Side::Buy, OrderType::Limit);
    }
    InboundOrderMsg new_limit_ask(Price p, Quantity q = 100) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, p, q,
                        Side::Sell, OrderType::Limit);
    }
    InboundOrderMsg new_market_buy(Quantity q) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, 0, q,
                        Side::Buy, OrderType::Market);
    }
    InboundOrderMsg new_market_sell(Quantity q) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, 0, q,
                        Side::Sell, OrderType::Market);
    }
    InboundOrderMsg new_ioc_bid(Price p, Quantity q) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, p, q,
                        Side::Buy, OrderType::IOC);
    }
    InboundOrderMsg new_fok_bid(Price p, Quantity q) noexcept {
        return make_msg(InboundOrderMsg::Kind::NewOrder, next_id_++, p, q,
                        Side::Buy, OrderType::FOK);
    }
    InboundOrderMsg cancel_msg(OrderId id) noexcept {
        InboundOrderMsg msg{};
        msg.kind          = InboundOrderMsg::Kind::Cancel;
        msg.order_id      = id;
        msg.instrument_id = kInst;
        return msg;
    }

    // Drain outbound queue to prevent spin-push back-pressure.
    void drain_outbound() noexcept {
        ExecutionReport r{};
        while (outbound_.try_pop(r)) {}
    }

    // Add a resting ask (no reports generated — opposite side is empty).
    OrderId add_resting_ask(Price p = kRestAsk, Quantity q = 100) noexcept {
        const OrderId id = next_id_;
        engine_->submit(new_limit_ask(p, q));
        drain_outbound();
        return id;
    }

    // Add a resting bid.
    OrderId add_resting_bid(Price p = kRestBid, Quantity q = 100) noexcept {
        const OrderId id = next_id_;
        engine_->submit(new_limit_bid(p, q));
        drain_outbound();
        return id;
    }

    bool check_and_recycle_pool(benchmark::State& state, std::size_t required = 100) noexcept {
        if (__builtin_expect(engine_->pool().free_count() < required, 0)) {
            engine_->reset_state();
        }
        if (engine_->pool().free_count() < required) {
            state.SkipWithError("Pool exhausted");
            return false;
        }
        return true;
    }
};

// =============================================================================
// 1. SINGLE ORDER LATENCY — no match, order rests in book
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_LimitOrder_Rest
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, LimitOrder_Rest)(benchmark::State& state) {
    add_resting_ask(kRestAsk);

    for (auto _ : state) {
        if (__builtin_expect(engine_->pool().free_count() < 100, 0)) {
            state.PauseTiming();
            engine_->reset_state();
            add_resting_ask(kRestAsk);
            state.ResumeTiming();
        }
        if (engine_->pool().exhausted()) { state.SkipWithError("Pool exhausted"); break; }

        InboundOrderMsg msg = new_limit_bid(kRestBid);
        benchmark::DoNotOptimize(msg);
        engine_->submit(msg);
        benchmark::ClobberMemory();

        state.PauseTiming();
        engine_->submit(cancel_msg(next_id_ - 1));
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/order (rest)");
}
BENCHMARK_REGISTER_F(MEFixture, LimitOrder_Rest)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 2. FULL MATCH LATENCY — one aggressive order ↔ one resting order
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_FullMatch_SingleOrder
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FullMatch_SingleOrder)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        benchmark::DoNotOptimize(agg);
        engine_->submit(agg);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/match");
}
BENCHMARK_REGISTER_F(MEFixture, FullMatch_SingleOrder)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_FullMatch_ExecutionPriceVerified
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FullMatch_ReportVerified)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);

        ExecutionReport r{};
        bool got = outbound_.try_pop(r);
        benchmark::DoNotOptimize(got);
        benchmark::DoNotOptimize(r);

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/submit+first_report");
}
BENCHMARK_REGISTER_F(MEFixture, FullMatch_ReportVerified)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 3. MULTI-LEVEL SWEEP — aggressive sweeps N price levels
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_MultiLevelSweep
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, MultiLevelSweep)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);

    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, static_cast<std::size_t>(kLevels) + 100)) {
            state.ResumeTiming();
            break;
        }
        for (int64_t i = 0; i < kLevels; ++i) {
            add_resting_ask(kRestAsk + static_cast<Price>(i), 100);
        }
        state.ResumeTiming();

        const Price limit = kRestAsk + static_cast<Price>(kLevels) - 1;
        InboundOrderMsg agg = new_limit_bid(limit, static_cast<Quantity>(kLevels * 100));
        benchmark::DoNotOptimize(agg);
        engine_->submit(agg);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * kLevels);
    state.SetLabel("ns/level @ N=" + std::to_string(kLevels));
}
BENCHMARK_REGISTER_F(MEFixture, MultiLevelSweep)
    ->Arg(1)->Arg(5)->Arg(10)->Arg(50)
    ->Unit(benchmark::kNanosecond);

// =============================================================================
// 4. ORDER TYPE LATENCY
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_MarketOrder_SingleLevel
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, MarketOrder_SingleLevel)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg mkt = new_market_buy(100);
        benchmark::DoNotOptimize(mkt);
        engine_->submit(mkt);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/market_order");
}
BENCHMARK_REGISTER_F(MEFixture, MarketOrder_SingleLevel)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_FOK_Reject
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FOK_Reject)(benchmark::State& state) {
    add_resting_ask(kRestAsk, 50);

    for (auto _ : state) {
        if (__builtin_expect(engine_->pool().free_count() < 100, 0)) {
            state.PauseTiming();
            engine_->reset_state();
            add_resting_ask(kRestAsk, 50);
            state.ResumeTiming();
        }
        if (engine_->pool().exhausted()) { state.SkipWithError("Pool exhausted"); break; }

        InboundOrderMsg fok = new_fok_bid(kAggBid, 200);
        benchmark::DoNotOptimize(fok);
        engine_->submit(fok);

        ExecutionReport r{};
        bool got = outbound_.try_pop(r);
        benchmark::DoNotOptimize(got);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/FOK_reject");
}
BENCHMARK_REGISTER_F(MEFixture, FOK_Reject)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_FOK_Accept
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FOK_Accept)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg fok = new_fok_bid(kAggBid, 100);
        benchmark::DoNotOptimize(fok);
        engine_->submit(fok);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/FOK_accept");
}
BENCHMARK_REGISTER_F(MEFixture, FOK_Accept)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_IOC_PartialFill
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, IOC_PartialFill)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 50);
        state.ResumeTiming();

        InboundOrderMsg ioc = new_ioc_bid(kAggBid, 100);
        benchmark::DoNotOptimize(ioc);
        engine_->submit(ioc);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/IOC_partial");
}
BENCHMARK_REGISTER_F(MEFixture, IOC_PartialFill)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 5. CANCEL ORDER LATENCY
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_CancelOrder
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, CancelOrder)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        OrderId oid = add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg cancel = cancel_msg(oid);
        benchmark::DoNotOptimize(cancel);
        engine_->submit(cancel);
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/cancel");
}
BENCHMARK_REGISTER_F(MEFixture, CancelOrder)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(0.5);

// =============================================================================
// 6. BURST THROUGHPUT
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_BurstThroughput
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, BurstThroughput)(benchmark::State& state) {
    const int64_t kBurst = state.range(0);

    std::vector<InboundOrderMsg> bids;
    bids.reserve(static_cast<std::size_t>(kBurst));
    for (int64_t i = 0; i < kBurst; ++i) {
        bids.push_back(new_limit_bid(kAggBid, 100));
    }

    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, static_cast<std::size_t>(kBurst) + 100)) {
            state.ResumeTiming();
            break;
        }
        for (int64_t i = 0; i < kBurst; ++i) {
            add_resting_ask(kRestAsk, 100);
        }
        state.ResumeTiming();

        for (int64_t i = 0; i < kBurst; ++i) {
            engine_->submit(bids[static_cast<std::size_t>(i)]);
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        for (int64_t i = 0; i < kBurst; ++i) {
            bids[static_cast<std::size_t>(i)].order_id = next_id_++;
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * kBurst);
    state.SetBytesProcessed(state.iterations() * kBurst *
                            static_cast<int64_t>(sizeof(InboundOrderMsg)));
    state.SetLabel("matches/sec @ N=" + std::to_string(kBurst));
}
BENCHMARK_REGISTER_F(MEFixture, BurstThroughput)
    ->Arg(100)->Arg(1000)->Arg(10000)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// =============================================================================
// 7. END-TO-END PIPELINE LATENCY
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_EndToEnd_LimitMatch
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_LimitMatch)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);

        ExecutionReport r{};
        const bool ok = outbound_.try_pop(r);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(r);

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/e2e (submit→first_report)");
}
BENCHMARK_REGISTER_F(MEFixture, EndToEnd_LimitMatch)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_EndToEnd_AllReports
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_AllReports)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, 100)) { state.ResumeTiming(); break; }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);

        ExecutionReport r1{}, r2{};
        bool g1 = outbound_.try_pop(r1);
        bool g2 = outbound_.try_pop(r2);
        benchmark::DoNotOptimize(g1);
        benchmark::DoNotOptimize(g2);
        benchmark::DoNotOptimize(r1);
        benchmark::DoNotOptimize(r2);

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/e2e (submit→both_reports)");
}
BENCHMARK_REGISTER_F(MEFixture, EndToEnd_AllReports)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_EndToEnd_Spread
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_Spread)(benchmark::State& state) {
    add_resting_bid(kRestBid, 1'000'000);

    for (auto _ : state) {
        state.PauseTiming();
        if (__builtin_expect(engine_->pool().free_count() < 100, 0)) {
            engine_->reset_state();
            add_resting_bid(kRestBid, 1'000'000);
        }
        if (engine_->pool().exhausted()) {
            state.SkipWithError("Pool exhausted");
            state.ResumeTiming();
            break;
        }
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);

        ExecutionReport r{};
        bool ok = outbound_.try_pop(r);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(r);

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/e2e (spread live)");
}
BENCHMARK_REGISTER_F(MEFixture, EndToEnd_Spread)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// =============================================================================
// 8. LATENCY PERCENTILE APPROXIMATION
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_Percentile_1000Samples
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, Percentile_1000Samples)(benchmark::State& state) {
    constexpr int kSamples = 1000;

    for (auto _ : state) {
        state.PauseTiming();
        if (!check_and_recycle_pool(state, kSamples + 100)) {
            state.ResumeTiming();
            break;
        }
        for (int i = 0; i < kSamples; ++i) {
            add_resting_ask(kRestAsk, 100);
        }
        state.ResumeTiming();

        for (int i = 0; i < kSamples; ++i) {
            InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
            engine_->submit(agg);
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        drain_outbound();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * kSamples);
    state.SetLabel("amortised_ns/match");
}
BENCHMARK_REGISTER_F(MEFixture, Percentile_1000Samples)
    ->Unit(benchmark::kNanosecond)
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_MAIN();

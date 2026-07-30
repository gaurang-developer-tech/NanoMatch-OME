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
};

// =============================================================================
// 1. SINGLE ORDER LATENCY — no match, order rests in book
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_LimitOrder_Rest
//
// Baseline: submit a limit order that does not cross any resting order.
// Measures: pool.acquire() + order field writes + book.add_order() + OrderIndex
// This is the lower bound for all matching engine operations.
// Expected: 20–80 ns
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, LimitOrder_Rest)(benchmark::State& state) {
    // Pre-populate the ask side so the bid never crosses.
    add_resting_ask(kRestAsk);

    for (auto _ : state) {
        // Bid well below the resting ask — guaranteed no match.
        InboundOrderMsg msg = new_limit_bid(kRestBid);
        benchmark::DoNotOptimize(msg);
        engine_->submit(msg);
        benchmark::ClobberMemory();

        // Drain and re-cycle so the book doesn't grow unboundedly and order is released back to pool.
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
//
// THE key latency benchmark:
//   Measures the full one-way latency of the matching engine's hot path:
//
//     engine.submit(aggressive_bid)
//       → dispatch()
//       → handle_new_order()
//       → build Order from pool (O(1))
//       → do_sweep<price_check>()
//       → fill_resting_order() — O(1) pointer write + counter update
//       → post_report() × 2   — push to outbound SPSC
//       → (no residual: agg fully filled)
//
//   This is the sub-microsecond target path.  Expected: 100–400 ns.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FullMatch_SingleOrder)(benchmark::State& state) {
    for (auto _ : state) {
        // Setup: add one resting ask (not in timed region)
        state.PauseTiming();
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        // Timed: aggressive bid — crosses and fully fills the resting ask.
        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        benchmark::DoNotOptimize(agg);
        engine_->submit(agg);
        benchmark::ClobberMemory();

        // Drain report queue (not timed — prevents back-pressure).
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
//
// Same as above but drains and verifies the ExecutionReport in the timed
// region to confirm the full pipeline (submit → report available to caller).
// Models the lowest achievable round-trip latency.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FullMatch_ReportVerified)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        // Timed region: submit + drain first report.
        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);

        ExecutionReport r{};
        // The report is already in the queue (synchronous ME).
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
// BM_ME_MultiLevelSweep  (parametrised by N = 1, 5, 10, 50)
//
// Pre-loads N resting asks at N consecutive price levels.
// One aggressive bid (at limit = asks[N-1].price) sweeps all N in one call.
//
// Measures incremental cost per additional level swept.
// Expected: linear in N with a very small slope (~50–100 ns per level).
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, MultiLevelSweep)(benchmark::State& state) {
    const int64_t kLevels = state.range(0);

    for (auto _ : state) {
        state.PauseTiming();
        // Pre-load N resting asks at prices kRestAsk, kRestAsk+1, …, kRestAsk+N-1
        for (int64_t i = 0; i < kLevels; ++i) {
            add_resting_ask(kRestAsk + static_cast<Price>(i), 100);
        }
        state.ResumeTiming();

        // Timed: one aggressive bid that crosses all N levels.
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
//
// FOK with insufficient liquidity — measures the pre-check path.
// No book modification; tests available_quantity_at_or_better() + reject post.
// Expected: faster than a full match (no intrusive list writes).
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FOK_Reject)(benchmark::State& state) {
    // 50 shares available at kRestAsk; FOK needs 200 → guaranteed reject.
    add_resting_ask(kRestAsk, 50);

    for (auto _ : state) {
        InboundOrderMsg fok = new_fok_bid(kAggBid, 200);
        benchmark::DoNotOptimize(fok);
        engine_->submit(fok);

        // Drain the reject report (not timed separately).
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
//
// FOK with sufficient liquidity — pre-check passes, then full sweep.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, FOK_Accept)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
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
//
// IOC where only 50 of 100 required shares are available.
// Tests: match 50 → post 3 reports (fill, partial, cancel_ack).
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, IOC_PartialFill)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
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
//
// Measures cancel_order path:
//   OrderIndex lookup (unordered_map::find) + intrusive splice + CancelAck post.
// Expected: ~100–300 ns (dominated by unordered_map::find hash computation).
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, CancelOrder)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
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
// 6. BURST THROUGHPUT — amortised ns/match across N concurrent crossings
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_BurstThroughput  (parametrised by burst size)
//
// This is the primary throughput benchmark.
//
// Setup  (outside timed region):
//   • Pre-build N InboundOrderMsg structs for crossing bids.
//   • Pre-load N resting asks at kRestAsk.
//
// Timed region:
//   • Submit all N crossing bids in a tight loop.
//   • Each bid fully matches exactly one resting ask → 2 reports.
//
// Throughput = N matches / total_time_ns → orders/second
// Latency    = total_time_ns / N → amortised ns/match
//
// Run at N = 100, 1000, 10000 to observe:
//   • L1 hot (100):   all data in L1 cache → best-case latency
//   • L2 warm (1000): working set ~100 KB → L2 cache hits
//   • L3 cold (10000):working set ~800 KB → LLC pressure
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, BurstThroughput)(benchmark::State& state) {
    const int64_t kBurst = state.range(0);

    // Pre-build the burst of crossing bids (stack-allocated for small bursts,
    // heap for large — but this is in setup, not the timed region).
    std::vector<InboundOrderMsg> bids;
    bids.reserve(static_cast<std::size_t>(kBurst));
    for (int64_t i = 0; i < kBurst; ++i) {
        bids.push_back(new_limit_bid(kAggBid, 100));
    }

    for (auto _ : state) {
        // ── Setup (not timed) ──────────────────────────────────────────────
        state.PauseTiming();
        // Load N resting asks — one per crossing bid.
        for (int64_t i = 0; i < kBurst; ++i) {
            add_resting_ask(kRestAsk, 100);
        }
        state.ResumeTiming();

        // ── Timed: burst of N crossings ───────────────────────────────────
        for (int64_t i = 0; i < kBurst; ++i) {
            engine_->submit(bids[static_cast<std::size_t>(i)]);
        }
        benchmark::ClobberMemory();

        // ── Drain (not timed) ─────────────────────────────────────────────
        state.PauseTiming();
        drain_outbound();
        // Reset bid IDs for the next iteration so they stay unique.
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
//
// The canonical HFT latency benchmark.
// Measures the COMPLETE one-way latency of the matching engine hot path:
//
//     t0 = clock_start
//     engine.submit(aggressive_bid)   ← all book ops + report push
//     outbound.try_pop(report)        ← confirm report is available
//     t1 = clock_end
//
//     Latency = t1 - t0
//
// This benchmark answers the question: "After a packet arrives and is
// decoded into an InboundOrderMsg, how long before the execution report
// is ready to be sent back on the wire?"
//
// Target: < 500 ns (p99 in production with CPU pinning + huge pages)
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_LimitMatch)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        // ── Timed: full pipeline ───────────────────────────────────────────
        InboundOrderMsg agg = new_limit_bid(kAggBid, 100);
        engine_->submit(agg);           // ← hot path start

        // Consume the first ExecutionReport (confirms pipeline completion).
        ExecutionReport r{};
        const bool ok = outbound_.try_pop(r);  // ← hot path end
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(r);
        // ─────────────────────────────────────────────────────────────────

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
//
// Same as above but waits for BOTH ExecutionReports (resting + aggressive)
// to be available before stopping the clock.
// This is the true "report flush" latency.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_AllReports)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        add_resting_ask(kRestAsk, 100);
        state.ResumeTiming();

        // Timed: submit + drain both reports.
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
        drain_outbound();   // clear any remainder
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("ns/e2e (submit→both_reports)");
}
BENCHMARK_REGISTER_F(MEFixture, EndToEnd_AllReports)
    ->Unit(benchmark::kNanosecond)
    ->MinWarmUpTime(1.0);

// ─────────────────────────────────────────────────────────────────────────────
// BM_ME_EndToEnd_Spread (bid+ask live together)
//
// Models the steady-state market-making scenario:
//   • One resting bid at kRestBid (passive market maker on the bid)
//   • One resting ask at kRestAsk (passive market maker on the ask)
//   • An aggressive bid arrives at kAggBid, sweeps the ask side
//   • The resting bid on the other side remains untouched
//
// This exercises the cursor management when one side is live while the
// other is being swept.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, EndToEnd_Spread)(benchmark::State& state) {
    // Permanent resting bid on the other side.
    add_resting_bid(kRestBid, 1'000'000);  // effectively permanent

    for (auto _ : state) {
        state.PauseTiming();
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
//
// Runs 1000 back-to-back matches and records each individual latency sample
// via a histogram counter. Google Benchmark will compute mean and stddev
// automatically; manual percentiles require a custom reporter in production.
//
// This benchmark is intended to be run with --benchmark_repetitions=10
// to get statistical confidence.
// ─────────────────────────────────────────────────────────────────────────────
BENCHMARK_DEFINE_F(MEFixture, Percentile_1000Samples)(benchmark::State& state) {
    constexpr int kSamples = 1000;

    for (auto _ : state) {
        state.PauseTiming();
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

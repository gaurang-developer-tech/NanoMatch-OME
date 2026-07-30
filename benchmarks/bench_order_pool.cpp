// =============================================================================
// bench_order_pool.cpp
// Google Benchmark — OrderPool acquire/release throughput and latency.
// =============================================================================

#include "core/order_pool.hpp"
#include <benchmark/benchmark.h>
#include <cstdint>

using namespace hft::core;

static constexpr std::size_t kBenchCapacity = 1'000'000;
using BenchPool = OrderPool<kBenchCapacity>;

// ─────────────────────────────────────────────────────────────────────────────
// BM_AcquireRelease_Single
// Tightest possible hot path: acquire one slot, release immediately.
// Measures the overhead of the intrusive freelist stack pop+push.
// ─────────────────────────────────────────────────────────────────────────────
static void BM_AcquireRelease_Single(benchmark::State& state) {
    BenchPool pool;
    for (auto _ : state) {
        Order* o = pool.acquire();
        benchmark::DoNotOptimize(o);
        pool.release(o);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("acquire+release per iteration");
}
BENCHMARK(BM_AcquireRelease_Single)
    ->Threads(1)
    ->Unit(benchmark::kNanosecond);

// ─────────────────────────────────────────────────────────────────────────────
// BM_AcquireBatch
// Acquire N orders in a tight loop (simulates a burst of new orders).
// Measures batch alloc throughput without interleaved releases.
// ─────────────────────────────────────────────────────────────────────────────
static void BM_AcquireBatch(benchmark::State& state) {
    const int batch = static_cast<int>(state.range(0));
    BenchPool pool;
    std::vector<Order*> buf(static_cast<std::size_t>(batch));

    for (auto _ : state) {
        for (int i = 0; i < batch; ++i) {
            buf[static_cast<std::size_t>(i)] = pool.acquire();
        }
        benchmark::DoNotOptimize(buf.data());
        // Release all before next iteration
        for (int i = 0; i < batch; ++i) {
            pool.release(buf[static_cast<std::size_t>(i)]);
        }
    }
    state.SetItemsProcessed(state.iterations() * batch);
    state.SetBytesProcessed(state.iterations() * batch * sizeof(Order));
}
BENCHMARK(BM_AcquireBatch)
    ->RangeMultiplier(4)->Range(1, 4096)
    ->Unit(benchmark::kNanosecond);

// ─────────────────────────────────────────────────────────────────────────────
// BM_AcquireRelease_Interleaved
// Alternating acquire/release simulating the matching engine hot loop.
// N/2 slots live at any time — tests steady-state freelist behavior.
// ─────────────────────────────────────────────────────────────────────────────
static void BM_AcquireRelease_Interleaved(benchmark::State& state) {
    const std::size_t window = static_cast<std::size_t>(state.range(0));
    BenchPool pool;

    // Pre-acquire window/2 orders to simulate pre-existing resting orders
    std::vector<Order*> live;
    live.reserve(window);
    for (std::size_t i = 0; i < window / 2; ++i) live.push_back(pool.acquire());

    std::size_t head = 0;

    for (auto _ : state) {
        Order* o = pool.acquire();
        benchmark::DoNotOptimize(o);
        live.push_back(o);

        // Release oldest resting order (simulates a fill completing)
        if (live.size() > window) {
            pool.release(live[head++]);
        }
    }

    // Cleanup
    for (std::size_t i = head; i < live.size(); ++i) pool.release(live[i]);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_AcquireRelease_Interleaved)
    ->Arg(64)->Arg(256)->Arg(1024)->Arg(8192)
    ->Unit(benchmark::kNanosecond);

// ─────────────────────────────────────────────────────────────────────────────
// BM_OrderReset
// Microbenchmark for Order::reset() in isolation.
// Reveals the cost of zeroing 64 bytes per acquire() call.
// ─────────────────────────────────────────────────────────────────────────────
static void BM_OrderReset(benchmark::State& state) {
    alignas(64) Order o{};
    for (auto _ : state) {
        benchmark::DoNotOptimize(o);
        o.reset();
        benchmark::ClobberMemory();
    }
    state.SetBytesProcessed(state.iterations() * sizeof(Order));
}
BENCHMARK(BM_OrderReset)->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();

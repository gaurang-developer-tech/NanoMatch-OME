// =============================================================================
// bench_spsc_queue.cpp
// Google Benchmark — SPSCQueue latency and throughput.
// =============================================================================

#include "spsc/spsc_queue.hpp"
#include <benchmark/benchmark.h>

#include <atomic>
#include <thread>
#include <cstdint>

using namespace hft::spsc;

// ─────────────────────────────────────────────────────────────────────────────
// BM_SPSC_SingleThread_Throughput
// Push + pop in a tight single-threaded loop.
// Measures pure overhead of the ring buffer mechanics (no contention).
// ─────────────────────────────────────────────────────────────────────────────
static void BM_SPSC_SingleThread_Throughput(benchmark::State& state) {
    SPSCQueue<uint64_t, 4096> q;
    uint64_t val = 0;
    for (auto _ : state) {
        q.try_push(val);
        q.try_pop(val);
        benchmark::DoNotOptimize(val);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("push+pop roundtrip (single thread)");
}
BENCHMARK(BM_SPSC_SingleThread_Throughput)->Unit(benchmark::kNanosecond);

// ─────────────────────────────────────────────────────────────────────────────
// BM_SPSC_CrossCore_Roundtrip
// True cross-core roundtrip: producer pushes, consumer pops, echoes back.
// Uses rdtsc-sampled latency exposed via state.SetIterationTime().
// ─────────────────────────────────────────────────────────────────────────────
static void BM_SPSC_CrossCore_Roundtrip(benchmark::State& state) {
    using Q = SPSCQueue<uint64_t, 65536>;
    Q forward, backward;

    std::atomic<bool> running{true};

    // Consumer/echo thread: pops from forward, pushes to backward
    std::jthread echo([&] {
        uint64_t v = 0;
        while (running.load(std::memory_order_acquire)) {
            if (forward.try_pop(v)) {
                while (!backward.try_push(v)) {}
            }
        }
        // Drain any remaining
        while (forward.try_pop(v)) {
            while (!backward.try_push(v)) {}
        }
    });

    uint64_t seq    = 0;
    uint64_t result = 0;

    for (auto _ : state) {
        while (!forward.try_push(seq++)) {}
        while (!backward.try_pop(result)) {}
        benchmark::DoNotOptimize(result);
    }

    running.store(false, std::memory_order_release);
    state.SetItemsProcessed(state.iterations());
    state.SetLabel("full producer→consumer→producer roundtrip");
}
BENCHMARK(BM_SPSC_CrossCore_Roundtrip)->Unit(benchmark::kNanosecond)->UseRealTime();

// ─────────────────────────────────────────────────────────────────────────────
// BM_SPSC_Batch_Throughput
// Producer pushes batches of N items; consumer drains batches.
// Measures amortized per-item cost of batch operations.
// ─────────────────────────────────────────────────────────────────────────────
static void BM_SPSC_Batch_Throughput(benchmark::State& state) {
    const std::size_t batch = static_cast<std::size_t>(state.range(0));
    SPSCQueue<uint64_t, 65536> q;

    std::vector<uint64_t> src(batch, 42u), dst(batch);

    for (auto _ : state) {
        std::size_t pushed = 0;
        while (pushed < batch) {
            pushed += q.try_push_batch(src.data() + pushed, batch - pushed);
        }
        std::size_t popped = 0;
        while (popped < batch) {
            popped += q.try_pop_batch(dst.data() + popped, batch - popped);
        }
        benchmark::DoNotOptimize(dst.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}
BENCHMARK(BM_SPSC_Batch_Throughput)
    ->RangeMultiplier(4)->Range(1, 4096)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();

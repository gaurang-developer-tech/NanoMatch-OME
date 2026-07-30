#pragma once

// =============================================================================
// matching_engine.hpp — Core Order Matching Engine
//
// Owns:
//   • DefaultOrderPool          — zero-allocation Order lifecycle management
//   • std::unordered_map<InstrumentId, OrderBook>  — per-instrument LOB
//
// References (non-owning):
//   • InboundQueue   — drains InboundOrderMsg from the Decoder thread
//   • OutboundQueue  — pushes ExecutionReport to the Outbound I/O thread
//
// Zero-allocation constraint:
//   Every operation in poll() / submit() touches ONLY:
//     • pool_.acquire() / release()   — O(1) pointer stack, no heap
//     • OrderBook operations          — O(1) pointer / vector-element writes
//     • SPSC queue push/pop           — array element writes, no heap
//   No new / malloc / std::vector growth on the hot path.
//
// Threading:
//   The MatchingEngine is designed to run on exactly ONE pinned core.
//   All methods are unsynchronised; calling from multiple threads is UB.
//
// Order type dispatch:
//
//   LIMIT   → sweep opposite side while price crosses, rest residual
//   MARKET  → sweep entire opposite side, discard residual
//   IOC     → sweep as much as possible at valid prices, cancel residual
//   FOK     → pre-check total liquidity; execute only if full qty available
//
// Execution price: always the resting (maker) order's price.
// =============================================================================

#include "core/order.hpp"
#include "core/order_pool.hpp"
#include "lob/order_book.hpp"
#include "spsc/spsc_queue.hpp"

#include <unordered_map>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <time.h>    // clock_gettime

// ── Platform spin-hint ───────────────────────────────────────────────────────
#if defined(__x86_64__) || defined(_M_X64)
#  include <immintrin.h>
#  define ME_PAUSE() _mm_pause()
#elif defined(__aarch64__)
#  define ME_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#  define ME_PAUSE() ((void)0)
#endif

namespace hft::matching {

using namespace hft::core;
using namespace hft::lob;
using namespace hft::spsc;

// Shorten the verbose enum qualifier used in every report call.
using ExKind = ExecutionReport::Kind;

// ─────────────────────────────────────────────────────────────────────────────
// MatchingEngine
// ─────────────────────────────────────────────────────────────────────────────
class MatchingEngine {
public:

    // ── Construction ──────────────────────────────────────────────────────────

    explicit MatchingEngine(InboundQueue&  inbound,
                            OutboundQueue& outbound)
        : inbound_ (inbound)
        , outbound_(outbound)
    {}

    // Non-copyable, non-movable (owns pool and book map).
    MatchingEngine(const MatchingEngine&)            = delete;
    MatchingEngine& operator=(const MatchingEngine&) = delete;
    MatchingEngine(MatchingEngine&&)                 = delete;
    MatchingEngine& operator=(MatchingEngine&&)      = delete;

    // ── Instrument Registration ───────────────────────────────────────────────
    //
    // Must be called for each instrument before any orders arrive.
    // Not thread-safe; call during startup before poll() begins.
    //
    void add_instrument(InstrumentId id,
                        Price        min_price,
                        Price        max_price,
                        Price        tick_size = 1) {
        books_.emplace(std::piecewise_construct,
                       std::forward_as_tuple(id),
                       std::forward_as_tuple(id, min_price, max_price, tick_size));
    }

    // ── poll() — main engine loop ─────────────────────────────────────────────
    //
    // Drains up to `max_batch` messages from the inbound SPSC queue and
    // processes each one.  Returns the number of messages processed.
    //
    // Designed to be called in a tight spin loop on the matching engine thread:
    //   while (running) { engine.poll(); }
    //
    // Zero heap allocation: batch is stack-allocated; all order objects
    // come from pool_.acquire().
    //
    std::size_t poll(std::size_t max_batch = 64) {
        // Stack-allocated batch — no heap, no dynamic sizing.
        InboundOrderMsg batch[64];
        const std::size_t n = inbound_.try_pop_batch(
            batch, std::min(max_batch, std::size_t{64}));

        for (std::size_t i = 0; i < n; ++i) {
            dispatch(batch[i]);
        }
        orders_processed_ += n;
        return n;
    }

    // ── submit() — direct bypass (for tests and benchmarks) ───────────────────
    //
    // Process a single message without going through the SPSC queue.
    // Useful for deterministic unit testing.
    //
    void submit(const InboundOrderMsg& msg) {
        dispatch(msg);
        ++orders_processed_;
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    [[nodiscard]] uint64_t orders_processed() const noexcept { return orders_processed_; }
    [[nodiscard]] uint64_t fills_executed()   const noexcept { return fills_executed_;   }
    [[nodiscard]] uint64_t orders_rejected()  const noexcept { return orders_rejected_;  }

    // Direct book access — for test introspection only.
    [[nodiscard]] OrderBook* book_for(InstrumentId id) noexcept {
        auto it = books_.find(id);
        return (it != books_.end()) ? &it->second : nullptr;
    }

private:

    // ─────────────────────────────────────────────────────────────────────────
    // dispatch() — route message to the correct handler
    // ─────────────────────────────────────────────────────────────────────────
    void dispatch(const InboundOrderMsg& msg) noexcept {
        switch (msg.kind) {
            case InboundOrderMsg::Kind::NewOrder: handle_new_order(msg); break;
            case InboundOrderMsg::Kind::Cancel:   handle_cancel(msg);    break;
            case InboundOrderMsg::Kind::Modify:   handle_cancel(msg);    break; // Phase 4: modify = cancel+resubmit
            default: break;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // handle_new_order()
    // ─────────────────────────────────────────────────────────────────────────
    void handle_new_order(const InboundOrderMsg& msg) noexcept {
        // Find the book for this instrument.
        OrderBook* book = book_for(msg.instrument_id);
        if (__builtin_expect(book == nullptr, 0)) {
            post_reject(msg.order_id, msg.quantity);
            ++orders_rejected_;
            return;
        }

        // Acquire a free Order slot from the pool — O(1), zero allocation.
        Order* o = pool_.acquire();
        if (__builtin_expect(o == nullptr, 0)) {
            // Pool exhausted — reject with a log-friendly reject report.
            post_reject(msg.order_id, msg.quantity);
            ++orders_rejected_;
            return;
        }

        // Populate Order fields from the inbound message.
        o->order_id      = msg.order_id;
        o->price         = msg.price;
        o->quantity      = msg.quantity;
        o->orig_quantity = msg.quantity;
        o->instrument_id = msg.instrument_id;
        o->client_id     = msg.client_id;
        o->side          = static_cast<Side>(msg.side);
        o->type          = static_cast<OrderType>(msg.type);
        o->status        = OrderStatus::New;
        o->timestamp_ns  = now_ns();
        o->next          = nullptr;
        o->prev          = nullptr;

        // Route to the correct matching strategy.
        switch (o->type) {
            case OrderType::Limit:  match_limit (*o, *book); break;
            case OrderType::Market: match_market(*o, *book); break;
            case OrderType::IOC:    match_ioc   (*o, *book); break;
            case OrderType::FOK:    match_fok   (*o, *book); break;
            default:
                post_reject(o->order_id, o->quantity);
                pool_.release(o);
                ++orders_rejected_;
                break;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // handle_cancel()
    // ─────────────────────────────────────────────────────────────────────────
    void handle_cancel(const InboundOrderMsg& msg) noexcept {
        OrderBook* book = book_for(msg.instrument_id);
        if (book == nullptr) return;

        // Find the Order pointer in the book's OrderIndex.
        Order* o = book->find_order(msg.order_id);
        if (o == nullptr) return;  // already filled / already cancelled

        const Quantity leaves = o->quantity;
        book->cancel_order(o);

        post_cancel_ack(o->order_id, leaves);
        pool_.release(o);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // do_sweep<PriceCheckFn>()
    //
    // Generic sweep loop — the heart of price-time priority matching.
    //
    // For each iteration:
    //   1. Peek at the best level on the opposite side.
    //   2. Run `price_ok(best->price)` — stops if the aggressive order can no
    //      longer cross (limit orders) or always continues (market orders).
    //   3. Take the HEAD order from that level (O(1)).
    //   4. Compute fill_qty = min(resting_qty, aggressive_remaining_qty).
    //   5. Call book.fill_resting_order() to atomically update the resting
    //      order, the PriceLevel aggregate, the OrderIndex, and the cursor.
    //   6. Post ExecutionReports for both parties.
    //   7. Release fully-filled resting orders back to the pool.
    //   8. Continue until aggressive qty == 0 or no crossable levels remain.
    //
    // Template parameter PriceCheckFn: (Price) → bool
    //   Limit buy:  [P](Price lp) { return lp <= P; }
    //   Limit sell: [P](Price lp) { return lp >= P; }
    //   Market:     [  ](Price  ) { return true;     }
    //
    // ─────────────────────────────────────────────────────────────────────────
    template<typename PriceCheckFn>
    void do_sweep(Order& agg, OrderBook& book, PriceCheckFn price_ok) noexcept {
        while (agg.quantity > 0) {
            // Peek at best level on opposite side.
            PriceLevel* best = agg.is_buy() ? book.best_ask() : book.best_bid();
            if (best == nullptr) break;              // book is empty
            if (!price_ok(best->price))  break;      // no more crossable levels

            // The head of the best level is always non-null (level is non-empty).
            Order* rest = best->head;
            assert(rest != nullptr);

            const Quantity fill_qty   = std::min(rest->quantity, agg.quantity);
            const Price    exec_price = rest->price;  // maker's price

            // ── Update aggressive order ──────────────────────────────────────
            agg.quantity -= fill_qty;

            // ── Update resting order + book state ─────────────────────────────
            // fill_resting_order does: level total_qty--, order qty--,
            // OrderIndex erase + cursor advance (if fully filled).
            const bool rest_done = book.fill_resting_order(rest, fill_qty);
            ++fills_executed_;

            // ── Post ExecutionReport for the RESTING order ────────────────────
            post_report(
                rest_done ? ExKind::Fill : ExKind::PartialFill,
                rest->order_id,
                agg.order_id,
                exec_price,
                fill_qty,
                rest->quantity   // leaves_qty: 0 if fully filled
            );

            // ── Post ExecutionReport for the AGGRESSIVE order ─────────────────
            post_report(
                agg.quantity == 0 ? ExKind::Fill : ExKind::PartialFill,
                agg.order_id,
                rest->order_id,
                exec_price,
                fill_qty,
                agg.quantity     // leaves_qty on the aggressive side
            );

            // ── Recycle fully-filled resting order ────────────────────────────
            if (rest_done) {
                pool_.release(rest);
            }
            // If rest was only partially filled, agg.quantity must now be 0,
            // and the outer while condition exits on the next check.
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // match_limit()
    //
    // Aggressive limit order:
    //   • Sweep while opposite side's best price crosses agg's limit price.
    //   • Rest any unfilled residual in the book.
    // ─────────────────────────────────────────────────────────────────────────
    void match_limit(Order& agg, OrderBook& book) noexcept {
        if (agg.is_buy()) {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price <= p;   // ask price must be at or below our bid
            });
        } else {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price >= p;   // bid price must be at or above our ask
            });
        }

        if (agg.quantity > 0) {
            // Residual rests in the book — add to tail of its price level.
            book.add_order(&agg);
        } else {
            // Fully matched — return the pool slot immediately.
            pool_.release(&agg);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // match_market()
    //
    // Market order: no price constraint — sweep the entire opposite side.
    // Any unfilled residual is silently discarded (never rests).
    // ─────────────────────────────────────────────────────────────────────────
    void match_market(Order& agg, OrderBook& book) noexcept {
        do_sweep(agg, book, [](Price) { return true; });

        if (agg.quantity > 0) {
            // Insufficient liquidity — generate a partial cancel for the remainder.
            post_cancel_ack(agg.order_id, agg.quantity);
        }
        // Market orders never rest in the book.
        pool_.release(&agg);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // match_ioc()
    //
    // Immediate-or-Cancel: match as much as possible at valid prices, then
    // immediately cancel any unfilled residual.  Never rests in the book.
    // ─────────────────────────────────────────────────────────────────────────
    void match_ioc(Order& agg, OrderBook& book) noexcept {
        if (agg.is_buy()) {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price <= p;
            });
        } else {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price >= p;
            });
        }

        if (agg.quantity > 0) {
            // Cancel unfilled residual — post a CancelAck for the remainder.
            post_cancel_ack(agg.order_id, agg.quantity);
        }
        // IOC never rests.
        pool_.release(&agg);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // match_fok()
    //
    // Fill-or-Kill: BEFORE touching the book, verify that the entire quantity
    // can be filled at valid prices.  If not, reject immediately without
    // modifying any order, level, or cursor.
    //
    // FOK feasibility check is O(matched_levels) — a sequential sum over
    // PriceLevel::total_qty which is maintained in O(1) per fill.
    // ─────────────────────────────────────────────────────────────────────────
    void match_fok(Order& agg, OrderBook& book) noexcept {
        // ── Feasibility check — zero book modification ─────────────────────
        const Quantity avail = book.available_quantity_at_or_better(
            agg.side, agg.price);

        if (avail < agg.quantity) {
            // Insufficient liquidity — reject without touching the book.
            post_reject(agg.order_id, agg.quantity);
            pool_.release(&agg);
            ++orders_rejected_;
            return;
        }

        // ── Full fill guaranteed — sweep with price check ──────────────────
        // (Same price logic as limit order — FOK respects the limit price.)
        if (agg.is_buy()) {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price <= p;
            });
        } else {
            do_sweep(agg, book, [p = agg.price](Price lvl_price) {
                return lvl_price >= p;
            });
        }

        // After a successful FOK, agg.quantity must be 0.
        assert(agg.quantity == 0 &&
               "FOK: feasibility check passed but order not fully filled — logic error");

        pool_.release(&agg);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Report helpers
    // ─────────────────────────────────────────────────────────────────────────

    void post_report(ExKind kind,
                     OrderId order_id,
                     OrderId counterpart_id,
                     Price   exec_price,
                     Quantity exec_qty,
                     Quantity leaves_qty) noexcept {
        ExecutionReport r{};
        r.kind                 = kind;
        r.order_id             = order_id;
        r.counterpart_order_id = counterpart_id;
        r.exec_price           = exec_price;
        r.exec_quantity        = exec_qty;
        r.leaves_quantity      = leaves_qty;
        r.timestamp_ns         = now_ns();

        // Spin until space in outbound queue.
        // In production, the outbound I/O thread drains this fast enough
        // that contention is rare.  Phase 7 will add overflow monitoring.
        while (!outbound_.try_push(r)) { ME_PAUSE(); }
    }

    void post_reject(OrderId order_id, Quantity original_qty) noexcept {
        ExecutionReport r{};
        r.kind                 = ExKind::Reject;
        r.order_id             = order_id;
        r.counterpart_order_id = 0;
        r.exec_price           = 0;
        r.exec_quantity        = 0;
        r.leaves_quantity      = original_qty;
        r.timestamp_ns         = now_ns();
        while (!outbound_.try_push(r)) { ME_PAUSE(); }
    }

    void post_cancel_ack(OrderId order_id, Quantity leaves_qty) noexcept {
        ExecutionReport r{};
        r.kind                 = ExKind::CancelAck;
        r.order_id             = order_id;
        r.counterpart_order_id = 0;
        r.exec_price           = 0;
        r.exec_quantity        = 0;
        r.leaves_quantity      = leaves_qty;
        r.timestamp_ns         = now_ns();
        while (!outbound_.try_push(r)) { ME_PAUSE(); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utility
    // ─────────────────────────────────────────────────────────────────────────

    [[nodiscard]] static uint64_t now_ns() noexcept {
        struct timespec ts{};
        ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL
             + static_cast<uint64_t>(ts.tv_nsec);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Data members
    // ─────────────────────────────────────────────────────────────────────────

    DefaultOrderPool                             pool_;         // Order memory pool
    std::unordered_map<InstrumentId, OrderBook>  books_;        // Per-instrument LOB
    InboundQueue&                                inbound_;      // From Decoder thread
    OutboundQueue&                               outbound_;     // To Outbound I/O thread

    // Stats — written only by the matching engine thread (no atomics needed).
    uint64_t orders_processed_ = 0;
    uint64_t fills_executed_   = 0;
    uint64_t orders_rejected_  = 0;
};

} // namespace hft::matching

#pragma once

// =============================================================================
// order_book.hpp — Cache-Friendly Limit Order Book (LOB)
//
// Architecture:
//   • Two flat std::vector<PriceLevel> arrays (bids, asks) — contiguous memory,
//     prefetcher-friendly, O(1) price→index via arithmetic (no tree traversal).
//   • Each PriceLevel owns an intrusive doubly-linked queue of resting Orders,
//     using the Order::prev / Order::next pointers already embedded in the
//     Order struct — zero external metadata, zero extra allocation.
//   • Monotonic best_bid_idx_ / best_ask_idx_ cursors — O(1) top-of-book
//     access; only scan inward (amortised O(1)) when a level is drained.
//   • OrderIndex (std::unordered_map<OrderId, Order*>) provides O(1)
//     cancel-by-id without searching the book.
//
// Memory layout of the arrays (example, 5 tick levels):
//
//   index:    0      1      2      3      4
//   price: min   min+1  min+2  min+3  min+4
//
//   Bids: worst ──────────────────────► best  (best_bid_idx_ = highest filled)
//   Asks: best  ◄──────────────────────  worst (best_ask_idx_ = lowest filled)
//
// =============================================================================

#include "core/order.hpp"

#include <vector>
#include <unordered_map>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>

namespace hft::lob {

using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// PriceLevel — 32 bytes, half a cache line
//
// Two PriceLevel objects fit in a single 64-byte cache line, so a sequential
// sweep of adjacent levels loads each pair in one cache miss.
//
// The intrusive queue is a doubly-linked list threaded through Order::next and
// Order::prev.  All four queue operations are O(1) pointer writes:
//
//   enqueue_tail   → append at tail  (new order arrives)
//   dequeue_head   → remove head     (best price matched)
//   remove_any     → splice-out      (cancel arbitrary resting order)
//   peek_head      → read head       (top-of-book quantity / order)
//
// ─────────────────────────────────────────────────────────────────────────────
struct PriceLevel {
    Order*   head        = nullptr;  //  8B  oldest order in queue (first to fill)
    Order*   tail        = nullptr;  //  8B  newest order in queue (append here)
    int64_t  price       = 0;        //  8B  tick price this level represents
    uint32_t total_qty   = 0;        //  4B  aggregate resting quantity
    uint32_t order_count = 0;        //  4B  number of resting orders
    // ── Total: 8+8+8+4+4 = 32 bytes ✓ ─────────────────────────────────────
};

// Strictly enforce the layout — if a field is added or reordered this fires.
static_assert(sizeof(PriceLevel) == 32,
    "PriceLevel must be exactly 32 bytes (half a cache line). "
    "Adjust fields or add explicit padding.");

// ─────────────────────────────────────────────────────────────────────────────
// PriceLevel inline helpers (free functions to keep the struct a plain POD)
// ─────────────────────────────────────────────────────────────────────────────

// Returns true if no orders rest at this level.
[[nodiscard]] inline bool level_empty(const PriceLevel& lvl) noexcept {
    return lvl.head == nullptr;
}

// Append `o` to the tail of `lvl`'s intrusive queue.  O(1).
// The order's prev / next pointers are set; the level counters are updated.
inline void level_enqueue_tail(PriceLevel& lvl, Order* o) noexcept {
    if (__builtin_expect(o == nullptr, 0)) return;
    assert(o != nullptr);
    o->next = nullptr;
    o->prev = lvl.tail;

    if (lvl.tail != nullptr) {
        lvl.tail->next = o;   // link previous tail → new node
    } else {
        lvl.head = o;         // list was empty; new node is also the head
    }

    lvl.tail          = o;
    lvl.total_qty    += o->quantity;
    lvl.order_count  += 1;
}

// Remove the head (oldest / highest-priority) order.  O(1).
// Returns the dequeued Order* or nullptr if the level is empty.
[[nodiscard]] inline Order* level_dequeue_head(PriceLevel& lvl) noexcept {
    Order* o = lvl.head;
    if (o == nullptr) return nullptr;

    lvl.head = o->next;
    if (lvl.head != nullptr) {
        lvl.head->prev = nullptr;
    } else {
        lvl.tail = nullptr;   // list is now empty
    }

    assert(lvl.total_qty  >= o->quantity);
    assert(lvl.order_count >= 1);
    if (lvl.total_qty >= o->quantity) lvl.total_qty -= o->quantity;
    else lvl.total_qty = 0;
    if (lvl.order_count >= 1) lvl.order_count -= 1;
    else lvl.order_count = 0;

    o->next = nullptr;
    o->prev = nullptr;
    return o;
}

// Splice `o` out of `lvl`'s intrusive queue without searching.  O(1).
// Relies on o->prev and o->next being maintained correctly by the book.
inline void level_remove(PriceLevel& lvl, Order* o) noexcept {
    if (__builtin_expect(o == nullptr, 0)) return;
    assert(o != nullptr);
    assert(lvl.order_count >= 1);
    assert(lvl.total_qty   >= o->quantity);

    // Re-link neighbours
    if (o->prev != nullptr) {
        o->prev->next = o->next;
    } else {
        lvl.head = o->next;   // o was the head
    }

    if (o->next != nullptr) {
        o->next->prev = o->prev;
    } else {
        lvl.tail = o->prev;   // o was the tail
    }

    if (lvl.total_qty >= o->quantity) lvl.total_qty -= o->quantity;
    else lvl.total_qty = 0;
    if (lvl.order_count >= 1) lvl.order_count -= 1;
    else lvl.order_count = 0;

    o->next = nullptr;
    o->prev = nullptr;
}

// Reduce `o`'s quantity by `delta` and update level aggregate.  O(1).
inline void level_reduce_qty(PriceLevel& lvl, Order* o, Quantity delta) noexcept {
    if (__builtin_expect(o == nullptr, 0)) return;
    assert(delta <= o->quantity);
    assert(lvl.total_qty >= delta);
    if (o->quantity >= delta) o->quantity -= delta;
    else o->quantity = 0;
    if (lvl.total_qty >= delta) lvl.total_qty -= delta;
    else lvl.total_qty = 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// OrderIndex — O(1) order-id → Order* lookup for cancellations
//
// Wraps std::unordered_map with a minimal interface.  In Phase 7 profiling
// this can be swapped for a flat robin-hood or absl map without changing
// OrderBook.
// ─────────────────────────────────────────────────────────────────────────────
class OrderIndex {
public:
    explicit OrderIndex(std::size_t reserve = 4096) {
        map_.reserve(reserve);
    }

    // Register a newly-resting order.
    void insert(Order* o) {
        if (__builtin_expect(o == nullptr, 0)) return;
        assert(o != nullptr);
        map_.emplace(o->order_id, o);
    }

    // Deregister an order (filled, cancelled, or expired).
    void erase(OrderId id) {
        map_.erase(id);
    }

    // Look up an order by id.  Returns nullptr if not found.
    [[nodiscard]] Order* find(OrderId id) const noexcept {
        auto it = map_.find(id);
        return (it != map_.end()) ? it->second : nullptr;
    }

    [[nodiscard]] bool     contains(OrderId id) const noexcept { return map_.count(id) > 0; }
    [[nodiscard]] std::size_t size()            const noexcept { return map_.size(); }
    [[nodiscard]] bool        empty()           const noexcept { return map_.empty(); }

    void clear() { map_.clear(); }

private:
    std::unordered_map<OrderId, Order*> map_;
};

// ─────────────────────────────────────────────────────────────────────────────
// OrderBook — per-instrument Limit Order Book
//
// Holds bid_levels_ and ask_levels_ as contiguous flat arrays.
//
// Price-to-index mapping (O(1) arithmetic):
//   index = (price - min_price_) / tick_size_
//
// The caller is responsible for ensuring prices are valid tick multiples
// within [min_price_, max_price_].  In a production exchange the decoder
// validates this before the order reaches the book.
//
// Thread safety: NONE.  The OrderBook is owned exclusively by the single
// Matching Engine thread.  All access must be from that thread.
// ─────────────────────────────────────────────────────────────────────────────
class OrderBook {
public:

    // ── Construction ──────────────────────────────────────────────────────────
    //
    // instrument_id — which instrument this book tracks
    // min_price     — lowest valid price tick (inclusive)
    // max_price     — highest valid price tick (inclusive)
    // tick_size     — smallest price increment (default: 1 tick)
    //
    // Memory: allocates 2 × num_levels × 32 bytes upfront.
    // Example: 10,000 levels → 640 KB per book.
    //
    OrderBook(InstrumentId instrument_id,
              Price        min_price,
              Price        max_price,
              Price        tick_size = 1)
        : instrument_id_(instrument_id)
        , min_price_(min_price)
        , max_price_(max_price)
        , tick_size_(tick_size)
    {
        assert(min_price < max_price);
        assert(tick_size > 0);
        assert((max_price - min_price) % tick_size == 0);

        const std::size_t n = num_levels_for(min_price, max_price, tick_size);
        bid_levels_.resize(n);
        ask_levels_.resize(n);

        // Pre-fill the price field in each level for O(1) validation in debug.
        for (std::size_t i = 0; i < n; ++i) {
            const Price p = min_price + static_cast<Price>(i) * tick_size;
            bid_levels_[i].price = p;
            ask_levels_[i].price = p;
        }

        best_bid_idx_ = kNoBest;
        best_ask_idx_ = kNoBest;
    }

    // Non-copyable (owns large vector storage).
    OrderBook(const OrderBook&)            = delete;
    OrderBook& operator=(const OrderBook&) = delete;
    OrderBook(OrderBook&&)                 = default;
    OrderBook& operator=(OrderBook&&)      = default;

    // ── clear() ───────────────────────────────────────────────────────────────
    // Reset all price levels and index to an empty state without releasing memory.
    void clear() noexcept {
        for (auto& lvl : bid_levels_) {
            lvl.head = nullptr; lvl.tail = nullptr; lvl.total_qty = 0; lvl.order_count = 0;
        }
        for (auto& lvl : ask_levels_) {
            lvl.head = nullptr; lvl.tail = nullptr; lvl.total_qty = 0; lvl.order_count = 0;
        }
        best_bid_idx_ = kNoBest;
        best_ask_idx_ = kNoBest;
        order_index_.clear();
    }

    // ── add_order() ───────────────────────────────────────────────────────────
    //
    // Insert a new resting order into the book at its price level.
    // The order is appended to the TAIL of the level's queue (FIFO/price-time
    // priority: orders at the same price compete in arrival order).
    //
    // Also registers the order in the OrderIndex for O(1) cancel-by-id.
    //
    // Complexity: O(1)
    //
    void add_order(Order* o) {
        if (__builtin_expect(o == nullptr, 0)) return;
        assert(o != nullptr);
        assert(o->is_active());
        assert(is_valid_price(o->price));
        if (!is_valid_price(o->price)) return;

        const std::size_t idx = tick_to_index(o->price);
        PriceLevel& lvl = side_levels(o->side)[idx];

        level_enqueue_tail(lvl, o);
        order_index_.insert(o);

        // Update best cursor
        if (o->is_buy()) {
            if (best_bid_idx_ == kNoBest || idx > best_bid_idx_) {
                best_bid_idx_ = idx;
            }
        } else {
            if (best_ask_idx_ == kNoBest || idx < best_ask_idx_) {
                best_ask_idx_ = idx;
            }
        }
    }

    // ── cancel_order(OrderId) ─────────────────────────────────────────────────
    //
    // Cancel a resting order by its ID.  Looks up the Order* in the index,
    // performs an O(1) intrusive splice-out, then deregisters it.
    //
    // Returns true if the order was found and cancelled.
    // Returns false if the order does not exist (already filled/cancelled).
    //
    // Complexity: O(1) — hash map lookup + pointer splice
    //
    bool cancel_order(OrderId id) {
        Order* o = order_index_.find(id);
        if (o == nullptr) return false;

        cancel_order(o);
        return true;
    }

    // ── cancel_order(Order*) ──────────────────────────────────────────────────
    //
    // Cancel when the caller already holds the Order pointer.
    //
    void cancel_order(Order* o) {
        if (__builtin_expect(o == nullptr, 0)) return;
        assert(o != nullptr);
        assert(is_valid_price(o->price));
        if (!is_valid_price(o->price)) return;

        const std::size_t idx = tick_to_index(o->price);
        PriceLevel& lvl = side_levels(o->side)[idx];

        level_remove(lvl, o);
        order_index_.erase(o->order_id);

        o->status = OrderStatus::Cancelled;

        // Advance best cursor if this level just became empty
        if (level_empty(lvl)) {
            advance_best_cursor(o->side, idx);
        }
    }

    // ── reduce_order_qty() ────────────────────────────────────────────────────
    //
    // Reduce the resting quantity of an order (partial fill or amend-down).
    // If new_qty == 0 the order is fully cancelled.
    //
    // Complexity: O(1)
    //
    void reduce_order_qty(Order* o, Quantity new_qty) {
        if (__builtin_expect(o == nullptr, 0)) return;
        assert(o != nullptr);
        assert(new_qty <= o->quantity);
        if (!is_valid_price(o->price)) return;

        const std::size_t idx = tick_to_index(o->price);
        PriceLevel& lvl = side_levels(o->side)[idx];

        const Quantity delta = o->quantity - new_qty;
        level_reduce_qty(lvl, o, delta);

        if (new_qty == 0) {
            level_remove(lvl, o);
            order_index_.erase(o->order_id);
            o->status = OrderStatus::Cancelled;
            if (level_empty(lvl)) {
                advance_best_cursor(o->side, idx);
            }
        }
    }

    // ── fill_resting_order() ───────────────────────────────────────────────────
    //
    // The core fill primitive called by the Matching Engine for every trade.
    //
    // Executes fill_qty against a resting order that is already in the book.
    // Simultaneously updates:
    //   • the order's own quantity and status
    //   • the PriceLevel's total_qty aggregate
    //   • the OrderIndex (erase on full fill)
    //   • the best cursor (advance on level drain)
    //
    // Returns true  → order is FULLY filled and has been removed from the book.
    //                 Caller MUST release the Order* back to the OrderPool.
    // Returns false → order is PARTIALLY filled and remains at the head of its
    //                 price level.  Caller must NOT release it.
    //
    // Complexity: O(1)
    //
    [[nodiscard]] bool fill_resting_order(Order* rest, Quantity fill_qty) noexcept {
        if (__builtin_expect(rest == nullptr, 0)) return true;
        assert(rest      != nullptr);
        assert(fill_qty  >  0);
        assert(fill_qty  <= rest->quantity);

        if (!is_valid_price(rest->price)) return true;
        const std::size_t idx = tick_to_index(rest->price);
        PriceLevel& lvl = side_levels(rest->side)[idx];

        assert(lvl.total_qty >= fill_qty);
        lvl.total_qty  -= fill_qty;
        rest->quantity -= fill_qty;

        if (rest->quantity == 0) {
            // Fully filled — splice out of intrusive list and deregister.
            level_remove(lvl, rest);
            order_index_.erase(rest->order_id);
            rest->status = OrderStatus::Filled;
            if (level_empty(lvl)) {
                advance_best_cursor(rest->side, idx);
            }
            return true;
        } else {
            // Partially filled — order stays at the head of its queue.
            rest->status = OrderStatus::Partial;
            return false;
        }
    }

    // ── available_quantity_at_or_better() ─────────────────────────────────────
    //
    // Sum the total resting quantity on the opposite side at prices that an
    // aggressive order at `limit_price` could match against.
    //
    // For an aggressive BUY  at limit_price: sums all ask levels ≤ limit_price.
    // For an aggressive SELL at limit_price: sums all bid levels ≥ limit_price.
    //
    // Used by the FOK feasibility check — does NOT modify the book.
    // Pass kInvalidPrice / 0 for market orders (no price limit).
    //
    // Complexity: O(matched_levels) — amortised O(1) in a tight market.
    //
    [[nodiscard]] Quantity
    available_quantity_at_or_better(Side agg_side, Price limit_price) const noexcept {
        Quantity total = 0;

        if (agg_side == Side::Buy) {
            // Sum ask levels where ask.price <= limit_price
            if (best_ask_idx_ == kNoBest) return 0;
            const std::size_t to_idx =
                (limit_price >= max_price_) ? (ask_levels_.size() - 1)
              : (limit_price <  min_price_) ? std::size_t{0}
              : tick_to_index(limit_price);
            if (best_ask_idx_ > to_idx) return 0;
            for (std::size_t i = best_ask_idx_; i <= to_idx; ++i) {
                total += ask_levels_[i].total_qty;
            }
        } else {
            // Sum bid levels where bid.price >= limit_price
            if (best_bid_idx_ == kNoBest) return 0;
            const std::size_t from_idx =
                (limit_price <= min_price_) ? std::size_t{0}
              : (limit_price >  max_price_) ? bid_levels_.size()
              : tick_to_index(limit_price);
            if (from_idx > best_bid_idx_) return 0;
            for (std::size_t i = from_idx; i <= best_bid_idx_; ++i) {
                total += bid_levels_[i].total_qty;
            }
        }
        return total;
    }

    // ── Matching helpers — dequeue from best level ────────────────────────────
    //
    // Pop the head order from the best bid level.  Called by the Matching
    // Engine when filling an aggressive ask against the resting bid book.
    // Returns nullptr if the bid side is empty.
    //
    // Complexity: O(1) — head dequeue + cursor check
    //
    Order* pop_best_bid_head() {
        if (best_bid_idx_ == kNoBest) return nullptr;
        PriceLevel& lvl = bid_levels_[best_bid_idx_];
        Order* o = level_dequeue_head(lvl);
        if (o != nullptr) {
            order_index_.erase(o->order_id);
            if (level_empty(lvl)) {
                advance_best_cursor(Side::Buy, best_bid_idx_);
            }
        }
        return o;
    }

    Order* pop_best_ask_head() {
        if (best_ask_idx_ == kNoBest) return nullptr;
        PriceLevel& lvl = ask_levels_[best_ask_idx_];
        Order* o = level_dequeue_head(lvl);
        if (o != nullptr) {
            order_index_.erase(o->order_id);
            if (level_empty(lvl)) {
                advance_best_cursor(Side::Sell, best_ask_idx_);
            }
        }
        return o;
    }

    // ── Top-of-book queries ───────────────────────────────────────────────────

    // Pointer to the best bid PriceLevel, or nullptr if no bids.
    [[nodiscard]] PriceLevel* best_bid() noexcept {
        return (best_bid_idx_ == kNoBest) ? nullptr : &bid_levels_[best_bid_idx_];
    }
    [[nodiscard]] const PriceLevel* best_bid() const noexcept {
        return (best_bid_idx_ == kNoBest) ? nullptr : &bid_levels_[best_bid_idx_];
    }

    // Pointer to the best ask PriceLevel, or nullptr if no asks.
    [[nodiscard]] PriceLevel* best_ask() noexcept {
        return (best_ask_idx_ == kNoBest) ? nullptr : &ask_levels_[best_ask_idx_];
    }
    [[nodiscard]] const PriceLevel* best_ask() const noexcept {
        return (best_ask_idx_ == kNoBest) ? nullptr : &ask_levels_[best_ask_idx_];
    }

    // Best prices (kInvalidPrice if side is empty)
    [[nodiscard]] Price best_bid_price() const noexcept {
        const auto* lvl = best_bid();
        return lvl ? lvl->price : kInvalidPrice;
    }
    [[nodiscard]] Price best_ask_price() const noexcept {
        const auto* lvl = best_ask();
        return lvl ? lvl->price : kInvalidPrice;
    }

    // ── Spread / market-data helpers ──────────────────────────────────────────

    [[nodiscard]] bool has_bids()  const noexcept { return best_bid_idx_ != kNoBest; }
    [[nodiscard]] bool has_asks()  const noexcept { return best_ask_idx_ != kNoBest; }
    [[nodiscard]] bool is_empty()  const noexcept { return !has_bids() && !has_asks(); }

    // ── Level access ──────────────────────────────────────────────────────────

    [[nodiscard]] PriceLevel& bid_level_at(Price p) noexcept {
        return bid_levels_[tick_to_index(p)];
    }
    [[nodiscard]] PriceLevel& ask_level_at(Price p) noexcept {
        return ask_levels_[tick_to_index(p)];
    }
    [[nodiscard]] const PriceLevel& bid_level_at(Price p) const noexcept {
        return bid_levels_[tick_to_index(p)];
    }
    [[nodiscard]] const PriceLevel& ask_level_at(Price p) const noexcept {
        return ask_levels_[tick_to_index(p)];
    }

    // ── Sweep spans for the matching loop ─────────────────────────────────────
    //
    // Returns a std::span over levels starting from the best ask upward.
    // The Matching Engine iterates this when sweeping for an aggressive bid.
    //
    [[nodiscard]] std::span<PriceLevel> ask_sweep_span() noexcept {
        if (best_ask_idx_ == kNoBest) return {};
        return std::span<PriceLevel>{ask_levels_}.subspan(best_ask_idx_);
    }

    [[nodiscard]] std::span<PriceLevel> bid_sweep_span() noexcept {
        if (best_bid_idx_ == kNoBest) return {};
        // Reverse view isn't trivial with span; return first N levels up to best
        return std::span<PriceLevel>{bid_levels_}.first(best_bid_idx_ + 1);
    }

    // ── OrderIndex access ─────────────────────────────────────────────────────

    [[nodiscard]] Order* find_order(OrderId id) const noexcept {
        return order_index_.find(id);
    }
    [[nodiscard]] bool   has_order(OrderId id) const noexcept {
        return order_index_.contains(id);
    }
    [[nodiscard]] std::size_t live_order_count() const noexcept {
        return order_index_.size();
    }

    // ── Metadata ──────────────────────────────────────────────────────────────

    [[nodiscard]] InstrumentId instrument_id() const noexcept { return instrument_id_; }
    [[nodiscard]] Price        min_price()     const noexcept { return min_price_;     }
    [[nodiscard]] Price        max_price()     const noexcept { return max_price_;     }
    [[nodiscard]] Price        tick_size()     const noexcept { return tick_size_;     }
    [[nodiscard]] std::size_t  num_levels()    const noexcept { return bid_levels_.size(); }

    // ── tick_to_index() ───────────────────────────────────────────────────────
    //
    // Convert a price tick to a flat-array index.  O(1) arithmetic.
    //
    // Design: subtraction + division by tick_size replaces any tree traversal.
    // For tick_size == 1 the compiler optimises the division away entirely.
    //
    [[nodiscard]] std::size_t tick_to_index(Price p) const noexcept {
        assert(is_valid_price(p));
        return static_cast<std::size_t>((p - min_price_) / tick_size_);
    }

    [[nodiscard]] Price index_to_price(std::size_t idx) const noexcept {
        return min_price_ + static_cast<Price>(idx) * tick_size_;
    }

    [[nodiscard]] bool is_valid_price(Price p) const noexcept {
        return p >= min_price_
            && p <= max_price_
            && (p - min_price_) % tick_size_ == 0;
    }

private:
    // ── Internal helpers ──────────────────────────────────────────────────────

    static constexpr std::size_t kNoBest =
        std::numeric_limits<std::size_t>::max();

    static std::size_t num_levels_for(Price mn, Price mx, Price tick) noexcept {
        return static_cast<std::size_t>((mx - mn) / tick) + 1;
    }

    // Return the correct level array for a given side.
    [[nodiscard]] std::vector<PriceLevel>& side_levels(Side s) noexcept {
        return (s == Side::Buy) ? bid_levels_ : ask_levels_;
    }
    [[nodiscard]] const std::vector<PriceLevel>& side_levels(Side s) const noexcept {
        return (s == Side::Buy) ? bid_levels_ : ask_levels_;
    }

    // Advance the best cursor after a level has been drained.
    //
    // Bid cursor: scan downward (toward lower prices = lower indices).
    // Ask cursor: scan upward  (toward higher prices = higher indices).
    //
    // Amortised O(1): in a normal market the gap between consecutive active
    // levels is small (1–5 ticks).  Worst case is O(num_levels) only if the
    // entire book is empty — which is also a correct linear scan.
    //
    void advance_best_cursor(Side side, std::size_t drained_idx) noexcept {
        if (side == Side::Buy) {
            // Only move the cursor if it was pointing at the drained level.
            if (best_bid_idx_ != drained_idx) return;

            if (drained_idx == 0) {
                best_bid_idx_ = kNoBest;
                return;
            }
            // Scan downward
            std::size_t i = drained_idx - 1;
            while (true) {
                if (!level_empty(bid_levels_[i])) {
                    best_bid_idx_ = i;
                    return;
                }
                if (i == 0) break;
                --i;
            }
            best_bid_idx_ = kNoBest;  // no more bids

        } else {
            if (best_ask_idx_ != drained_idx) return;

            const std::size_t last = ask_levels_.size() - 1;
            if (drained_idx == last) {
                best_ask_idx_ = kNoBest;
                return;
            }
            // Scan upward
            for (std::size_t i = drained_idx + 1; i < ask_levels_.size(); ++i) {
                if (!level_empty(ask_levels_[i])) {
                    best_ask_idx_ = i;
                    return;
                }
            }
            best_ask_idx_ = kNoBest;  // no more asks
        }
    }

    // ── Data members ──────────────────────────────────────────────────────────

    InstrumentId              instrument_id_;
    Price                     min_price_;
    Price                     max_price_;
    Price                     tick_size_;
    std::vector<PriceLevel>   bid_levels_;     // index 0 = min_price (worst bid)
    std::vector<PriceLevel>   ask_levels_;     // index 0 = min_price (best ask)
    std::size_t               best_bid_idx_;   // highest non-empty bid index
    std::size_t               best_ask_idx_;   // lowest  non-empty ask index
    OrderIndex                order_index_;    // order_id → Order* for O(1) cancel
};

} // namespace hft::lob

#pragma once

// =============================================================================
// uring_sender.hpp — io_uring Based ExecutionReport Sender
//
// Responsibility:
//   Runs on the Sender thread (pinned to a dedicated core).
//   Drains ExecutionReport structs from the OutboundQueue SPSC (posted by
//   the Matching Engine), serialises them to WireExecReport (48 bytes) using
//   the Encoder, and submits IORING_OP_SEND SQEs to the io_uring ring.
//
// Send buffer management (zero-heap):
//   Maintains a pre-allocated pool of N send buffers.  Each buffer holds
//   exactly one serialised WireExecReport.  On CQE completion, the buffer
//   slot is recycled.  The pool never allocates from the heap.
//
// io_uring setup:
//   Uses the same SQPOLL-first / standard fallback strategy as UringReceiver.
//   With SQPOLL active, pushing outbound reports involves:
//     1. Serialise to send buffer      — 1 memcpy (48 bytes)
//     2. Write SQE to shared ring     — ~5 stores
//     3. io_uring_submit()            — 0 syscalls (kernel thread awake)
//   Total outbound path = ~50 ns per ExecutionReport.
//
// Backpressure:
//   If all kNumSendBufs send buffers are in-flight, the sender spins on the
//   CQ until one completes.  This prevents infinite queue growth.
//   In practice, with kNumSendBufs = 256, backpressure never occurs unless
//   the remote client is extremely slow (disconnected / congested).
//
// Thread safety: NONE.  Call only from the Sender thread.
// =============================================================================

#ifdef __linux__

#include "io/wire_protocol.hpp"
#include "io/decoder.hpp"        // Encoder
#include "spsc/spsc_queue.hpp"

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <cstddef>
#include <cstdint>

#if defined(__x86_64__)
#  include <immintrin.h>
#  define TX_PAUSE() _mm_pause()
#else
#  define TX_PAUSE() ((void)0)
#endif

namespace hft::io {

using namespace hft::spsc;
using namespace hft::core;

// ─────────────────────────────────────────────────────────────────────────────
// UringSender
// ─────────────────────────────────────────────────────────────────────────────
class UringSender {
public:
    static constexpr uint32_t kRingDepth  = 256;
    static constexpr uint32_t kNumSendBufs= 256;  // max in-flight SEND ops
    static constexpr uint32_t kSqPollIdle = 2000;

    // ── Constructor ───────────────────────────────────────────────────────────
    //
    // sock_fd       — connected TCP socket for the outbound client session
    // out_queue     — SPSC queue drained from the Matching Engine thread
    //
    UringSender(int            sock_fd,
                OutboundQueue& out_queue)
        : sock_fd_   (sock_fd)
        , out_queue_ (out_queue)
    {
        assert(sock_fd_ >= 0);
        init_ring();
    }

    ~UringSender() {
        io_uring_queue_exit(&ring_);
    }

    UringSender(const UringSender&)            = delete;
    UringSender& operator=(const UringSender&) = delete;

    // ── poll() — tight send loop ──────────────────────────────────────────────
    //
    // Call in a tight loop on the Sender thread:
    //   while (running) { tx.poll(); }
    //
    // Each call:
    //   1. Drain CQ → recycle completed send buffers.
    //   2. Drain OutboundQueue → serialise → submit SEND SQEs.
    //   3. Flush pending SQEs.
    //
    // Returns total SQEs submitted this call.
    //
    int poll() noexcept {
        // Reclaim completed send buffers first (to maximise available slots).
        drain_completions();

        // Submit pending SQEs from previous call (batched for efficiency).
        flush_sqes();

        // Drain the outbound queue and submit new SENDs.
        ExecutionReport report{};
        int submitted = 0;

        while (out_queue_.try_pop(report)) {
            // If no free send buffer, spin until one completes.
            // This is backpressure: the sender is slower than the ME.
            while (free_count_ == 0) {
                drain_completions();
                TX_PAUSE();
            }

            const uint32_t slot = alloc_send_buf();

            // Serialise ExecutionReport → WireExecReport into send buffer.
            const std::size_t len =
                Encoder::encode_exec_report(report, send_bufs_[slot].data());
            assert(len == sizeof(WireExecReport));

            // Prepare SEND SQE.
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
            if (__builtin_expect(sqe == nullptr, 0)) {
                // SQ full — flush and retry.
                flush_sqes();
                drain_completions();
                sqe = io_uring_get_sqe(&ring_);
                if (sqe == nullptr) {
                    // Still full — give up on this report (log in production).
                    free_send_buf(slot);
                    ++errors_;
                    continue;
                }
            }

            io_uring_prep_send(sqe, sock_fd_,
                               send_bufs_[slot].data(), len,
                               MSG_NOSIGNAL);  // suppress SIGPIPE
            io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(slot));

            ++pending_sqes_;
            ++submitted;
            ++reports_sent_;
        }

        return submitted;
    }

    // ── Statistics ────────────────────────────────────────────────────────────
    [[nodiscard]] bool     sqpoll_active() const noexcept { return sqpoll_active_; }
    [[nodiscard]] uint64_t reports_sent()  const noexcept { return reports_sent_;  }
    [[nodiscard]] uint64_t errors()        const noexcept { return errors_;        }

private:

    // ── Ring init (SQPOLL with fallback) ─────────────────────────────────────
    void init_ring() {
        struct io_uring_params params{};
        params.flags          = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = kSqPollIdle;

        int ret = io_uring_queue_init_params(kRingDepth, &ring_, &params);
        if (ret == 0) {
            sqpoll_active_ = true;
            return;
        }

        ret = io_uring_queue_init(kRingDepth, &ring_, 0);
        if (ret < 0) {
            throw std::runtime_error(
                std::string("UringSender: io_uring_queue_init failed: ") +
                strerror(-ret));
        }
        sqpoll_active_ = false;
    }

    // ── Send buffer pool ──────────────────────────────────────────────────────

    // Allocate a free send buffer slot.  Caller must hold free_count_ > 0.
    [[nodiscard]] uint32_t alloc_send_buf() noexcept {
        for (uint32_t i = 0; i < kNumSendBufs; ++i) {
            if (!buf_in_flight_[i]) {
                buf_in_flight_[i] = true;
                --free_count_;
                return i;
            }
        }
        assert(false && "alloc_send_buf called with free_count_ == 0");
        __builtin_unreachable();
    }

    void free_send_buf(uint32_t slot) noexcept {
        assert(slot < kNumSendBufs);
        assert(buf_in_flight_[slot]);
        buf_in_flight_[slot] = false;
        ++free_count_;
    }

    // ── CQ drain — recycle completed sends ───────────────────────────────────
    void drain_completions() noexcept {
        struct io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
            const uint32_t slot = static_cast<uint32_t>(io_uring_cqe_get_data64(cqe));
            const int32_t  res  = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);

            if (res < 0) ++errors_;
            free_send_buf(slot);
        }
    }

    // ── SQE flush ─────────────────────────────────────────────────────────────
    void flush_sqes() noexcept {
        if (pending_sqes_ > 0) {
            io_uring_submit(&ring_);
            pending_sqes_ = 0;
        }
    }

    // ── Data members ──────────────────────────────────────────────────────────

    int             sock_fd_;
    struct io_uring ring_{};
    bool            sqpoll_active_  = false;
    uint32_t        pending_sqes_   = 0;

    // Pre-allocated send buffers (one WireExecReport each)
    std::array<std::array<uint8_t, sizeof(WireExecReport)>, kNumSendBufs> send_bufs_{};
    bool     buf_in_flight_[kNumSendBufs]{};
    uint32_t free_count_ = kNumSendBufs;

    OutboundQueue& out_queue_;

    uint64_t reports_sent_ = 0;
    uint64_t errors_       = 0;
};

} // namespace hft::io

#endif // __linux__

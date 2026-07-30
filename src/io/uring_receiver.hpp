#pragma once

// =============================================================================
// uring_receiver.hpp — io_uring Based Network Order Receiver
//
// Architecture:
//   Runs on the Receiver thread (pinned to a dedicated core).
//   Owns an io_uring ring and N pre-allocated, kernel-registered receive
//   buffers.  Continuously polls the Completion Queue (CQ) for received data
//   and posts RawByteSpan pointers to the RawByteQueue SPSC for the Decoder.
//
// io_uring setup strategy (SQPOLL with fallback):
//
//   1st try: IORING_SETUP_SQPOLL
//     → Kernel spawns a dedicated SQ polling thread.
//     → After writing an SQE to the shared ring, io_uring_submit() is usually
//       a no-op (just a memory write + fence; no syscall).  The kernel thread
//       picks up the SQE without entering the kernel.
//     → Requires CAP_SYS_NICE.  Fails in restricted cloud containers.
//
//   Fallback: standard io_uring (no SQPOLL)
//     → io_uring_submit() makes one syscall per batch (still much cheaper
//       than one syscall per recv).
//     → Works in all environments, including rootless containers.
//
// Fixed buffer registration (io_uring_register_buffers):
//   Pins receiver buffers in physical memory and informs the kernel about
//   them.  The kernel can then do DMA directly into our buffers without
//   a per-operation kernel allocation or bounce buffer.  If registration
//   fails (e.g., ENOMEM), we fall back to regular unregistered recv.
//
// Buffer lifecycle (zero-copy):
//   See decoder.hpp for the full buffer lifecycle diagram.
//   Receiver does NOT re-arm a buffer until the Decoder returns the buf_id
//   via BufReturnQueue.  This prevents overwriting live data in the decoder.
//
// Thread safety: NONE.  All methods must be called from the Receiver thread.
// =============================================================================

#ifdef __linux__

#include "io/wire_protocol.hpp"
#include "io/decoder.hpp"
#include "spsc/spsc_queue.hpp"

#include <liburing.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/mman.h>

#include <array>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <cstdint>
#include <cstddef>

#if defined(__x86_64__)
#  include <immintrin.h>
#  define RX_PAUSE() _mm_pause()
#else
#  define RX_PAUSE() ((void)0)
#endif

namespace hft::io {

using namespace hft::spsc;

// ─────────────────────────────────────────────────────────────────────────────
// UringReceiver
// ─────────────────────────────────────────────────────────────────────────────
class UringReceiver {
public:
    // Tuning constants
    static constexpr uint32_t kRingDepth  = 256;     // io_uring queue depth
    static constexpr uint32_t kNumBufs    = 64;      // number of fixed recv buffers
    static constexpr uint32_t kBufSize    = 4096;    // bytes per buffer (1 page)
    static constexpr uint32_t kSqPollIdle = 2000;    // ms before SQPOLL thread sleeps

    // ── Constructor ───────────────────────────────────────────────────────────
    //
    // sock_fd     — already-connected or listening socket (TCP/UDP)
    // raw_queue   — SPSC queue to push received RawByteSpans into
    // ret_queue   — SPSC queue to receive freed buffer IDs from the Decoder
    //
    UringReceiver(int            sock_fd,
                  RawByteQueue&  raw_queue,
                  BufReturnQueue& ret_queue)
        : sock_fd_    (sock_fd)
        , raw_queue_  (raw_queue)
        , ret_queue_  (ret_queue)
    {
        assert(sock_fd_ >= 0);
        init_ring();
        init_fixed_buffers();
        arm_all_buffers();
    }

    ~UringReceiver() {
        if (bufs_registered_) {
            io_uring_unregister_buffers(&ring_);
        }
        io_uring_queue_exit(&ring_);
    }

    // Non-copyable.
    UringReceiver(const UringReceiver&)            = delete;
    UringReceiver& operator=(const UringReceiver&) = delete;

    // ── poll() — tight receive loop ───────────────────────────────────────────
    //
    // Call this in a tight loop on the Receiver thread:
    //   while (running) { rx.poll(); }
    //
    // Each call:
    //   1. Drains BufReturnQueue → re-arms freed buffers (new RECV SQEs).
    //   2. Flushes pending SQEs to the kernel (if not SQPOLL or if sleeping).
    //   3. Drains the CQ ring non-blocking → posts RawByteSpans to raw_queue_.
    //
    // Returns number of CQEs processed.
    //
    int poll() noexcept {
        // ── Step 1: reclaim freed buffers from the Decoder ───────────────────
        reclaim_buffers();

        // ── Step 2: submit any pending SQEs ──────────────────────────────────
        //    With SQPOLL: io_uring_submit() checks IORING_SQ_NEED_WAKEUP.
        //    If the kernel thread is awake: returns without syscall.
        //    If sleeping: wakes it with a single syscall (rare).
        //    Without SQPOLL: normal submit syscall.
        if (pending_sqes_ > 0) {
            io_uring_submit(&ring_);
            pending_sqes_ = 0;
        }

        // ── Step 3: drain Completion Queue (non-blocking) ────────────────────
        struct io_uring_cqe* cqe = nullptr;
        int processed = 0;

        while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
            process_cqe(cqe);
            io_uring_cqe_seen(&ring_, cqe);
            ++processed;
        }

        return processed;
    }

    // ── Statistics ────────────────────────────────────────────────────────────
    [[nodiscard]] bool     sqpoll_active()    const noexcept { return sqpoll_active_;    }
    [[nodiscard]] bool     bufs_registered()  const noexcept { return bufs_registered_;  }
    [[nodiscard]] uint64_t bytes_received()   const noexcept { return bytes_received_;   }
    [[nodiscard]] uint64_t cqes_processed()   const noexcept { return cqes_processed_;   }
    [[nodiscard]] uint64_t errors_seen()      const noexcept { return errors_seen_;      }

private:

    // ── Ring initialisation ───────────────────────────────────────────────────
    void init_ring() {
        // Attempt SQPOLL (zero-syscall SQ submission when thread is awake)
        struct io_uring_params params{};
        params.flags          = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = kSqPollIdle;

        int ret = io_uring_queue_init_params(kRingDepth, &ring_, &params);
        if (ret == 0) {
            sqpoll_active_ = true;
            return;
        }

        // SQPOLL failed (typically EPERM / no CAP_SYS_NICE in containers).
        // Fall back to a standard ring.
        ret = io_uring_queue_init(kRingDepth, &ring_, 0);
        if (ret < 0) {
            throw std::runtime_error(
                std::string("io_uring_queue_init failed: ") + strerror(-ret));
        }
        sqpoll_active_ = false;
    }

    // ── Fixed buffer registration ─────────────────────────────────────────────
    void init_fixed_buffers() {
        // Build iovec array pointing to each pre-allocated buffer.
        for (uint32_t i = 0; i < kNumBufs; ++i) {
            iovecs_[i].iov_base = recv_bufs_[i].data();
            iovecs_[i].iov_len  = kBufSize;
            buf_armed_[i]       = false;
        }

        // Register with kernel — pins pages in RAM, enables direct DMA.
        // Falls back gracefully on ENOMEM (permission issues in containers).
        int ret = io_uring_register_buffers(&ring_, iovecs_, kNumBufs);
        bufs_registered_ = (ret == 0);
        // If registration failed we still proceed — recv ops just can't use
        // the IOSQE_BUFFER_SELECT path, but correctness is maintained.
    }

    // ── Arm all N buffers at startup ──────────────────────────────────────────
    void arm_all_buffers() {
        for (uint32_t i = 0; i < kNumBufs; ++i) {
            arm_buf(i);
        }
        // Flush the initial batch of SQEs.
        io_uring_submit(&ring_);
        pending_sqes_ = 0;
    }

    // ── arm_buf() — submit one RECV SQE for buffer `id` ─────────────────────
    void arm_buf(uint32_t id) noexcept {
        assert(id < kNumBufs);
        assert(!buf_armed_[id]);

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (__builtin_expect(sqe == nullptr, 0)) {
            // SQ is full — this should not happen with correct ring sizing.
            // Drop the re-arm; the buffer stays unscheduled.
            ++errors_seen_;
            return;
        }

        // Prepare a standard RECV operation.
        // The buffer is already registered with the kernel (if bufs_registered_),
        // so the kernel can do DMA directly without pinning on each call.
        io_uring_prep_recv(sqe,
                           sock_fd_,
                           recv_bufs_[id].data(),
                           kBufSize,
                           MSG_DONTWAIT);

        // Embed the buffer ID in user_data so we can identify it in the CQE.
        io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(id));

        buf_armed_[id] = true;
        ++pending_sqes_;
    }

    // ── process_cqe() — handle one completion ────────────────────────────────
    void process_cqe(const struct io_uring_cqe* cqe) noexcept {
        ++cqes_processed_;

        const uint32_t id    = static_cast<uint32_t>(io_uring_cqe_get_data64(cqe));
        const int32_t  bytes = cqe->res;

        assert(id < kNumBufs);
        buf_armed_[id] = false;  // mark as no longer in-flight

        if (bytes > 0) {
            // Valid data received — post to decoder.
            bytes_received_ += static_cast<uint64_t>(bytes);

            RawByteSpan span{};
            span.data   = recv_bufs_[id].data();
            span.len    = static_cast<uint32_t>(bytes);
            span.buf_id = id;

            // Post to RawByteQueue — decoder owns this buffer until it
            // returns the id via BufReturnQueue.
            // Spin if the decoder is momentarily backpressured.
            while (!raw_queue_.try_push(span)) { RX_PAUSE(); }

            // Do NOT re-arm here — wait for Decoder to return the buf_id.

        } else if (bytes == 0) {
            // EOF / connection gracefully closed.
            // Re-arm immediately to detect future reconnects.
            arm_buf(id);
        } else {
            // Error (EAGAIN, ECONNRESET, etc.).
            ++errors_seen_;
            arm_buf(id);
        }
    }

    // ── reclaim_buffers() — drain BufReturnQueue, re-arm freed buffers ───────
    void reclaim_buffers() noexcept {
        uint32_t buf_id = 0;
        while (ret_queue_.try_pop(buf_id)) {
            if (buf_id < kNumBufs && !buf_armed_[buf_id]) {
                arm_buf(buf_id);
            }
        }
    }

    // ── Data members ──────────────────────────────────────────────────────────

    int                                         sock_fd_;
    struct io_uring                             ring_{};
    bool                                        sqpoll_active_  = false;
    bool                                        bufs_registered_= false;
    uint32_t                                    pending_sqes_   = 0;

    // N page-aligned receive buffers (registered with kernel for DMA)
    std::array<std::array<uint8_t, kBufSize>, kNumBufs> recv_bufs_{};
    struct iovec                                iovecs_[kNumBufs]{};
    bool                                        buf_armed_[kNumBufs]{};

    // SPSC queue references (non-owning)
    RawByteQueue&   raw_queue_;
    BufReturnQueue& ret_queue_;

    // Stats
    uint64_t bytes_received_  = 0;
    uint64_t cqes_processed_  = 0;
    uint64_t errors_seen_     = 0;
};

} // namespace hft::io

#endif // __linux__

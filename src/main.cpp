// =============================================================================
// main.cpp — HFT Order Matching Engine Daemon
//
// Overview:
//   This is the top-level daemon entry point that ties together all components
//   of the High-Frequency Trading Order Matching Engine:
//
//     1. Lock-free SPSC Queues (RawByteQueue, BufReturnQueue, InboundQueue, OutboundQueue)
//     2. Network & protocol pipeline (UringReceiver, Decoder, UringSender)
//     3. Zero-allocation Matching Engine (MatchingEngine & OrderPool)
//
// Architecture & Threading:
//   The engine deploys four dedicated worker threads via std::jthread:
//     • Receiver Thread : polls io_uring completion queue for network bytes & pushes to RawByteQueue
//     • Decoder Thread  : drains RawByteQueue, parses wire format, pushes to InboundQueue
//     • Matching Thread : drains InboundQueue, executes matches in O(1)/O(levels), pushes ExecutionReports
//     • Sender Thread   : drains OutboundQueue, serializes reports, submits io_uring send operations
//
// Signal Handling & Graceful Shutdown:
//   Catches SIGINT and SIGTERM to transition global std::atomic<bool> g_running to false.
//   Upon receiving a shutdown signal, worker threads cleanly complete their current poll cycle,
//   the main thread joins all workers, prints final operational statistics, and terminates cleanly.
// =============================================================================

#include "core/order.hpp"
#include "core/order_pool.hpp"
#include "spsc/spsc_queue.hpp"
#include "lob/order_book.hpp"
#include "matching/matching_engine.hpp"
#include "io/wire_protocol.hpp"
#include "io/decoder.hpp"

#if defined(__linux__)
#  include "io/uring_receiver.hpp"
#  include "io/uring_sender.hpp"
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <unistd.h>
#endif

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <csignal>
#include <chrono>
#include <memory>
#include <stdexcept>

namespace {
    // ─────────────────────────────────────────────────────────────────────────
    // Global signal state
    // ─────────────────────────────────────────────────────────────────────────
    std::atomic<bool> g_running{true};

    void signal_handler(int signum) {
        if (signum == SIGINT || signum == SIGTERM) {
            // Memory order release ensures worker threads observe the change
            // when reading with memory order acquire in their polling loops.
            g_running.store(false, std::memory_order_release);
        }
    }
} // anonymous namespace

int main() {
    std::cout << "===============================================================\n";
    std::cout << "        HIGH-FREQUENCY TRADING ORDER MATCHING ENGINE           \n";
    std::cout << "===============================================================\n";
    std::cout << "[Config] sizeof(Order)         = " << sizeof(hft::core::Order) << " bytes (Cache-Line Aligned)\n";
    std::cout << "[Config] sizeof(PriceLevel)    = " << sizeof(hft::lob::PriceLevel) << " bytes (Half Cache-Line)\n";
    std::cout << "[Config] OrderPool Capacity    = " << hft::core::kDefaultPoolCapacity << " orders\n";

    // ── 1. Register signal handlers for graceful shutdown ────────────────────
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::cout << "[Init] Registered SIGINT and SIGTERM signal handlers.\n";

    try {
        // ── 2. Initialize SPSC Ring Buffer Queues ──────────────────────────────
        // Heap allocated via std::make_unique to avoid excessive stack usage.
        auto raw_queue      = std::make_unique<hft::spsc::RawByteQueue>();
        auto buf_ret_queue  = std::make_unique<hft::io::BufReturnQueue>();
        auto inbound_queue  = std::make_unique<hft::spsc::InboundQueue>();
        auto outbound_queue = std::make_unique<hft::spsc::OutboundQueue>();
        std::cout << "[Init] Lock-free SPSC communication ring buffers allocated.\n";

        // ── 3. Initialize Decoder and Matching Engine ──────────────────────────
        hft::io::Decoder decoder;
        hft::matching::MatchingEngine engine(*inbound_queue, *outbound_queue);

        // Register default test instrument (e.g. Instrument ID 1, prices 1000 to 100000 ticks)
        constexpr hft::core::InstrumentId kDefaultInstId = 1;
        engine.add_instrument(kDefaultInstId, 1000, 100'000, /*tick_size=*/1);
        std::cout << "[Init] Matching Engine ready. Instrument ID " << kDefaultInstId << " registered.\n";

        // ── 4. Initialize io_uring Receiver & Sender (Linux architecture only) ──
#if defined(__linux__)
        // For staging and integration testing without a live network attachment,
        // we create a non-blocking dummy socketpair so liburing can register valid socket FDs.
        int dummy_fds[2] = {-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, dummy_fds) < 0) {
            std::cerr << "[Warning] Failed to create UNIX socketpair, attempting fallback sockets.\n";
            dummy_fds[0] = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
            dummy_fds[1] = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
        }

        auto receiver = std::make_unique<hft::io::UringReceiver>(dummy_fds[0], *raw_queue, *buf_ret_queue);
        auto sender   = std::make_unique<hft::io::UringSender>(dummy_fds[1], *outbound_queue);
        std::cout << "[Init] Linux io_uring Receiver & Sender initialized (SQPOLL active: "
                  << (receiver->sqpoll_active() ? "YES" : "NO / Fallback Standard Ring") << ").\n";
#else
        std::cout << "[Init] Non-Linux target detected; skipping io_uring network layer instantiation.\n";
#endif

        // ── 5. Spawn Dedicated Worker Threads ──────────────────────────────────
        std::cout << "[Daemon] Spawning dedicated pipeline worker threads...\n";

#if defined(__linux__)
        std::jthread receiver_thread([&receiver]() {
            while (g_running.load(std::memory_order_acquire)) {
                receiver->poll();
            }
        });

        std::jthread sender_thread([&sender]() {
            while (g_running.load(std::memory_order_acquire)) {
                sender->poll();
            }
        });
#endif

        std::jthread decoder_thread([&decoder, &raw_queue, &inbound_queue, &buf_ret_queue]() {
            while (g_running.load(std::memory_order_acquire)) {
                decoder.poll(*raw_queue, *inbound_queue, *buf_ret_queue);
            }
        });

        std::jthread matching_thread([&engine]() {
            while (g_running.load(std::memory_order_acquire)) {
                engine.poll();
            }
        });

        std::cout << "[Daemon] All worker threads active and polling in non-blocking loops.\n";
        std::cout << "[Daemon] System online. Send SIGINT (Ctrl+C) or SIGTERM to gracefully shut down.\n";

        // ── 6. Main thread idle supervision loop ──────────────────────────────
        while (g_running.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // ── 7. Graceful Shutdown & Cleanup ────────────────────────────────────
        std::cout << "\n[Shutdown] Shutdown signal intercepted (g_running == false).\n";
        std::cout << "[Shutdown] Awaiting worker thread termination...\n";

#if defined(__linux__)
        if (receiver_thread.joinable()) receiver_thread.join();
        if (sender_thread.joinable())   sender_thread.join();
#endif
        if (decoder_thread.joinable())  decoder_thread.join();
        if (matching_thread.joinable()) matching_thread.join();

#if defined(__linux__)
        if (dummy_fds[0] >= 0) ::close(dummy_fds[0]);
        if (dummy_fds[1] >= 0) ::close(dummy_fds[1]);
#endif
        std::cout << "[Shutdown] All worker threads joined successfully.\n";

        // ── 8. Print Final System Statistics ──────────────────────────────────
        std::cout << "\n===============================================================\n";
        std::cout << "                     FINAL SYSTEM STATISTICS                   \n";
        std::cout << "===============================================================\n";
        std::cout << "  [Decoder Thread]\n";
        std::cout << "    • Raw Byte Spans Processed : " << decoder.spans_seen() << "\n";
        std::cout << "    • Valid Orders Decoded     : " << decoder.decoded_count() << "\n";
        std::cout << "    • Protocol Decode Errors   : " << decoder.error_count() << "\n\n";

        std::cout << "  [Matching Engine Thread]\n";
        std::cout << "    • Orders Processed         : " << engine.orders_processed() << "\n";
        std::cout << "    • Fills Executed           : " << engine.fills_executed() << "\n";
        std::cout << "    • Orders Rejected          : " << engine.orders_rejected() << "\n";
#if defined(__linux__)
        std::cout << "\n  [I/O uring Network Layer]\n";
        std::cout << "    • Bytes Received           : " << receiver->bytes_received() << "\n";
        std::cout << "    • CQEs Processed (Rx)      : " << receiver->cqes_processed() << "\n";
        std::cout << "    • Exec Reports Sent (Tx)   : " << sender->reports_sent() << "\n";
        std::cout << "    • I/O Errors Observed      : " << (receiver->errors_seen() + sender->errors()) << "\n";
#endif
        std::cout << "===============================================================\n\n";
        std::cout << "[Daemon] Clean shutdown sequence completed. Exiting cleanly.\n";

    } catch (const std::exception& e) {
        std::cerr << "[Fatal] Unhandled exception in main: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

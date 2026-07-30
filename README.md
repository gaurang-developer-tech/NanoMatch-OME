# NanoMatch OME (Order Matching Engine)

A high-frequency, ultra-low latency Order Matching Engine built in C++20. NanoMatch is designed with strict hardware sympathy, utilizing lock-free concurrency, kernel-bypassing I/O, and a zero-allocation hot path to achieve sub-microsecond end-to-end execution latency.

## Core Architecture & Features

* **Zero-Allocation Hot Path:** Custom `std::pmr`-style memory slab allocator pre-faults huge pages (`MAP_HUGETLB | MAP_POPULATE`) on startup. Absolutely no `new` or `malloc` calls occur during the matching cycle.
* **Wait-Free Thread Communication:** CPU cores communicate exclusively via custom Single-Producer Single-Consumer (SPSC) ring buffers. Uses split cache-line padding (`alignas(64)`) to completely eliminate false sharing, relying strictly on explicit `acquire/release` atomic memory barriers rather than costly mutexes or `seq_cst`.
* **O(1) Limit Order Book (LOB):** The LOB is implemented using a cache-friendly flat array for price levels and an embedded intrusive doubly-linked list for resting orders. Order cancellations execute in strict $O(1)$ time regardless of queue depth.
* **Kernel-Bypassing I/O (`io_uring`):** Network ingestion and outbound reporting leverage Linux `liburing` with `IORING_SETUP_SQPOLL` and registered fixed buffers, enabling zero-syscall packet processing and direct DMA.

## Pipeline Topology

The engine isolates workloads across pinned, dedicated CPU cores:

1. **UringReceiver:** Polls `io_uring` CQEs, passing raw network byte spans to the decoder.
2. **Decoder:** Deserializes fixed-length binary wire protocols into `InboundOrderMsg` structs.
3. **Matching Engine:** Sweeps the LOB array, executes Limit/Market/IOC/FOK logic, and generates `ExecutionReport` structures.
4. **UringSender:** Serializes reports and submits `IORING_OP_SEND` SQEs back to the network.

## Build Instructions (Linux)

This project requires a Linux environment (Kernel 5.11+) to support `io_uring` and C++20 compiler features (GCC 11+ or Clang 14+).

```bash
# Clone the repository
git clone https://github.com/gaurang-developer-tech/NanoMatch-OME.git
cd NanoMatch-OME

# Configure and build
chmod +x build.sh
./build.sh          # Standard release build
./build.sh --tsan   # ThreadSanitizer build (data race verification)
./build.sh --bench  # Execute Google Benchmark suite
```

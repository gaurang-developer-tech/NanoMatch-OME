#!/usr/bin/env bash
# =============================================================================
# build.sh — Bootstrap, configure, build, and test the HFT OME
#
# Usage:
#   chmod +x build.sh
#   ./build.sh            # Release build + run all tests
#   ./build.sh --asan     # ASan+UBSan build + tests
#   ./build.sh --tsan     # TSan build + tests
#   ./build.sh --bench    # Release build + run benchmarks
# =============================================================================

set -euo pipefail

# ─── Colour helpers ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── Dependency checks ────────────────────────────────────────────────────────
check_deps() {
    for cmd in cmake g++ ninja git; do
        if ! command -v "$cmd" &>/dev/null; then
            error "'$cmd' not found. Install with: sudo apt install cmake g++ ninja-build git"
        fi
    done

    GCC_VER=$(g++ -dumpversion | cut -d. -f1)
    if [[ "$GCC_VER" -lt 11 ]]; then
        error "GCC >= 11 required for C++20. Found: GCC $GCC_VER"
    fi
    info "Toolchain: $(g++ --version | head -1)"
    info "CMake:     $(cmake --version | head -1)"
}

# ─── Optional: install liburing ───────────────────────────────────────────────
check_liburing() {
    if ! dpkg -s liburing-dev &>/dev/null 2>&1; then
        warn "liburing-dev not found — io_uring targets will be skipped."
        warn "Install with: sudo apt install liburing-dev"
    else
        info "liburing-dev found."
    fi
}

# ─── Huge page check (informational) ─────────────────────────────────────────
check_hugepages() {
    local hp
    hp=$(cat /proc/sys/vm/nr_hugepages 2>/dev/null || echo 0)
    if [[ "$hp" -eq 0 ]]; then
        warn "Huge pages not configured (nr_hugepages=0). OrderPool will fall back to 4KB pages."
        warn "To enable: sudo sysctl -w vm.nr_hugepages=512"
    else
        info "Huge pages available: $hp × 2MB"
    fi
}

# ─── Build ────────────────────────────────────────────────────────────────────
BUILD_TYPE="Release"
SUFFIX=""
RUN_BENCH=false

case "${1:-}" in
    --asan)   BUILD_TYPE="Debug";   SUFFIX="_asan" ;;
    --tsan)   BUILD_TYPE="Debug";   SUFFIX="_tsan" ;;
    --bench)  BUILD_TYPE="Release"; RUN_BENCH=true  ;;
    "")       BUILD_TYPE="Release" ;;
    *)        error "Unknown option: $1. Use --asan, --tsan, or --bench." ;;
esac

BUILD_DIR="build_${BUILD_TYPE,,}"

info "Build type: $BUILD_TYPE  |  Build dir: $BUILD_DIR"

check_deps
check_liburing
check_hugepages

# ─── Configure ────────────────────────────────────────────────────────────────
info "Configuring..."
cmake -S . -B "$BUILD_DIR"             \
    -G Ninja                           \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"   \
    -DCMAKE_CXX_COMPILER=g++

# ─── Compile ──────────────────────────────────────────────────────────────────
NPROC=$(nproc)
info "Building with $NPROC parallel jobs..."
cmake --build "$BUILD_DIR" --parallel "$NPROC"

info "Build complete ✓"

# ─── Test ─────────────────────────────────────────────────────────────────────
if $RUN_BENCH; then
    info "Running benchmarks..."
    for bench in "$BUILD_DIR"/benchmarks/bench_*; do
        [[ -x "$bench" ]] || continue
        echo ""
        info ">>> $(basename "$bench")"
        "$bench" --benchmark_min_time=1s
    done
else
    info "Running tests (suffix='${SUFFIX}')..."
    ctest --test-dir "$BUILD_DIR"       \
          --output-on-failure           \
          -R ".*${SUFFIX}$"             \
          --parallel "$NPROC"
fi

echo ""
info "All done! ✓"

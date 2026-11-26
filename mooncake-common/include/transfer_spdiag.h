#pragma once
#include <chrono>
#include <cstring>
#include "spdiag_program_name.h"
#ifndef SPDIAG_PERF_DEF_FILE
#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#endif
#ifndef SPDIAG_PROGRAM_NAME
#define SPDIAG_PROGRAM_NAME mooncake::SpDiagProgramName("mooncake_store")
#endif
#include "mooncake_spdiag.h"
#if defined(SPDIAG_ENABLE_PERFLOG) && !defined(SPDIAG_DISABLE)
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace mooncake::transfer_diag {
inline bool enabled() {
#ifdef SPDIAG_DISABLE
    return false;
#else
    auto& m = SpDiag::PerfManager::Instance();
    m.EnsureInitialized();
    return m.IsInitialized() && m.IsLevelEnabled(SpDiag::PerfLevel::MODULE);
#endif
}
inline void record(PerfKey key, uint64_t ns, int status = 0) {
#ifndef SPDIAG_DISABLE
    // v1.0.0 has no API to submit a previously measured duration. Use its
    // public local shard API, with the same accumulation rules as End().
    // Calling Start()/End() here would measure the recorder, not the transfer.
    const auto slot = static_cast<uint32_t>(key);
    if (slot == 0 || slot >= static_cast<uint32_t>(PerfKey::COUNT) ||
        !enabled()) {
        return;
    }
    auto& manager = SpDiag::PerfManager::Instance();
    auto& stat = manager.GetShard().stats[slot];
    stat.slotIndex = manager.FastResolveSlot(slot);
    stat.level = static_cast<uint8_t>(SpDiag::PerfLevel::MODULE);
    if (status == 0) {
        ++stat.backGoodCount;
    } else {
        ++stat.backBadCount;
    }
    ++stat.tickCount;
    stat.totalTimeNs += ns;
    if (stat.minTimeNs == 0 || ns < stat.minTimeNs) stat.minTimeNs = ns;
    if (ns > stat.maxTimeNs) stat.maxTimeNs = ns;
#ifdef SPDIAG_ENABLE_PERCENTILE
    SpDiag::InsertTopN(stat, ns);
#endif
#ifdef SPDIAG_ENABLE_PERFLOG
    if (manager.IsPerfLogEnabled()) {
        uint64_t timestamp;
#if defined(__aarch64__)
        // PerfLog v1.0.0 stores raw architectural counter ticks, not ns.
        __asm__ volatile("mrs %0, cntvct_el0" : "=r"(timestamp));
#else
        timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
#endif
        static thread_local const auto tid = syscall(SYS_gettid);
        const auto index = stat.perflogWritePos.fetch_add(
                               1, std::memory_order_relaxed) %
                           SpDiag::perflogTimestampCount;
        stat.timestamps[index] = timestamp;
        stat.perflogCosts[index] = ns;
        stat.perflogThreadIds[index] = static_cast<uint16_t>(tid);
    }
#endif
#else
    (void)key;
    (void)ns;
    (void)status;
#endif
}
inline void interval(PerfKey key, uint64_t start, uint64_t end,
                     int status = 0) {
    if (start && end >= start) record(key, end - start, status);
}
struct Boundaries {
    uint64_t batch = 0, start = 0, done = 0;
};
inline thread_local Boundaries last_transfer;
inline void taskEdges(uint64_t batch, uint64_t created, uint64_t terminal) {
    if (batch != last_transfer.batch) return;
    interval(PerfKey::SD_TRANSFER_PRETASK, last_transfer.start, created);
    interval(PerfKey::SD_TRANSFER_PUBLISH, terminal, last_transfer.done);
}
class Scope {
   public:
    explicit Scope(PerfKey key) : point_(key, SpDiag::PerfLevel::MODULE) {
        point_.Start();
    }
    ~Scope() { point_.End(); }

   private:
    SpDiag::PerfPoint point_;
};
}  // namespace mooncake::transfer_diag

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
#include "spdiag/auto_perf.h"
namespace mooncake::transfer_diag {
inline bool enabled() {
    auto& m = SpDiag::PerfManager::Instance();
    m.EnsureInitialized();
    return m.IsInitialized() && m.IsLevelEnabled(SpDiag::PerfLevel::MODULE);
}
inline void record(PerfKey key, uint64_t ns, int status = 0) {
    SpDiag::PerfPoint p(key, SpDiag::PerfLevel::MODULE);
    p.RecordDurationNs(ns, status);
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

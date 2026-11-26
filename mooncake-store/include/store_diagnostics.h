#include "transfer_spdiag.h"
#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <array>
#include <chrono>
#include <sstream>
#include <string>

#include "mooncake_logging.h"

namespace mooncake {

// Same opt-in as transfer_trace and ub_task_trace. No clock reads or log
// formatting when disabled. Nanosecond stamps are comparable within a process.
inline uint64_t StoreTransferTraceMinBytes() {
    static const uint64_t threshold = [] {
        const char* raw = std::getenv("MC_TRANSFER_TRACE_MIN_BYTES");
        if (!raw || !*raw || *raw == '-') return uint64_t{0};
        char* end = nullptr;
        auto value = std::strtoull(raw, &end, 10);
        return *end == '\0' ? static_cast<uint64_t>(value) : uint64_t{0};
    }();
    return threshold;
}

inline bool SampleStoreTransferTrace() {
    return StoreTransferTraceMinBytes() > 0 &&
           logging::ShouldSampleHiFreqLog(logging::CurrentTraceId()) &&
           logging::ShouldLog(google::INFO);
}

// Synchronous get_buffer call context. TransferFuture::wait records the
// observed transfer boundaries on the calling thread, never on a CQ thread.
class ScopedReadTransferTrace {
   public:
    using Clock = std::chrono::steady_clock;
    ScopedReadTransferTrace(uint64_t bytes, Clock::time_point start)
        : previous_(current_),
          start_(start),
          enabled_(transfer_diag::enabled() ||
                   (SampleStoreTransferTrace() &&
                    bytes >= StoreTransferTraceMinBytes())) {
        current_ = enabled_ ? this : nullptr;
    }
    ~ScopedReadTransferTrace() { current_ = previous_; }
    ScopedReadTransferTrace(const ScopedReadTransferTrace&) = delete;
    ScopedReadTransferTrace& operator=(const ScopedReadTransferTrace&) = delete;
    static void record(Clock::time_point start, Clock::time_point done) {
        if (!current_) return;
        ++current_->transfers_;
        current_->submit_ = start;
        current_->done_ = done;
    }
    void recordRead(Clock::time_point end) const {
        if (!valid(end)) return;
        transfer_diag::record(
            PerfKey::SD_READ_BEFORE,
            std::chrono::duration_cast<std::chrono::nanoseconds>(submit_ -
                                                                 start_)
                .count());
        transfer_diag::record(
            PerfKey::SD_READ_TRANSFER,
            std::chrono::duration_cast<std::chrono::nanoseconds>(done_ -
                                                                 submit_)
                .count());
        transfer_diag::record(
            PerfKey::SD_READ_AFTER,
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - done_)
                .count());
    }
    bool valid(Clock::time_point end) const {
        return enabled_ && transfers_ == 1 && start_ <= submit_ &&
               submit_ <= done_ && done_ <= end;
    }
    double beforeUs(Clock::time_point end) const {
        return valid(end) ? us(submit_ - start_) : -1.0;
    }
    double afterUs(Clock::time_point end) const {
        return valid(end) ? us(end - done_) : -1.0;
    }
    double transferUs(Clock::time_point end) const {
        return valid(end) ? us(done_ - submit_) : -1.0;
    }
    size_t transfers() const { return transfers_; }

   private:
    static double us(Clock::duration duration) {
        return std::chrono::duration<double, std::micro>(duration).count();
    }
    inline static thread_local ScopedReadTransferTrace* current_ = nullptr;
    ScopedReadTransferTrace* previous_;
    Clock::time_point start_, submit_{}, done_{};
    bool enabled_;
    size_t transfers_ = 0;
};

inline void RecordPathStage(const char* scope, const char* phase, uint64_t ns) {
    if (!std::strcmp(scope, "get_buffer_read") &&
        !std::strcmp(phase, "slices_and_replica"))
        transfer_diag::record(
            PerfKey::SD_PATH_GET_BUFFER_READ_SLICES_AND_REPLICA, ns);
    if (!std::strcmp(scope, "get_buffer_read") &&
        !std::strcmp(phase, "client_get"))
        transfer_diag::record(PerfKey::SD_PATH_GET_BUFFER_READ_CLIENT_GET, ns);
    if (!std::strcmp(scope, "get_buffer_read") &&
        !std::strcmp(phase, "get_breakdown_log"))
        transfer_diag::record(
            PerfKey::SD_PATH_GET_BUFFER_READ_GET_BREAKDOWN_LOG, ns);
    if (!std::strcmp(scope, "get_buffer_read") && !std::strcmp(phase, "tail"))
        transfer_diag::record(PerfKey::SD_PATH_GET_BUFFER_READ_TAIL, ns);
    if (!std::strcmp(scope, "get_buffer_read") && !std::strcmp(phase, "total"))
        transfer_diag::record(PerfKey::SD_PATH_GET_BUFFER_READ_TOTAL, ns);
    if (!std::strcmp(scope, "client_get") &&
        !std::strcmp(phase, "select_replica"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_SELECT_REPLICA, ns);
    if (!std::strcmp(scope, "client_get") &&
        !std::strcmp(phase, "cache_and_validate"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_CACHE_AND_VALIDATE,
                              ns);
    if (!std::strcmp(scope, "client_get") &&
        !std::strcmp(phase, "transfer_read"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_TRANSFER_READ, ns);
    if (!std::strcmp(scope, "client_get") &&
        !std::strcmp(phase, "release_cache_and_metrics"))
        transfer_diag::record(
            PerfKey::SD_PATH_CLIENT_GET_RELEASE_CACHE_AND_METRICS, ns);
    if (!std::strcmp(scope, "client_get") && !std::strcmp(phase, "checksum"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_CHECKSUM, ns);
    if (!std::strcmp(scope, "client_get") && !std::strcmp(phase, "tail"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_TAIL, ns);
    if (!std::strcmp(scope, "client_get") && !std::strcmp(phase, "total"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_GET_TOTAL, ns);
    if (!std::strcmp(scope, "client_transfer_data") &&
        !std::strcmp(phase, "plan_and_submit"))
        transfer_diag::record(
            PerfKey::SD_PATH_CLIENT_TRANSFER_DATA_PLAN_AND_SUBMIT, ns);
    if (!std::strcmp(scope, "client_transfer_data") &&
        !std::strcmp(phase, "future_get"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_TRANSFER_DATA_FUTURE_GET,
                              ns);
    if (!std::strcmp(scope, "client_transfer_data") &&
        !std::strcmp(phase, "diagnostics_and_finish"))
        transfer_diag::record(
            PerfKey::SD_PATH_CLIENT_TRANSFER_DATA_DIAGNOSTICS_AND_FINISH, ns);
    if (!std::strcmp(scope, "client_transfer_data") &&
        !std::strcmp(phase, "tail"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_TRANSFER_DATA_TAIL, ns);
    if (!std::strcmp(scope, "client_transfer_data") &&
        !std::strcmp(phase, "total"))
        transfer_diag::record(PerfKey::SD_PATH_CLIENT_TRANSFER_DATA_TOTAL, ns);
}

class ScopedTransferPathTrace {
   public:
    ScopedTransferPathTrace(const char* scope, uint64_t bytes)
        : scope_(scope),
          bytes_(bytes),
          enabled_(transfer_diag::enabled() ||
                   (SampleStoreTransferTrace() &&
                    bytes >= StoreTransferTraceMinBytes())) {
        if (enabled_) {
            trace_id_ = logging::CurrentTraceId();
            start_ = last_ = Clock::now();
        }
    }
    void mark(const char* phase) {
        if (!enabled_ || count_ == phases_.size()) return;
        auto now = Clock::now();
        RecordPathStage(
            scope_, phase,
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_)
                .count());
        phases_[count_++] = {phase, now - last_};
        last_ = now;
    }
    ~ScopedTransferPathTrace() {
        if (!enabled_) return;
        const auto end = Clock::now();
        RecordPathStage(
            scope_, "tail",
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - last_)
                .count());
        RecordPathStage(
            scope_, "total",
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_)
                .count());
        if (!SampleStoreTransferTrace()) return;
        std::ostringstream fields;
        for (size_t i = 0; i < count_; ++i) {
            fields << ' ' << phases_[i].name << "_us="
                   << std::chrono::duration<double, std::micro>(
                          phases_[i].duration)
                          .count();
        }
        MC_LOG(INFO)
            << "transfer_path_trace trace_id=" << trace_id_
            << " scope=" << scope_ << " bytes=" << bytes_ << " start_ns="
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   start_.time_since_epoch())
                   .count()
            << " end_ns="
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   end.time_since_epoch())
                   .count()
            << fields.str() << " tail_us="
            << std::chrono::duration<double, std::micro>(end - last_).count()
            << " total_us="
            << std::chrono::duration<double, std::micro>(end - start_).count();
    }
    ScopedTransferPathTrace(const ScopedTransferPathTrace&) = delete;
    ScopedTransferPathTrace& operator=(const ScopedTransferPathTrace&) = delete;

   private:
    using Clock = std::chrono::steady_clock;
    struct Phase {
        const char* name = nullptr;
        Clock::duration duration{};
    };
    const char* scope_;
    uint64_t bytes_, trace_id_ = 0;
    bool enabled_;
    Clock::time_point start_{}, last_{};
    std::array<Phase, 12> phases_{};
    size_t count_ = 0;
};

// Store transfer and RPC breakdown diagnostics run on request hot paths.
// Require an explicit opt-in, then apply the process-wide high-frequency
// sampling rate as a second gate.
inline bool ShouldSampleStoreTransferDiagnostics(uint64_t trace_id) {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_STORE_TRANSFER_DIAG");
        if (!value) return false;

        std::string normalized(value);
        std::transform(
            normalized.begin(), normalized.end(), normalized.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return normalized == "1" || normalized == "true" ||
               normalized == "yes" || normalized == "on";
    }();
    return enabled && mooncake::logging::ShouldSampleHiFreqLog(trace_id) &&
           mooncake::logging::ShouldLog(google::INFO);
}

}  // namespace mooncake

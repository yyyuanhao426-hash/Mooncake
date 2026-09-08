// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "scheduler/scheduler_core.h"
#include "scheduler/scheduler_policy.h"
#include "transfer_engine.h"

DEFINE_string(target_seg_name, "", "Target segment printed by tebench");
DEFINE_string(metadata_conn_string, "P2PHANDSHAKE",
              "Metadata connection string");
DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name used by the initiator");
DEFINE_bool(scheduling, false, "Submit through the classic TE scheduler");
DEFINE_uint64(buffer_size, 1ULL << 30,
              "Registered initiator buffer size in bytes");
DEFINE_int32(foreground_threads, 1,
             "Worker count for each foreground request size");
DEFINE_int32(background_threads, 3,
             "Worker count for each background request size");
DEFINE_int32(warmup_seconds, 30, "Warmup duration for each repetition");
DEFINE_int32(duration_seconds, 120, "Measurement duration per repetition");
DEFINE_int32(repetitions, 5, "Number of independent repetitions");
DEFINE_uint64(foreground_deadline_us, 0,
              "Optional relative deadline for foreground requests");
DEFINE_string(output_jsonl, "scheduler-benchmark.jsonl",
              "File to which one JSON record per repetition is appended");
DEFINE_uint64(scheduler_quantum_bytes, 1ULL << 20, "Scheduler byte quantum");
DEFINE_uint64(scheduler_max_inflight_bytes, 16ULL << 20,
              "Scheduler maximum in-flight bytes");
DEFINE_uint64(scheduler_reserved_high_bytes, 1ULL << 20,
              "Scheduler bytes reserved for HIGH traffic");
DEFINE_uint32(scheduler_max_slices, 32,
              "Scheduler maximum transport slices per grant");

namespace {

using Clock = std::chrono::steady_clock;

struct TrafficClass {
    const char* name;
    size_t block_size;
    int threads;
    mooncake::TransferRequest::OpCode opcode;
    mooncake::TaskIntent intent;
    const char* tenant;
};

struct WorkerResult {
    std::vector<double> latency_us;
    uint64_t bytes{0};
    double duration_seconds{0};
};

void check(const mooncake::Status& status, const char* operation) {
    LOG_ASSERT(status.ok()) << operation << " failed: " << status.ToString();
}

double percentile(std::vector<double> samples, double value) {
    if (samples.empty()) return 0.0;
    std::sort(samples.begin(), samples.end());
    const double rank = value / 100.0 * (samples.size() - 1);
    const size_t lower = static_cast<size_t>(rank);
    const size_t upper = std::min(lower + 1, samples.size() - 1);
    const double fraction = rank - lower;
    return samples[lower] * (1.0 - fraction) + samples[upper] * fraction;
}

double runTransfer(mooncake::TransferEngine& engine, mooncake::SegmentID target,
                   void* local, uint64_t remote_offset,
                   const TrafficClass& traffic) {
    const auto batch = engine.allocateBatchID(1);
    mooncake::TransferRequest request;
    request.opcode = traffic.opcode;
    request.source = local;
    request.target_id = target;
    request.target_offset = remote_offset;
    request.length = traffic.block_size;

    const auto started = Clock::now();
    if (FLAGS_scheduling) {
        mooncake::SchedulingHint hint;
        hint.tenant_id = traffic.tenant;
        hint.intent = traffic.intent;
        if (FLAGS_foreground_deadline_us != 0 &&
            traffic.intent == mooncake::TaskIntent::FOREGROUND_GET) {
            hint.deadline_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now().time_since_epoch())
                    .count() +
                FLAGS_foreground_deadline_us * 1000ULL;
        }
        check(engine.submitScheduledTransfer(batch, {{request, hint}}),
              "submitScheduledTransfer");
    } else {
        check(engine.submitTransfer(batch, {request}), "submitTransfer");
    }

    while (true) {
        mooncake::TransferStatus status;
        check(engine.getTransferStatus(batch, 0, status), "getTransferStatus");
        if (status.s == mooncake::TransferStatusEnum::COMPLETED) break;
        LOG_ASSERT(status.s != mooncake::TransferStatusEnum::FAILED &&
                   status.s != mooncake::TransferStatusEnum::TIMEOUT &&
                   status.s != mooncake::TransferStatusEnum::CANCELED)
            << "transfer reached terminal status " << status.s;
        std::this_thread::yield();
    }
    const double elapsed_us =
        std::chrono::duration<double, std::micro>(Clock::now() - started)
            .count();
    check(engine.freeBatchID(batch), "freeBatchID");
    return elapsed_us;
}

void runWorker(mooncake::TransferEngine& engine, mooncake::SegmentID target,
               uint8_t* local_base, uint64_t remote_base, size_t worker_index,
               size_t address_stride, const TrafficClass& traffic,
               WorkerResult& result) {
    void* local = local_base + worker_index * address_stride;
    const uint64_t remote = remote_base + worker_index * address_stride;
    auto until = Clock::now() + std::chrono::seconds(FLAGS_warmup_seconds);
    while (Clock::now() < until)
        runTransfer(engine, target, local, remote, traffic);

    const auto started = Clock::now();
    until = started + std::chrono::seconds(FLAGS_duration_seconds);
    while (Clock::now() < until) {
        result.latency_us.push_back(
            runTransfer(engine, target, local, remote, traffic));
        result.bytes += traffic.block_size;
    }
    result.duration_seconds =
        std::chrono::duration<double>(Clock::now() - started).count();
}

void appendRecord(const std::vector<TrafficClass>& classes,
                  const std::vector<std::vector<WorkerResult>>& results,
                  int repetition) {
    std::ofstream output(FLAGS_output_jsonl, std::ios::app);
    LOG_ASSERT(output) << "cannot open " << FLAGS_output_jsonl;

    double aggregate_throughput = 0.0;
    struct ClassMetrics {
        double p99_us;
        double throughput_gbps;
        uint64_t operations;
    };
    std::vector<ClassMetrics> metrics;
    for (size_t class_index = 0; class_index < classes.size(); ++class_index) {
        std::vector<double> latencies;
        uint64_t bytes = 0;
        uint64_t operations = 0;
        double throughput = 0.0;
        for (const auto& worker : results[class_index]) {
            latencies.insert(latencies.end(), worker.latency_us.begin(),
                             worker.latency_us.end());
            bytes += worker.bytes;
            operations += worker.latency_us.size();
            if (worker.duration_seconds > 0)
                throughput += worker.bytes / 1e9 / worker.duration_seconds;
        }
        aggregate_throughput += throughput;
        metrics.push_back(
            {percentile(std::move(latencies), 99.0), throughput, operations});
    }

    output << std::fixed << std::setprecision(6)
           << "{\"schema_version\":1,\"scheduling\":"
           << (FLAGS_scheduling ? "true" : "false")
           << ",\"repetition\":" << repetition
           << ",\"aggregate_throughput_gbps\":" << aggregate_throughput
           << ",\"classes\":[";
    for (size_t i = 0; i < classes.size(); ++i) {
        if (i != 0) output << ',';
        output << "{\"name\":\"" << classes[i].name
               << "\",\"threads\":" << classes[i].threads
               << ",\"block_size\":" << classes[i].block_size
               << ",\"operations\":" << metrics[i].operations
               << ",\"p99_us\":" << metrics[i].p99_us
               << ",\"throughput_gbps\":" << metrics[i].throughput_gbps << '}';
    }
    output << "]}\n";
    LOG_ASSERT(output) << "failed to write " << FLAGS_output_jsonl;

    std::cout << "repetition=" << repetition << " scheduling=" << std::boolalpha
              << FLAGS_scheduling
              << " aggregate_throughput=" << aggregate_throughput << " GB/s"
              << std::endl;
    for (size_t i = 0; i < classes.size(); ++i)
        std::cout << "  " << classes[i].name << ": p99=" << metrics[i].p99_us
                  << " us throughput=" << metrics[i].throughput_gbps << " GB/s"
                  << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "Classic Transfer Engine scheduler A/B benchmark initiator");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    LOG_ASSERT(!FLAGS_target_seg_name.empty())
        << "--target_seg_name is required";
    LOG_ASSERT(FLAGS_foreground_threads > 0 && FLAGS_background_threads > 0);
    LOG_ASSERT(FLAGS_foreground_threads <= 128 &&
               FLAGS_background_threads <= 128)
        << "thread counts must not exceed 128 per traffic class";
    LOG_ASSERT(FLAGS_warmup_seconds >= 0 && FLAGS_duration_seconds > 0 &&
               FLAGS_repetitions > 0);

    std::vector<TrafficClass> classes = {
        {"foreground-4k", 4ULL << 10, FLAGS_foreground_threads,
         mooncake::TransferRequest::READ, mooncake::TaskIntent::FOREGROUND_GET,
         "online"},
        {"foreground-64k", 64ULL << 10, FLAGS_foreground_threads,
         mooncake::TransferRequest::READ, mooncake::TaskIntent::FOREGROUND_GET,
         "online"},
        {"migration-8m", 8ULL << 20, FLAGS_background_threads,
         mooncake::TransferRequest::WRITE, mooncake::TaskIntent::MIGRATION,
         "batch"},
        {"checkpoint-64m", 64ULL << 20, FLAGS_background_threads,
         mooncake::TransferRequest::WRITE, mooncake::TaskIntent::CHECKPOINT,
         "batch"},
    };

    size_t worker_count = 0;
    for (const auto& traffic : classes) worker_count += traffic.threads;
    const size_t address_stride = 64ULL << 20;
    const size_t required_buffer = worker_count * address_stride;
    LOG_ASSERT(FLAGS_buffer_size <= std::numeric_limits<size_t>::max() &&
               FLAGS_buffer_size % 4096 == 0)
        << "--buffer_size must fit in size_t and be a multiple of 4096";
    LOG_ASSERT(FLAGS_buffer_size >= required_buffer)
        << "--buffer_size must be at least " << required_buffer;

    auto* local_buffer = static_cast<uint8_t*>(
        std::aligned_alloc(4096, static_cast<size_t>(FLAGS_buffer_size)));
    LOG_ASSERT(local_buffer != nullptr) << "failed to allocate local buffer";

    mooncake::TransferEngine engine(true);
    LOG_ASSERT(
        engine.init(FLAGS_metadata_conn_string, FLAGS_local_server_name) == 0)
        << "TransferEngine initialization failed";
    LOG_ASSERT(engine.registerLocalMemory(local_buffer, FLAGS_buffer_size) == 0)
        << "local memory registration failed";

    if (FLAGS_scheduling) {
        mooncake::scheduling::SchedulerConfig config;
        config.quantum_bytes = FLAGS_scheduler_quantum_bytes;
        config.max_inflight_bytes = FLAGS_scheduler_max_inflight_bytes;
        config.reserved_high_bytes = FLAGS_scheduler_reserved_high_bytes;
        config.max_slices = FLAGS_scheduler_max_slices;
        check(engine.configureScheduling(config), "configureScheduling");
    }

    const auto target = engine.openSegment(FLAGS_target_seg_name);
    auto segment = engine.getMetadata()->getSegmentDescByID(target);
    LOG_ASSERT(segment && !segment->buffers.empty())
        << "target segment has no registered buffers";
    const auto& remote_buffer = segment->buffers.front();
    LOG_ASSERT(remote_buffer.length >= required_buffer)
        << "target buffer must be at least " << required_buffer << " bytes";

    for (int repetition = 1; repetition <= FLAGS_repetitions; ++repetition) {
        std::vector<std::vector<WorkerResult>> results;
        results.reserve(classes.size());
        for (const auto& traffic : classes)
            results.emplace_back(traffic.threads);

        std::vector<std::thread> workers;
        size_t worker_index = 0;
        for (size_t class_index = 0; class_index < classes.size();
             ++class_index) {
            for (int class_worker = 0;
                 class_worker < classes[class_index].threads;
                 ++class_worker, ++worker_index) {
                workers.emplace_back(
                    runWorker, std::ref(engine), target, local_buffer,
                    remote_buffer.addr, worker_index, address_stride,
                    std::cref(classes[class_index]),
                    std::ref(results[class_index][class_worker]));
            }
        }
        for (auto& worker : workers) worker.join();
        appendRecord(classes, results, repetition);
    }

    engine.closeSegment(target);
    engine.unregisterLocalMemory(local_buffer);
    engine.freeEngine();
    std::free(local_buffer);
    return 0;
}

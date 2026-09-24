// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <atomic>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <string>
#include <utility>
#include "config.h"
#include "cuda_alike.h"
#include "memory_location.h"
#include <cassert>
#include "ub_allocator.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transport/kunpeng_transport/ub_endpoint.h"
#include "transport/kunpeng_transport/urma/urma_endpoint.h"
#include "mooncake_logging.h"

namespace mooncake {
namespace {
constexpr uint64_t kNumaAffinitySampleInterval = 10000;
constexpr uint64_t kSubmitLogSampleInterval = 10000;

}  // namespace

UbTransport::UbTransport(UB_ENDPOINT_TYPE endpoint_type)
    : endpoint_type_(endpoint_type) {}

UbTransport::~UbTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

UbGpuMode UbTransport::gpuMode() const {
    std::call_once(gpu_mode_once_, [this] {
        const char* env = std::getenv("MC_UB_GPU_MODE");
        if (env) {
            std::string mode(env);
            if (mode == "host") {
                gpu_mode_ = UbGpuMode::kHost;
            } else if (mode == "staging") {
                gpu_mode_ = UbGpuMode::kStaging;
            } else if (mode == "gdr-peermem") {
                gpu_mode_ = UbGpuMode::kGdrPeermem;
            } else {
                LOG(WARNING) << "Unknown MC_UB_GPU_MODE=\"" << mode
                             << "\", fallback to staging";
                gpu_mode_ = UbGpuMode::kStaging;
            }
            LOG(INFO) << "UbTransport GPU mode: " << gpuModeName();
            return;
        }

        const char* env_legacy = std::getenv("MC_UB_TRANSPORT_CPU_STAGING");
        gpu_mode_ = (!env_legacy || std::string(env_legacy) != "0")
                        ? UbGpuMode::kStaging
                        : UbGpuMode::kHost;
        LOG(INFO) << "UbTransport GPU mode: " << gpuModeName()
                  << " (derived from MC_UB_TRANSPORT_CPU_STAGING)";
    });
    return gpu_mode_;
}

const char* UbTransport::gpuModeName() const {
    switch (gpu_mode_) {
        case UbGpuMode::kHost: return "host";
        case UbGpuMode::kStaging: return "staging";
        case UbGpuMode::kGdrPeermem: return "gdr-peermem";
    }
    return "unknown";
}

bool UbTransport::isDevicePointer(const void* ptr) const {
    if (!ptr) return false;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, ptr);
    if (status != cudaSuccess) {
        return false;
    }
    return attributes.type == cudaMemoryTypeDevice;
#else
    return false;
#endif
}

bool UbTransport::copyDeviceToHost(void* dst, const void* src,
                                   size_t size) const {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost) == cudaSuccess;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

bool UbTransport::copyHostToDevice(void* dst, const void* src,
                                   size_t size) const {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice) == cudaSuccess;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

bool UbTransport::shouldDeferSuccess(Slice* slice) {
    std::lock_guard<std::mutex> lock(staged_read_mutex_);
    const bool deferred =
        staged_read_slices_.find(slice) != staged_read_slices_.end();
    static std::atomic<uint64_t> defer_log_counter{0};
    if (VLOG_IS_ON(2) && defer_log_counter.fetch_add(
            1, std::memory_order_relaxed) % kSubmitLogSampleInterval == 0) {
        VLOG(2) << "[ub_staging] shouldDeferSuccess slice=" << slice
                << " deferred=" << deferred;
    }
    return deferred;
}

void UbTransport::onStagedSliceSuccess(Slice* slice) {
    void* gpu_dst = nullptr;
    {
        std::lock_guard<std::mutex> lock(staged_read_mutex_);
        auto it = staged_read_slices_.find(slice);
        if (it == staged_read_slices_.end()) {
            slice->markSuccess();
            return;
        }
        gpu_dst = it->second;
        staged_read_slices_.erase(it);
    }
    if (!copyHostToDevice(gpu_dst, slice->source_addr, slice->length)) {
        LOG(ERROR) << "UbTransport: H2D staging copy failed for READ, size="
                   << slice->length << " dst=" << gpu_dst;
        slice->markFailed();
        return;
    }
    static std::atomic<uint64_t> h2d_log_counter{0};
    if (VLOG_IS_ON(2) && h2d_log_counter.fetch_add(
            1, std::memory_order_relaxed) % kSubmitLogSampleInterval == 0) {
        VLOG(2) << "[ub_staging] H2D done gpu_dst=" << gpu_dst
                << " len=" << slice->length;
    }
    slice->markSuccess();
}

void UbTransport::onStagedSliceFinalFailure(Slice* slice) {
    {
        std::lock_guard<std::mutex> lock(staged_read_mutex_);
        staged_read_slices_.erase(slice);
    }
    slice->markFailed();
}

int UbTransport::install(std::string& local_server_name,
                         std::shared_ptr<TransferMetadata> meta,
                         std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "UbTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }
    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;
    auto ret = initializeUbResources(this);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot initialize Ub resources";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: initialize Ub resources done";

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "Transfer engine allocate local segment done";

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot start handshake daemon";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: start handshake daemon done";

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot publish segments";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: publish segments done";

    return 0;
}

int UbTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& name,
                                      bool remote_accessible,
                                      bool update_metadata) {
    const bool is_device_ptr = isDevicePointer(addr);
    const auto mode = gpuMode();
    VLOG(1) << "[ub_register] addr=" << addr << " len=" << length
            << " mode=" << gpuModeName() << " is_device=" << is_device_ptr;
    if (mode == UbGpuMode::kGdrPeermem && !is_device_ptr) {
        LOG(ERROR) << "gdr-peermem requires device memory, addr=" << addr;
        return ERR_INVALID_ARGUMENT;
    }
    if (is_device_ptr) {
        switch (mode) {
            case UbGpuMode::kHost:
                LOG(ERROR) << "refusing device memory in host mode, addr="
                           << addr;
                return ERR_INVALID_ARGUMENT;
            case UbGpuMode::kStaging: {
                // Per-buffer staging: allocate host barbuffer, register to URMA,
                // record GPU_VA -> host mapping. GPU buffer itself is not registered.
                void* host = ub_allocate_memory(4096, length);
                if (!host) {
                    LOG(ERROR) << "failed to allocate staging host buffer for "
                               << addr;
                    return ERR_MEMORY;
                }
                int ret = registerLocalMemory(host, length, kWildcardLocation,
                                              false, true);
                if (ret) {
                    ub_free_memory(host);
                    return ret;
                }
                std::lock_guard<std::mutex> lock(staging_map_mutex_);
                staging_map_[addr] = {addr, host, length};
                VLOG(1) << "[ub_staging] mapped gpu_va=" << addr
                        << " -> host=" << host << " len=" << length;
                return 0;
            }
            case UbGpuMode::kGdrPeermem:
                VLOG(1) << "[ub_gdr] registering GPU region addr=" << addr;
                break;
        }
    }

    const auto region_type = is_device_ptr ? UbMemoryRegionType::kGpu
                                           : UbMemoryRegionType::kHost;
    BufferDesc buffer_desc;
    for (auto& context : context_list_) {
        int ret = context->registerMemoryRegion((uint64_t)addr, length,
                                                region_type);
        if (ret) {
            LOG(ERROR) << "UbTransport: cannot register LocalMemory";
            return ret;
        }
        ret = context->buildLocalBufferDesc((uint64_t)addr, buffer_desc);
        if (ret) {
            LOG(ERROR) << "UbTransport: build buffer description failed";
            return ret;
        }
    }
    VLOG(1) << "[ub_register] URMA region addr=" << addr << " type="
            << (region_type == UbMemoryRegionType::kGpu ? "kGpu" : "kHost");

    if (name == kWildcardLocation) {
        bool only_first_page = true;
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length, only_first_page);
        if (entries.empty()) {
            for (auto& context : context_list_)
                context->unregisterMemoryRegion((uint64_t)addr);
            return -1;
        }
        buffer_desc.name = entries[0].location;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        int node = parseCpuNumaNode(buffer_desc.name);
        if (node >= 0) buffer_desc.chip_id = numaNodeToChipId(node);
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
        if (rc) {
            for (auto& context : context_list_)
                context->unregisterMemoryRegion((uint64_t)addr);
            return rc;
        }
    } else {
        buffer_desc.name = name;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        int node = parseCpuNumaNode(buffer_desc.name);
        if (node >= 0) buffer_desc.chip_id = numaNodeToChipId(node);
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
        if (rc) {
            for (auto& context : context_list_)
                context->unregisterMemoryRegion((uint64_t)addr);
            return rc;
        }
    }

    return 0;
}

int UbTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    const bool is_device_ptr = isDevicePointer(addr);
    const auto mode = gpuMode();
    VLOG(1) << "[ub_unregister] addr=" << addr << " mode=" << gpuModeName()
            << " is_device=" << is_device_ptr;
    if (is_device_ptr && mode == UbGpuMode::kStaging) {
        // Staging: unregister the host staging buffer, free it, remove mapping.
        StagingMapping m;
        {
            std::lock_guard<std::mutex> lock(staging_map_mutex_);
            auto it = staging_map_.find(addr);
            if (it == staging_map_.end()) return ERR_ADDRESS_NOT_REGISTERED;
            m = it->second;
            staging_map_.erase(it);
        }
        VLOG(1) << "[ub_staging] unmapping gpu_va=" << addr
                << " host=" << m.host_ptr;
        unregisterLocalMemory(m.host_ptr, true);
        ub_free_memory(m.host_ptr);
        return 0;
    }
    VLOG(1) << "[ub_unregister] URMA region addr=" << addr;
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;
    for (auto& context : context_list_)
        context->unregisterMemoryRegion((uint64_t)addr);
    return 0;
}

int UbTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    std::vector<std::future<int>> results;
    results.reserve(buffer_list.size());
    for (auto& buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemory(buffer.addr, buffer.length, location,
                                           true, false);
            }));
    }

    int first_error = 0;
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        int ret = results[i].get();
        if (ret) {
            LOG(WARNING) << "UbTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
            if (!first_error) first_error = ret;
        }
    }
    if (first_error) return first_error;

    return metadata_->updateLocalSegmentDesc();
}

int UbTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    std::vector<std::future<int>> results;
    results.reserve(addr_list.size());
    for (auto& addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemory(addr, false);
            }));
    }

    int first_error = 0;
    for (size_t i = 0; i < addr_list.size(); ++i) {
        int ret = results[i].get();
        if (ret) {
            LOG(WARNING) << "UbTransport: Failed to unregister memory: addr "
                         << addr_list[i];
            if (!first_error) first_error = ret;
        }
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

Status UbTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UbTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "UbTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask*> task_list;
    task_list.reserve(entries.size());
    for (auto& request : entries) {
        auto& task = batch_desc.task_list[task_id];
        ++task_id;
        task.batch_id = batch_id;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<TransferRequest*>(&request);
#else
        task.request = &request;
#endif
        task_list.push_back(&task);
    }
    return submitTransferTask(task_list);
}

Status UbTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::unordered_map<std::shared_ptr<UbContext>, std::vector<Slice*>>
        slices_to_post;
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit;
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;
    uint64_t nr_slices;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        nr_slices = 0;
        assert(task.request);
        auto& request = *task.request;
        void* effective_source = request.source;
        bool staged_read = false;
        void* original_gpu_ptr = nullptr;

        const bool is_device_source = isDevicePointer(request.source);
        const auto mode = gpuMode();
        static std::atomic<uint64_t> submit_log_counter{0};
        if (VLOG_IS_ON(2) && submit_log_counter.fetch_add(
                1, std::memory_order_relaxed) % kSubmitLogSampleInterval == 0) {
            VLOG(2) << "[ub_transfer] mode=" << gpuModeName()
                    << " is_device_source=" << is_device_source
                    << " opcode=" << (request.opcode == TransferRequest::READ
                                          ? "READ"
                                          : "WRITE")
                    << " source=" << request.source
                    << " effective_source=" << effective_source
                    << " length=" << request.length;
        }
        if (mode == UbGpuMode::kGdrPeermem && !is_device_source) {
            return Status::InvalidArgument(
                "UbTransport: gdr-peermem requires device source pointer");
        }
        if (is_device_source && mode != UbGpuMode::kGdrPeermem) {
            if (mode == UbGpuMode::kHost) {
                return Status::InvalidArgument(
                    "UbTransport: device pointer is not allowed in host mode");
            }
            // kStaging: route through the per-buffer host staging buffer.
            // The GPU VA may point into the middle of a registered region
            // (e.g. batch reads use base + k*value_size), so do a range lookup
            // and preserve the intra-region offset in the host mapping.
            StagingMapping m;
            {
                std::lock_guard<std::mutex> lock(staging_map_mutex_);
                auto it = staging_map_.upper_bound(request.source);
                if (it != staging_map_.begin()) {
                    --it;
                    m = it->second;
                }
                if (!m.gpu_base || request.source < m.gpu_base ||
                    reinterpret_cast<char*>(request.source) >=
                        reinterpret_cast<char*>(m.gpu_base) + m.size) {
                    return Status::AddressNotRegistered(
                        "UbTransport: device pointer is not registered for "
                        "staging, address: " +
                        std::to_string(
                            reinterpret_cast<uintptr_t>(request.source)));
                }
            }
            const size_t staging_offset =
                reinterpret_cast<char*>(request.source) -
                reinterpret_cast<char*>(m.gpu_base);
            effective_source =
                reinterpret_cast<char*>(m.host_ptr) + staging_offset;
            original_gpu_ptr = request.source;
            if (request.opcode == TransferRequest::WRITE) {
                if (!copyDeviceToHost(effective_source, request.source,
                                      request.length)) {
                    LOG(ERROR)
                        << "UbTransport: D2H staging copy failed for WRITE, "
                           "size="
                        << request.length << " src=" << request.source;
                    return Status::Memory(
                        "UbTransport: D2H staging copy failed");
                }
                if (VLOG_IS_ON(2) && submit_log_counter.load(
                        std::memory_order_relaxed) % kSubmitLogSampleInterval == 0) {
VLOG(2) << "[ub_staging] D2H done gpu=" << request.source
                        << " -> host=" << effective_source
                        << " len=" << request.length;
                }
            } else {
                staged_read = true;  // defer success until H2D
            }
        } else if (mode == UbGpuMode::kGdrPeermem && is_device_source) {
            if (VLOG_IS_ON(2) && submit_log_counter.load(
                    std::memory_order_relaxed) % kSubmitLogSampleInterval == 0) {
                VLOG(2) << "[ub_gdr] direct source gpu_va=" << request.source
                        << " len=" << request.length;
            }
        }

        auto local_segment_desc =
            metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        auto request_buffer_id = -1, request_device_id = -1;

        if (selectDevice(local_segment_desc.get(), (uint64_t)effective_source,
                         request.length, request_buffer_id,
                         request_device_id)) {
            request_buffer_id = -1;
            request_device_id = -1;
        }

        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice* slice = getSliceCache().allocate();
            assert(slice);
            if (!slice->from_cache) {
                nr_slices++;
            }
            slice->peer_nic_path.clear();
            slice->dest_rkeys.clear();
            bool merge_final_slice =
                request.length - offset <= kBlockSize + kFragmentSize;
            slice->source_addr = (char*)effective_source + offset;
            slice->length =
                merge_final_slice ? request.length - offset : kBlockSize;
            slice->opcode = request.opcode;
            slice->trace_id = mooncake::logging::CurrentTraceId();
            // LOG(INFO) << "target_offset : " << request.target_offset << ",
            // offset : " << offset;
            slice->ub.dest_addr = request.target_offset + offset;
            slice->ub.retry_cnt = 0;
            slice->ub.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->ts = 0;
            slice->status = Slice::PENDING;
            slice->ub.src_chip_id = INVALID_CHIP_ID;
            slice->ub.dst_chip_id = INVALID_CHIP_ID;
            task.slice_list.push_back(slice);
            if (staged_read) {
                uint64_t slice_offset = static_cast<char*>(slice->source_addr) -
                                  static_cast<char*>(effective_source);
                void* gpu_dst = static_cast<char*>(original_gpu_ptr) + slice_offset;
                std::lock_guard<std::mutex> lock(staged_read_mutex_);
                staged_read_slices_[slice] = gpu_dst;
                static std::atomic<uint64_t> staged_read_log_counter{0};
                if (VLOG_IS_ON(2) && staged_read_log_counter.fetch_add(
                        1, std::memory_order_relaxed) %
                        kSubmitLogSampleInterval == 0) {
                    VLOG(2) << "[ub_staging] staged READ recorded slice="
                            << slice << " gpu_dst=" << gpu_dst
                            << " len=" << slice->length;
                }
            }

            int buffer_id = -1, device_id = -1,
                retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            if (request_buffer_id >= 0 && request_device_id >= 0) {
                found_device = true;
                buffer_id = request_buffer_id;
                device_id = request_device_id;
            }
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                assert(device_id >= 0 &&
                       static_cast<size_t>(device_id) < context_list_.size());
                auto& context = context_list_[device_id];
                assert(context.get());
                if (!context->active()) continue;
                assert(buffer_id >= 0 &&
                       static_cast<size_t>(buffer_id) <
                           local_segment_desc->buffers.size());
                assert(local_segment_desc->buffers[buffer_id].tseg.size() ==
                       context_list_.size());
                found_device = true;
                break;
            }
            if (device_id < 0) {
                auto source_addr = slice->source_addr;
                // Do not deallocate slices already queued in slices_to_post
                // here: every slice is also recorded in its owning
                // TransferTask::slice_list right after allocation, and
                // ~TransferTask() returns everything in slice_list to the
                // cache exactly once. Deallocating here double-frees them
                // into ThreadLocalSliceCache, letting a later allocate()
                // hand the same Slice* to two unrelated transfers.
                LOG(ERROR)
                    << "UbTransport: Address not registered by any device(s) "
                    << source_addr;
                if (staged_read) {
                    // Clean up staged-read mappings recorded for this task;
                    // these slices will never reach the H2D completion path.
                    std::lock_guard<std::mutex> lock(staged_read_mutex_);
                    for (Slice* s : task.slice_list) {
                        staged_read_slices_.erase(s);
                    }
                }
                if (task.scheduled) {
                    // Earlier watermark batches may already be in flight.
                    // Complete only the unposted suffix here; the scheduler
                    // retains the grant until that prefix also drains.
                    __sync_fetch_and_add(&task.slice_count, 1);
                    slice->markFailed();
                    for (auto& pending : slices_to_post)
                        for (auto* unposted : pending.second)
                            unposted->markFailed();
                    slices_to_post.clear();
                }
                return Status::AddressNotRegistered(
                    "UbTransport: not registered by any device(s), "
                    "address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }
            // start to submit batch request task
            auto& context = context_list_[device_id];
            if (!context->active()) {
                LOG(ERROR) << "Device " << device_id << " is not active";
                if (staged_read) {
                    // Clean up staged-read mappings recorded for this task;
                    // these slices will never reach the H2D completion path.
                    std::lock_guard<std::mutex> lock(staged_read_mutex_);
                    for (Slice* s : task.slice_list) {
                        staged_read_slices_.erase(s);
                    }
                }
                return Status::InvalidArgument(
                    "Device " + std::to_string(device_id) + " is not active");
            }
            auto local_tseg_index =
                local_segment_desc->buffers[buffer_id].l_seg_index[device_id];
            slice->ub.l_seg = context->localSegWithIndex(local_tseg_index);
            if (context->numa_affinity()) {
                const auto& local_buf = local_segment_desc->buffers[buffer_id];
                int data_numa = parseCpuNumaNode(local_buf.name);
                if (local_buf.chip_id >= 0) {
                    // Prefer the chip id published at registration.
                    slice->ub.src_chip_id = (uint8_t)local_buf.chip_id;
                } else {
                    // Each UB buffer belongs to one NUMA node and is named
                    // "cpu:N"; no offset-based segment lookup is required.
                    slice->ub.src_chip_id = numaNodeToChipId(data_numa);
                }
                static std::atomic<uint64_t> numa_log_counter{0};
                if (VLOG_IS_ON(2) &&
                    numa_log_counter.fetch_add(1, std::memory_order_relaxed) %
                            kNumaAffinitySampleInterval ==
                        0) {
                    VLOG(2)
                        << "[numa_affinity] local_sample trace_id="
                        << slice->trace_id << " target_id=" << slice->target_id
                        << " opcode="
                        << (slice->opcode == Transport::TransferRequest::READ
                                ? "READ"
                                : "WRITE")
                        << " local_data_numa=" << data_numa
                        << " src_chip=" << (int)slice->ub.src_chip_id
                        << " local_name=" << local_buf.name;
                }
            }
            slices_to_post[context].push_back(slice);
             task.total_bytes += slice->length;
            __sync_fetch_and_add(&task.slice_count, 1);
            if (nr_slices >= kSubmitWatermark) {
                for (auto& entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);
                slices_to_post.clear();
                nr_slices = 0;
            }

            if (merge_final_slice) {
                break;
            }
        }
    }
    for (auto& entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status UbTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                      TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UbTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count)
            status.s = FAILED;
        else
            status.s = COMPLETED;
        task.is_finished = true;
    } else {
        status.s = WAITING;
    }
    return Status::OK();
}

Status UbTransport::scheduledTransferLength(const TransferRequest& request,
                                            uint32_t max_slices,
                                            size_t& length) {
    const uint64_t block = globalConfig().slice_size;
    if (!block || !max_slices)
        return Status::InvalidArgument("Invalid UB slice budget");
    length = request.length;
    if (uint64_t(max_slices) <= UINT64_MAX / block)
        length = std::min<uint64_t>(length, block * max_slices);
    return Status::OK();
}

Transport::SegmentID UbTransport::getSegmentID(
    const std::string& segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int UbTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ub";
    for (auto& context : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = context->deviceName();
        device_desc.eid = context->getEid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_);
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int UbTransport::onSetupConnections(const HandShakeDesc& peer_desc,
                                    HandShakeDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<UbContext> context;
    int index = 0;
    for (auto& entry : local_topology_->getHcaList()) {
        if (entry == local_nic_name) {
            context = context_list_[index];
            break;
        }
        index++;
    }
    if (!context) return ERR_INVALID_ARGUMENT;

#ifdef CONFIG_ERDMA
    if (context->deleteEndpoint(peer_desc.local_nic_path)) return ERR_ENDPOINT;
#endif
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    return endpoint->setupConnectionsByPassive(peer_desc, local_desc);
}

int UbTransport::startHandshakeDaemon(std::string& local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&UbTransport::onSetupConnections, this, std::placeholders::_1,
                  std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              int& buffer_id, int& device_id, int retry_cnt) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_cnt);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              std::string_view hint, int& buffer_id,
                              int& device_id, int retry_cnt) {
    if (desc == nullptr) {
        LOG(ERROR) << "UbTransport Get Segment Desc failed";
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    const auto& buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto& buffer = buffers[buffer_id];
        // Check if offset is within buffer range
        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;
        }

        // UB memory is allocated and mounted as one independent segment per
        // NUMA node. Its BufferDesc name is already the resolved location
        // ("cpu:N"), so no offset-based segments-location lookup is needed.
        const std::string& location = buffer.name;
        device_id =
            hint.empty()
                ? desc->topology.selectDevice(location, retry_cnt)
                : desc->topology.selectDevice(location, hint, retry_cnt);
        if (device_id >= 0) return 0;
        device_id = hint.empty() ? desc->topology.selectDevice(
                                       kWildcardLocation, retry_cnt)
                                 : desc->topology.selectDevice(
                                       kWildcardLocation, hint, retry_cnt);
        if (device_id >= 0) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int UbTransport::initializeUbResources(UbTransport* t) {
    auto ret = init(t);
    if (ret != 0) {
        LOG(ERROR) << "Failed to init, ret = " << ret;
        return -1;
    }

    std::vector<std::string> hca_list;
    // Try to get device list from topology
    if (t->local_topology_) {
        hca_list = t->local_topology_->getHcaList();
    }

    // If no devices from topology, use mock device
    if (hca_list.empty()) {
        hca_list.push_back("mock_urma_device");
        LOG(INFO) << "Using mock_urma_device for testing";
    }

    for (auto& device_name : hca_list) {
        auto& config = globalConfig();
        auto max_endpoints = config.max_ep_per_ctx;
        auto context = buildContext(t, device_name, max_endpoints);
        ret = context->doConstruct(config);
        if (ret) {
            if (t->local_topology_) {
                t->local_topology_->disableDevice(device_name);
            }
            LOG(WARNING) << "Disable device " << device_name;
        } else {
            t->context_list_.push_back(context);
            LOG(INFO) << "device " << context->deviceName() << " add to list";
        }
    }

    if (t->context_list_.empty()) {
        LOG(ERROR) << "UbTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }

    LOG(INFO) << "ub resources init success";
    return 0;
}

int UbTransport::init(UbTransport* transport) {
    if (transport->runtime_initialized_) return 0;

    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        if (!UrmaContext::init()) {
            LOG(ERROR) << "UrmaContext init failed";
            return -1;
        }
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return -1;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
        return -1;
    }
    transport->runtime_initialized_ = true;
    return 0;
}

void UbTransport::uninit(UbTransport* transport) {
    transport->context_list_.clear();
    if (!transport->runtime_initialized_) return;

    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        if (!UrmaContext::uninit()) {
            LOG(ERROR) << "UrmaContext uninit failed";
        }
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
    }
    transport->runtime_initialized_ = false;
}

std::shared_ptr<UbContext> UbTransport::buildContext(
    UbTransport* t, const std::string& device_name, int max_endpoints) {
    if (t->endpoint_type_ == URMA_ENDPOINT) {
        auto context =
            std::make_shared<UrmaContext>(*t, device_name, max_endpoints);
        if (!context) {
            LOG(ERROR) << "UrmaContext build failed";
            return nullptr;
        }
        return context;
    } else if (t->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return nullptr;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << t->endpoint_type_;
        return nullptr;
    }
}
}  // namespace mooncake

# Mooncake UB GDR Deployment Guide

This guide covers building and deploying Mooncake's UB transport with GPU Direct
RDMA (GDR) support, and benchmarking cross-node GDR + remote-SSD data transfer
using `stress_cluster_bench`.

## 1. Test Object & Architecture

### Test objective

Measure cross-node GDR transfer from a remote SSD to GPU HBM:

- A **writer** process on the SSD node prefills data and offloads it to local
  SSD via eviction.
- A **reader** process on the GPU node pulls SSD data through the GDR link and
  lands it directly into GPU HBM (gdr-peermem) or via a host staging buffer
  (staging).

### Deployment topology (remote_disk scenario only)

```
                              Control plane (no KV data)
                         +-----------------------------+
                         |       mooncake_master       |
                         | gRPC :50051   HTTP metadata |
                         |        metrics :9003        |
                         +--------------+--------------+
                                        |
                           +------------+------------+
                           |        DATA PLANE       |
                           |                         |
               +-----------------------+    +-----------------------+
               | writer (SSD node)     |    | reader (GPU node)     |
               | role = writer         |    | role = reader         |
               | write data into SSD   |    | gpu_mode =            |
               | (as beginning)        |    |   staging|gdr-peermem |
               +-----------------------+    +-----------------------+
                           |                         |
                           +------------+------------+
```

## 2. Prerequisites

- **UMDK user-mode lib** with `urma_seg_cfg_t::is_gpu_seg` field (e.g. the
  `UMDK_tool_netlab` source tree). The build probes this field and aborts
  (FATAL_ERROR) if missing. Headers are discovered by `FindUrma.cmake` in
  this order: `-DURMA_ROOT` → `$URMA_ROOT` (env) → `-DUMDK_ROOT` →
  `$UMDK_ROOT` (env) → `/usr/include/ub` → `/usr/include`; if none has the
  headers, UMDK `v25.12.0.B081` is fetched via FetchContent (that tag may
  lack `is_gpu_seg`, so prefer an explicit UMDK tree or an installed SDK).
- **UMDK kernel-mode libs** (ubcore / udma driver modules matching the UMDK
  version).
- **oe-kernel-6.6.0** with user-defined IOMMU / UMMU to adapt GPU direct access.
- **CUDA toolkit and GPU driver** — required for `USE_CUDA` builds
  (`cuMemAlloc`, `cuPointerSetAttribute`, `cudaMemcpy`).
- **liburma.so** — provide the exact path via `-DURMA_LIBRARY`, or let CMake
  `find_library` search `/usr/lib64 /usr/local/lib64 /usr/lib`. If none is
  found, a mock URMA backend is built (GDR unsupported).

## 3. Compilation

```bash
cd <Mooncake-root>
mkdir -p build && cd build
cmake .. \
  -DUSE_UB=ON \
  -DUSE_CUDA=ON \
  -DUSE_URING=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=OFF \
  -DSTORE_USE_ETCD=OFF \
  -DWITH_TE=ON \
  -DWITH_STORE=ON \
  -DWITH_STORE_RUST=OFF \
  -DWITH_STORE_GO=OFF \
  -DBUILD_BENCHMARK=ON \
  -DBUILD_UNIT_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_SKIP_RPATH=ON \
  -DCMAKE_BUILD_RPATH="" \
  -DCMAKE_INSTALL_RPATH="" \
  -DURMA_ROOT=<umdk>/src/urma/lib/urma/core/include \
  -DUMDK_ROOT=<umdk>/src/urma/lib/urma/bond/include \
  -DURMA_LIBRARY=/path/to/liburma.so

# USE_UB: enable UB protocol transport (required for GDR)
# USE_CUDA: enable NVIDIA GPU support (cuMemAlloc / cudaMemcpy)
# USE_URING: enable io_uring-based async file I/O with O_DIRECT (bypasses OS
#            page cache); strongly recommended when the SSD offload path is
#            backed by NVMe (Gen4/Gen5). Without this flag the writer falls
#            back to synchronous preadv, which limits SSD read throughput.
# URMA_ROOT: directory that directly contains urma_api.h
#            (e.g. <umdk>/src/urma/lib/urma/core/include)
# UMDK_ROOT: directory that directly contains urma_ubagg.h
#            (e.g. <umdk>/src/urma/lib/urma/bond/include)
#            FindUrma.cmake searches URMA_ROOT -> $URMA_ROOT -> UMDK_ROOT ->
#            $UMDK_ROOT -> /usr/include/ub -> /usr/include, then falls back to
#            FetchContent (UMDK v25.12.0.B081; may lack is_gpu_seg). With an
#            SDK installed under /usr/include/ub both flags can be omitted.
#            The discovered dirs are exported as urma_INCLUDE_DIR.
# URMA_LIBRARY: path to liburma.so file; empty falls back to find_library, then mock
# CMAKE_SKIP_RPATH=ON: disable RPATH so the binary relies on LD_LIBRARY_PATH for
#                      shared library resolution (prevents stale build-machine paths)

make -j$(nproc) stress_cluster_bench mooncake_master mooncake_client

# Build artifact locations:
# build/mooncake-store/benchmarks/stress_cluster_bench
# build/mooncake-store/src/mooncake_master
# build/mooncake-store/src/mooncake_client
```

- The configure step probes `urma_seg_cfg_t::is_gpu_seg` via
  `check_cxx_source_compiles` and aborts (FATAL_ERROR) if the UMDK headers
  lack the field. Point `URMA_ROOT` at the directory containing `urma_api.h`
  and `UMDK_ROOT` at the directory containing `urma_ubagg.h` from a tree that
  has the field (e.g. `UMDK_tool_netlab`: `.../lib/urma/core/include` and
  `.../lib/urma/bond/include`), or install the SDK headers to
  `/usr/include/ub` and omit both flags.

## 4. Deployment

Start in order: master -> writer -> reader. The writer must finish prefill +
offload before the reader starts.

### Step 1 — master (any node)

```bash
mooncake_master \
  --rpc_address=0.0.0.0 \
  --port=50051 \
  --metrics_port=9003 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --enable_offload=true \
  --offload_on_evict=true \
  --eviction_high_watermark_ratio=0.01 \
  --eviction_ratio=0.00 \
  --logtostderr=true
```

### Step 2 — writer (SSD node)

The writer allocates a FileStorage `ClientBuffer` for SSD read staging (default 1.25 GiB,
configurable via `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES`). With batch- or
multi-thread reads the default may cause `BUFFER_OVERFLOW`; set it to at least
`(1 + num_threads) × batch_size × value_size`, or larger.

io_uring (`MOONCAKE_OFFLOAD_USE_URING=true`) is **strongly recommended** when
the SSD offload path is backed by NVMe. It enables O_DIRECT (bypasses the OS
page cache) and allows the FileStorage backend to submit multiple I/Os in a
single io_uring batch, saturating NVMe queue depth. Without it the writer falls
back to synchronous `preadv`, limiting throughput.

```bash
# --- Workload parameters (writer and reader MUST use the same values) ---
VALUE_SIZE=4194304           # 4MB per key
NUM_KEYS=2048                # total keys
# writer global_segment_size = VALUE_SIZE * NUM_KEYS (8GB) — large enough to hold all data
SEGMENT_SIZE=$((VALUE_SIZE * NUM_KEYS))

MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=8589934592  \
MOONCAKE_OFFLOAD_USE_URING=true                       \
MC_OFFLOAD_RPC_THREAD_NUM=16                          \
stress_cluster_bench \
  --scenario=remote_disk \
  --role=writer \
  --protocol=ub \
  --device_name=bonding_dev_0 \
  --local_hostname=<SSD_NODE_IP>:12345 \
  --metadata_server=http://<MASTER_IP>:8080/metadata \
  --master_server=<MASTER_IP>:50051 \
  --global_segment_size=${SEGMENT_SIZE} \
  --local_buffer_size=0 \
  --value_size=${VALUE_SIZE} \
  --num_keys=${NUM_KEYS} \
  --batch_size=1 \
  --replica_num=1 \
  --enable_ssd_offload=true \
  --ssd_offload_path=</mnt/your-nvme> \
  --wait_seconds=300 \
  --logtostderr=true
```

- Writer uses host buffers only (`--gpu_mode` must be empty/`host`).
- `</mnt/your-nvme>` must exist and be writable before launch.
- `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES`: FileStorage ClientBuffer size;
  raise above the default (1.25 GiB) to avoid BUFFER_OVERFLOW under concurrency.
- `MOONCAKE_OFFLOAD_USE_URING=true`: enable io_uring + O_DIRECT for SSD reads
  (requires `-DUSE_URING=ON` at build time).
- `MC_OFFLOAD_RPC_THREAD_NUM=16`: coro_rpc threads serving `batch_get_offload_object`
  RPCs; increase from the default (8) to match reader thread count.

### Step 3 — reader (GPU node, after writer finished)

```bash
# --- Workload parameters (MUST match writer's VALUE_SIZE and NUM_KEYS) ---
VALUE_SIZE=4194304           # 4MB per key (must match writer)
NUM_KEYS=2048                # total keys (must match writer)

# --- Reader tuning (adjust per test scenario, see Recommended values) ---
BATCH_SIZE=1                 # 1 for latency; 32/64/128/+ for bandwidth
NUM_THREADS=1                # 1 for latency; 4/8/16 for bandwidth (binds to NUMA nodes)
GPU_MODE=gdr-peermem         # gdr-peermem | staging (reader GPU link mode; empty=host)
GPU_DEVICE=0                 # CUDA device index

# MC_OFFLOAD_PUSH=true: push mode (writer urma_write into reader VA).
# Unset or =false: pull mode (reader urma_read from writer DDR, default).
MC_OFFLOAD_PUSH=true \
stress_cluster_bench \
  --scenario=remote_disk \
  --role=reader \
  --protocol=ub \
  --device_name=bonding_dev_0 \
  --gpu_mode=${GPU_MODE} \
  --gpu_device=${GPU_DEVICE} \
  --local_hostname=<GPU_NODE_IP>:12346 \
  --metadata_server=http://<MASTER_IP>:8080/metadata \
  --master_server=<MASTER_IP>:50051 \
  --global_segment_size=33554432 \
  --local_buffer_size=0 \
  --value_size=${VALUE_SIZE} \
  --num_keys=${NUM_KEYS} \
  --batch_size=${BATCH_SIZE} \
  --num_threads=${NUM_THREADS} \
  --wait_seconds=10 \
  --verify=true \
  --logtostderr=true
```

For the staging path instead of gdr-peermem, set `--gpu_mode=staging`.

## 5. Parameter Reference

### Common flags

| Flag | Meaning |
|------|---------|
| `--scenario=remote_disk` | cross-node SSD read scenario |
| `--role=writer\|reader` | writer prefill, reader benchmark |
| `--protocol=ub` | UB transport (required for GDR) |
| `--device_name` | UB device (comma-separated) |
| `--local_hostname=IP:PORT` | local endpoint |
| `--metadata_server` | master HTTP metadata URL |
| `--master_server` | master gRPC address |
| `--value_size`, `--num_keys`, `--batch_size` | workload shape |
| `--wait_seconds` | wait for peer / offload / eviction |

### GPU flags (reader only)

| Flag | Values | Notes |
|------|--------|-------|
| `--gpu_mode` | `gdr-peermem` \| `staging` \| (empty=host) | gdr-direct/proxy are reserved and FATAL |
| `--gpu_device` | int (default 0) | validated against `cudaGetDeviceCount` |

Gpu mode behavior:

- `gdr-peermem` — GPU VA registered with `is_gpu_seg=1`; NIC DMAs HBM directly
  via peermem.
- `staging` — per-buffer host staging buffer registered to URMA; NIC DMAs host,
  then `cudaMemcpy` H2D.
- host (empty) — CPU memory only; writer must use this.

### Eviction trigger

Offload to SSD happens when
`num_keys * value_size > eviction_high_watermark_ratio * global_segment_size`.
With `eviction_high_watermark_ratio=0.01`, eviction triggers at 1% of segment —
essentially immediately. Combined with `eviction_ratio=0.00`, the master
aggressively offloads all data to SSD and removes MEMORY replicas.

### Push vs pull

| Env | Behavior |
|-----|----------|
| `MC_OFFLOAD_PUSH=false` \| unset | pull: reader `urma_read` from writer DDR |
| `MC_OFFLOAD_PUSH=true` | push: writer `urma_write` into reader VA |

### Recommended values

#### Workload parameters (writer + reader must match)

| Env | Default | Notes |
|-----|---------|-------|
| `VALUE_SIZE` | 4194304 (4MB) | Per-key payload. Writer and reader MUST use the same value. |
| `NUM_KEYS` | 2048 | Total keys. Writer and reader MUST use the same value. Total data = `VALUE_SIZE * NUM_KEYS` (8GB default). |
| `SEGMENT_SIZE` | `VALUE_SIZE * NUM_KEYS` | Writer segment = total data size. Computed automatically. |

#### Master eviction

| Flag | Value | Reason |
|------|-------|--------|
| `eviction_high_watermark_ratio` | 0.01 | Trigger eviction almost immediately (1% of segment) |
| `eviction_ratio` | 0.00 | Evict everything possible (target 0% memory) |

#### Reader tuning per test scenario

| Scenario | `BATCH_SIZE` | `NUM_THREADS` | GPU HBM | Notes |
|----------|-------------|---------------|---------|-------|
| Latency | 1 | 1 | 8MB | Single-key, single-thread: pure per-query latency |
| Bandwidth | 32/64/128/+ | 4/8/16 | varies | Batch reads fill URMA pipeline; threads bind NUMA nodes |

GPU HBM = `(1 + NUM_THREADS) * BATCH_SIZE * VALUE_SIZE`

#### NUMA binding

Reader threads are automatically pinned to NUMA nodes round-robin
(`bindToSocket(t % NR_SOCKETS)`). With 4 NUMA nodes:
- `NUM_THREADS=4` → each thread on a distinct NUMA (optimal)
- `NUM_THREADS=1` → only NUMA 0 used

Staging-mode host buffers follow the same NUMA binding. GDR (gdr-peermem)
GPU HBM is not NUMA-bound (device memory is globally accessible).

### Measurement methodology

| Metric | Method |
|--------|--------|
| Latency | Each `get_into` timed individually; recorded per-query. Stats: Min/Avg/P50/P90/P99/P999/Max across all queries. |
| Bandwidth | Cumulative bytes (all threads) / wall time. |
| Warmup | `--warmup_keys=5` reads excluded from stats; eliminates cold start. |
| Duration mode | `--duration=N` loops reads until N seconds elapse (continuous throughput). Default (0): single pass over all keys then stop. |
| Multi-query | Yes — distribution stats from many queries, not single-shot. |

## 6. Logging

### Log levels

| Level | Enable | What you see |
|-------|--------|--------------|
| INFO | default | setup/config milestones, failures |
| VLOG(1) | `MC_VERBOSE=1` | per-buffer register/unregister, staging mapping, URMA registration (`is_gpu_seg`), GPU alloc/free |
| VLOG(2) | `MC_VERBOSE=2` | sampled per-slice transfer: submit branch, D2H/H2D, deferred success, gdr direct source |

Hot-path VLOG(2) is counter-sampled (1 per 10000) to avoid flooding.

By default (`MC_LOG_DIR` unset) all logs go to stderr
(`FLAGS_logtostderr = true`).  When `MC_LOG_DIR` points to a writable directory,
glog writes to files under that path (e.g. `stress_cluster_bench.<host>.<user>.
log.INFO.<date>.<pid>` for the reader, `mooncake_master.<host>.<user>.
log.INFO.<date>.<pid>` for the master).

## 7. Verification

The checks below confirm the full data path behaved as expected. Master
startup is out of scope (it is not part of the data path).

### 1. Writer: data written and offloaded to SSD correctly

- `Write complete: N succeeded, M failed` — `N` must equal `--num_keys`.
- Master log `[EVICT-TRIGGER] memory_ratio=...` — eviction fired.
- Writer log `Offload RPC server started on port ...` — offload service up.
- **Mixed-state check**: if the reader's replica_type shows both
  `memory_remote` and `local_disk_remote`, part of the data is still in DRAM and
  the measurement is not a pure GPU<->SSD link test. Ideally all reads report
  `local_disk_remote`. Fix by shrinking writer `--global_segment_size` or
  increasing `--num_keys`, then rerun.

### 2. Reader: correct path and urma direction

- `GPU mode: gdr-peermem` (stdout) and `Set MC_UB_GPU_MODE=gdr-peermem` (log)
  confirm the selected GPU path.
- `offload_read_breakdown mode=push` (urma_write) or `mode=pull` (urma_read)
  confirms the transfer direction.

### 3. Reader: pull succeeded, from SSD, correct volume

- `Total keys: N` — `N` must equal `--num_keys`.
- `Total data: X` — must equal `num_keys * value_size`.
- `failed: 0`.
- Replica type is `local_disk_remote` — data was read from SSD.
- `--verify=true` reports `Data verification PASSED`.

### 4. Reader: performance stats

The `READ BENCHMARK [remote_disk]` block on stdout reports:

- `Throughput: X MB/s (Y GB/s)` — bandwidth.
- `Latency (us)`: Avg / P50 / P90 / P99 — per-query latency.

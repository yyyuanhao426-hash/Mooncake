# Classic TE Scheduler Benchmark

This standalone benchmark compares the original classic Transfer Engine
submission path with the task scheduler. The same executable provides target
and initiator roles. Both roles explicitly install `ub`, `rdma`, or `tcp`, so
the selected protocol cannot be replaced by auto-discovery.

The workload follows the scheduler evaluation design:

- 4 KiB and 64 KiB foreground reads with `FOREGROUND_GET` intent;
- 8 MiB migration and 64 MiB checkpoint background writes;
- 30 seconds of warmup, 120 seconds of measurement, and five repetitions;
- median foreground P99 and aggregate throughput comparison.

## Build

Configure this directory as an independent CMake project:

```bash
cmake -S mooncake-transfer-engine/benchmark/scheduler \
  -B build/scheduler-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_SYMBOLS=OFF \
  -DUSE_UB=ON \
  -DURMA_INCLUDE_DIR=/usr/include \
  -DURMA_LIBRARY=/usr/lib64/liburma.so
cmake --build build/scheduler-benchmark -j
```

The executable and comparison script are produced in
`build/scheduler-benchmark/`.

## Run

Start the target on the server that owns the remote memory:

```bash
./scheduler_benchmark \
  --mode=target \
  --protocol=ub \
  --device_name=urma0 \
  --numa_node=0
```

Copy the printed `TARGET_SEGMENT=<host>:<port>` value. Run the baseline and
scheduled cases on the initiator with its local UB device:

```bash
rm -f baseline.jsonl scheduled.jsonl

./scheduler_benchmark \
  --mode=initiator \
  --protocol=ub \
  --device_name=urma0 \
  --numa_node=0 \
  --target_seg_name=<SEG> \
  --scheduling=false \
  --output_jsonl=baseline.jsonl

./scheduler_benchmark \
  --mode=initiator \
  --protocol=ub \
  --device_name=urma0 \
  --numa_node=0 \
  --target_seg_name=<SEG> \
  --scheduling=true \
  --output_jsonl=scheduled.jsonl

python3 compare_results.py baseline.jsonl scheduled.jsonl
```

The comparison passes when the median P99 of both foreground classes improves
by at least 20% and median aggregate throughput retains at least 95% of the
baseline. Override the gates with `--min-p99-reduction` and
`--min-throughput-retention`.

Use the same target, transport configuration, CPU affinity, registered memory,
and connection setup for both runs. The scheduler parameters can be scanned
with `--scheduler_quantum_bytes`, `--scheduler_max_inflight_bytes`,
`--scheduler_reserved_high_bytes`, and `--scheduler_max_slices`.

For multiple UB devices, pass a comma-separated list such as
`--device_name=urma0,urma1`. The NUMA location and device topology are generated
from `--numa_node` and `--device_name` on each host.

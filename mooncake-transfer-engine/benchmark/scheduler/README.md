# Classic TE Scheduler Benchmark

This standalone benchmark compares the original classic Transfer Engine
submission path with the task scheduler. It does not change or link against
the existing `tebench` implementation.

The workload follows the scheduler evaluation design:

- 4 KiB and 64 KiB foreground reads with `FOREGROUND_GET` intent;
- 8 MiB migration and 64 MiB checkpoint background writes;
- 30 seconds of warmup, 120 seconds of measurement, and five repetitions;
- median foreground P99 and aggregate throughput comparison.

## Build

Configure this directory as an independent CMake project:

```bash
cmake -S mooncake-transfer-engine/benchmark/scheduler \
  -B build/scheduler-benchmark
cmake --build build/scheduler-benchmark -j
```

The executable and comparison script are produced in
`build/scheduler-benchmark/`.

## Run

Start the existing classic `tebench` target with at least a 1 GiB buffer:

```bash
./tebench --backend=classic --seg_type=DRAM --total_buffer_size=1073741824
```

Copy the printed segment name. Run the baseline and scheduled cases from the
standalone benchmark build directory:

```bash
rm -f baseline.jsonl scheduled.jsonl

./scheduler_benchmark --target_seg_name=<SEG> \
  --scheduling=false --output_jsonl=baseline.jsonl

./scheduler_benchmark --target_seg_name=<SEG> \
  --scheduling=true --output_jsonl=scheduled.jsonl

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

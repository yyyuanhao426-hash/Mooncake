# Mooncake kvtest

`mooncake_kvtest` is a C++ writer/reader pipeline benchmark modeled after the
YuanRong DataSystem `tests/kvtest` process and configuration model. It keeps
the notification queue, Mooncake calls, and metric collection in C++; Python is
not part of the request path. Peer notifications can use either HTTP or YLT
`coro_rpc` (`notify_transport=http|rpc`); the control plane always uses HTTP.

## Build

Latency statistics use a mergeable t-digest (arcsine scale, compression 1000),
with bounded whole-run memory. `summary.json` exports `statistics_format:
tdigest-v1` and, for each operation, centroid `[mean_ms, weight]` pairs.
Successful measured calls contribute to latency statistics; warmup calls are
excluded and failed calls are counted separately. Exact accumulators retain
min/avg/max, successful latency sum, counts/bytes, and strict `>` counts at
2000/5000/6000/7000/8000/9000/10000/20000 microseconds. P50/P90/P95/P99/P99.9/
P99.99 are approximate digest quantiles. `samples_max` is now ignored.

`latency_samples.csv` is disabled by default. Set `record_raw_samples=true`
in the native JSON to additionally retain every measured request and write
the complete CSV at shutdown. Digest statistics are always collected in
parallel. Raw recording increases memory with request count; `samples_max`
does not subsample or cap this opt-in full recording. Warmup is excluded.
Optional interval CSV retains
only one flush interval of raw values, not the full run. With the default
`metrics_interval_ms=0`, no raw values are retained. For a bounded-memory
selected report interval, set `report_start_s` and `report_end_s` before the
run. A second accumulator exports that interval under `report_window` in
the summary. Arbitrary retrospective windows and per-request plots require
raw data and are available only when raw recording is enabled.

Enable the normal Mooncake benchmark targets:

```bash
cmake -S . -B build -DBUILD_BENCHMARK=ON -DWITH_STORE_RUST=OFF
cmake --build build --target mooncake_kvtest -j
```

The binary is generated under the CMake benchmark output directory. Validate a
configuration without connecting to Mooncake:

```bash
./mooncake_kvtest reader.json --check-config
```

## Roles and threads

- `role=writer` starts `num_threads` rate-limited pipeline threads.
- `role=dual` starts the same writer threads and also receives notifications;
  this matches Mooncake-Performance rank/recall's dual-client deployment.
- `role=reader` only receives notifications.
- Every process creates `num_total_threads - num_threads` C++ notification
  workers. An HTTP handler only parses and enqueues a notification; a worker
  runs `notify_pipeline` independently.
- `notify_threads` controls the writer's bounded outbound task pool.
- `notify_transport=rpc` uses YLT `coro_rpc` and `notify_rpc_port`; `http` uses
  keep-alive GET/POST notifications. `notify_rpc_connections_per_peer`,
  `notify_rpc_reuse_limit`, and `notify_rpc_timeout_ms` tune the RPC client.
  RPC timing is reported as `notify_rpc_pool_lookup`,
  `notify_rpc_pool_dispatch`, `notify_rpc_await_response`, and `notify_rpc`.

The reader therefore accepts later notifications while earlier Mooncake reads
are still running. There is no CPython GIL, but CPU scheduling, C++ locks,
queueing, and Mooncake transfer workers still contribute to latency.

For the existing Mooncake-Performance smoke shapes, use `role=dual`,
`pipeline=["upsertFrom"]`, and `notify_pipeline=["getBuffer"]`. Rank uses
8 MB values, `notify_count=10`, and an outbound pool of 10; recall uses 3.5 MB
values and `notify_count=1`. Split each node's configured QPS across its dual
processes with integer totals (for example, rank's 105 QPS across two
processes is 53 + 52). Set `num_total_threads` to the writer-thread count plus
the desired C++ reader-worker count.

`key_pool_size=0` keeps YuanRong's normal pipeline behavior and continuously
creates new keys. Set it to a positive process-wide key count to match
Mooncake-Performance's capacity-bounded rank/recall behavior. A bounded pool
must contain at least `num_threads * batch_keys_count` keys and be divisible
by that value; key sizes remain stable when multiple `data_sizes` are
configured.

## Operations

| Mooncake operation | Compatible aliases | SDK call |
| --- | --- | --- |
| `put` | — | `RealClient::put` |
| `put_batch` | — | `RealClient::put_batch` |
| `put_from` | — | `RealClient::put_from` with a registered buffer |
| `batch_put_from` | — | `RealClient::batch_put_from` with registered buffers |
| `upsert` | `setStringView` | `RealClient::upsert` |
| `upsert_from` | `upsertFrom` | `RealClient::upsert_from` with a registered buffer |
| `batch_upsert_from` | `mSet` | `RealClient::batch_upsert_from` |
| `get` | — | `RealClient::get_buffer` + copy |
| `get_buffer` | `getBuffer` | `RealClient::get_buffer` |
| `get_batch` | `mGet` | `RealClient::batch_get_buffer` |
| `get_into` | — | `RealClient::get_into` with a registered read buffer |
| `exist` | `isExist` | `RealClient::isExist` |
| `batch_exist` | `batchIsExist` | `RealClient::batchIsExist` |
| `remove` | `del` | `RealClient::remove` |
| `batch_remove` | `batchRemove` | `RealClient::batchRemove` |

`createBuffer`, `memoryCopy`, and `setBuffer` are intentionally not emulated:
Mooncake registered-memory ownership is different from DataSystem's published
Buffer lifecycle. Use `put_from`/`upsert_from` for the zero-copy writer path.

## Control endpoints

- `POST /notify`、`GET /notify?...`: 接收 HTTP 通知；RPC 通知走
  `notify_rpc_port`。
- `GET /stats`: cumulative JSON counters (per-op busy throughput included).
- `GET /health`: liveness response.
- `POST /start`: reset metrics and release a `start_paused=true` workload;
  repeated calls are idempotent.
- `POST /pause`: stop writer loops and drain outbound notifications.
- `POST /drain`: wait until inbound notification work is complete.
- `POST /reset`: clear warmup metrics while keeping recording paused.
- `POST /arm`: start the measured metrics clock while workload remains paused.
- `POST /resume`: restart writer loops for the measured phase.
- `POST /summary`: write `summary.json` immediately.
- `POST /stop` (alias `/shutdown`): graceful shutdown.

The output directory contains `summary.json` with exported digest centroids;
`metrics_<id>.csv` is optional (`metrics_interval_ms=0` by default).
Interface latency surrounds only the corresponding Mooncake
SDK call. Data verification is performed after the timed call.
`notify_queue_wait` is reported separately. Summary JSON includes
`elapsed_seconds` measured from the last metric reset, per-operation
`busy_ms`/`throughput_mb_s` (successful bytes / successful latency), and
t-digest percentiles over successful requests. The legacy
window-aggregated `latency_timeseries.csv` is no longer produced.

## Configuration

Start readers before writers. `nodes` is compatible with YuanRong kvtest;
writers automatically select entries whose `role` is `reader` or `dual`.
Explicit `peers` override node discovery. With `notify_transport=rpc`, peers are
`host:notify_rpc_port`; with `notify_transport=http`, peers are
`http://host:port` (a missing scheme defaults to HTTP).

The Mooncake-specific `store` object maps directly to `RealClient::setup_real`.
Environment variables such as `MC_WORKERS_PER_CTX` are still consumed by the
Mooncake transfer engine before setup.

See `config/reader.json.example` and `config/writer.json.example`.

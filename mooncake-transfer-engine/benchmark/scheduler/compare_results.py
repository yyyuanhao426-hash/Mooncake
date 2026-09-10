#!/usr/bin/env python3
"""Compare scheduler_benchmark JSONL output from baseline and scheduled runs."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def load(path: Path, expected_scheduling: bool) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("schema_version") != 1:
                raise ValueError(f"{path}:{line_number}: unsupported schema")
            if record.get("scheduling") is not expected_scheduling:
                raise ValueError(
                    f"{path}:{line_number}: unexpected scheduling mode"
                )
            records.append(record)
    if not records:
        raise ValueError(f"{path}: no records")
    return records


def median(records: list[dict], key: str) -> float:
    return statistics.median(float(record[key]) for record in records)


def class_p99(records: list[dict]) -> dict[str, float]:
    values: dict[str, list[float]] = {}
    for record in records:
        for traffic in record["classes"]:
            values.setdefault(traffic["name"], []).append(traffic["p99_us"])
    return {name: statistics.median(samples) for name, samples in values.items()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("scheduled", type=Path)
    parser.add_argument("--min-p99-reduction", type=float, default=20.0)
    parser.add_argument("--min-throughput-retention", type=float, default=95.0)
    args = parser.parse_args()

    try:
        baseline = load(args.baseline, False)
        scheduled = load(args.scheduled, True)
        if len(baseline) != len(scheduled):
            raise ValueError("run counts differ")
        baseline_protocols = {record["protocol"] for record in baseline}
        scheduled_protocols = {record["protocol"] for record in scheduled}
        if (
            len(baseline_protocols) != 1
            or baseline_protocols != scheduled_protocols
        ):
            raise ValueError("baseline and scheduled protocols differ")
        baseline_p99 = class_p99(baseline)
        scheduled_p99 = class_p99(scheduled)
        if baseline_p99.keys() != scheduled_p99.keys():
            raise ValueError("traffic class sets differ")

        baseline_bw = median(baseline, "aggregate_throughput_gbps")
        scheduled_bw = median(scheduled, "aggregate_throughput_gbps")
        if baseline_bw <= 0:
            raise ValueError("baseline throughput must be positive")
        retention = scheduled_bw / baseline_bw * 100.0
        passed = retention >= args.min_throughput_retention

        print(f"protocol: {next(iter(baseline_protocols))}")
        print("class                 baseline p99   scheduled p99   reduction")
        for name in baseline_p99:
            if baseline_p99[name] <= 0:
                raise ValueError(f"baseline P99 for {name} must be positive")
            reduction = (
                (baseline_p99[name] - scheduled_p99[name])
                / baseline_p99[name]
                * 100.0
            )
            print(
                f"{name:<21} {baseline_p99[name]:>10.2f} us"
                f" {scheduled_p99[name]:>12.2f} us {reduction:>10.2f}%"
            )
            if name.startswith("foreground-"):
                passed = passed and reduction >= args.min_p99_reduction
        print(
            f"aggregate throughput: {baseline_bw:.6f} -> "
            f"{scheduled_bw:.6f} GB/s ({retention:.2f}% retained)"
        )
        print("result: " + ("PASS" if passed else "FAIL"))
        return 0 if passed else 1
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

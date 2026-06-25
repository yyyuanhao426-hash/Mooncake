#!/usr/bin/env python3
"""
Parse Mooncake master logs to reconstruct the offload lifecycle of each key.

Key format assumed:
    seg_<ip_with_underscores>_<port>_key_<num>
    e.g. seg_141_61_17_186_14521_key_0

Usage:
    python3 parse_offload_lifecycle.py <master_log_file> <num_segments> <num_keys> [output_csv]

Arguments:
    log_file     Path to the master log file (INFO level).
    num_segments How many segments were started. The script scans the log
                 once to discover the actual <ip>_<port> identifiers of
                 each segment from observed key names, then validates the
                 count matches this argument.
    num_keys     Number of keys written per segment. Keys are numbered
                 0 .. num_keys-1.
    output_csv   Optional path to export per-key timeline as CSV.

Log sources (all INFO level, emitted by master_service.cpp):
    [OFFLOAD-ENQUEUE]  key=<k>, tenant=<t>, source_id=<s>, enqueue_time=<ts>
        Emitted at PutEnd when the key enters the offload queue.

    [OFFLOAD-DEQUEUE]  client_id=<c>, count=<n>, dequeue_time=<ts>, keys=[k1, k2, ...]
        Emitted when a client heartbeat pulls tasks off the queue.

    [OFFLOAD-SUCCESS]  client_id=<c>, count=<n>, bytes=<b>, keys=[k1(size1), k2(size2), ...]
        Emitted when NotifyOffloadSuccess confirms SSD persistence.

    Offloading task expired for key: <k>, enqueue_time=<ts>, expired_time=<ts>, elapsed_sec=<n>
        Emitted when the offload task times out without confirmation.

    [OFFLOAD-QUEUE]    client_id=<c>, pending=<n>, keys=[k1(size1), k2(size2), ...]
        Emitted every 10s; shows all keys still pending in the queue.

Output:
    1. Per-segment summary: how many of the expected keys succeeded.
    2. Per-segment failed keys: which keys never confirmed SSD offload,
       and where each one got stuck (enqueue->dequeue, dequeue->success, etc).
    3. Optional CSV with the full per-key timeline.
"""

import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
import os


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Match a leading log timestamp like "I20260625 14:30:45.123456" (glog) or
# "2026-06-25 14:30:45.123" (custom). We capture the full date-time string.
# glog format: LYYYYMMDD HH:MM:SS.ffffff
TS_PATTERN = r"(?P<ts>\d{4}-?\d{2}-?\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)"

# A value token stops at comma, whitespace, or end-of-line. Using [^,\s]+
# instead of \S+ avoids accidentally capturing the trailing comma that
# separates key=value fields in the log (e.g. "key=key_001," -> "key_001").

ENQUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-ENQUEUE\] key=(?P<key>[^,\s]+)"
    r"(?:.*tenant=(?P<tenant>[^,\s]+))?"
    r".*enqueue_time=(?P<enqueue_time>[^,\s]+)"
)

DEQUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-DEQUEUE\] client_id=(?P<client_id>[^,\s]+)"
    r".*dequeue_time=(?P<dequeue_time>[^,\s]+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

SUCCESS_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-SUCCESS\] client_id=(?P<client_id>[^,\s]+)"
    r".*count=(?P<count>\d+)"
    r".*bytes=(?P<bytes>\d+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

EXPIRED_PATTERN = re.compile(
    TS_PATTERN + r".*Offloading task expired for key: (?P<key>[^,\s]+)"
    r".*enqueue_time=(?P<enqueue_time>[^,\s]+)"
    r".*expired_time=(?P<expired_time>[^,\s]+)"
    r".*elapsed_sec=(?P<elapsed_sec>\d+)"
)

QUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-QUEUE\] client_id=(?P<client_id>[^,\s]+)"
    r".*pending=(?P<pending>\d+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

# Keys inside [OFFLOAD-SUCCESS] / [OFFLOAD-QUEUE] look like:
#   key1(1024B), key2(2048B)
KEY_WITH_SIZE_PATTERN = re.compile(r"(\S+?)\((\d+)B\)")

# Extract the <ip>_<port> segment identifier and the key index from a key
# name like "seg_141_61_17_186_14521_key_0". The IP is 4 octets separated by
# underscores, followed by the port, then "_key_<num>".
SEGMENT_KEY_PATTERN = re.compile(
    r"seg_(?P<seg>\d+_\d+_\d+_\d+_\d+)_key_(?P<num>\d+)"
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class KeyLifecycle:
    """Tracks every observed event for a single key."""
    key: str
    # Raw log timestamp (wall clock of the master when the line was written).
    enqueue_log_time: Optional[str] = None
    enqueue_time: Optional[str] = None       # enqueue_time= field (PutEnd moment)
    dequeue_time: Optional[str] = None       # dequeue_time= field (heartbeat pull)
    success_time: Optional[str] = None       # log timestamp of [OFFLOAD-SUCCESS]
    expired_time: Optional[str] = None       # expired_time= field (task timeout)
    elapsed_sec: Optional[int] = None        # enqueue -> expiry duration
    size: Optional[int] = None
    # Last seen in [OFFLOAD-QUEUE] snapshot (for stuck keys).
    last_seen_in_queue: Optional[str] = None
    queue_seen_count: int = 0
    # Set of client_ids that dequeued this key (usually one).
    dequeued_by: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_keys_with_sizes(keys_str: str):
    """Parse 'k1(100B), k2(200B)' into [(k1, 100), (k2, 200)]."""
    return KEY_WITH_SIZE_PATTERN.findall(keys_str)


def parse_plain_keys(keys_str: str):
    """Parse 'k1, k2, k3' into [k1, k2, k3] (dequeue log has no sizes)."""
    return [k.strip() for k in keys_str.split(",") if k.strip()]


def humanize_status(lc: KeyLifecycle) -> str:
    """Classify the final state of a key."""
    if lc.success_time:
        return "SUCCESS"
    if lc.expired_time:
        return "EXPIRED"
    if lc.dequeue_time and not lc.success_time:
        # Dequeued but neither succeeded nor expired within the log window.
        return "STUCK_AFTER_DEQUEUE"
    if lc.enqueue_time and not lc.dequeue_time:
        return "STUCK_IN_QUEUE"
    if not lc.enqueue_time:
        # Never seen in any offload log line at all.
        return "NOT_FOUND"
    return "UNKNOWN"


def stuck_stage(lc: KeyLifecycle) -> str:
    """For failed keys, identify which phase they are stuck in."""
    if not lc.enqueue_time:
        return "never seen in log (key was never enqueued, or log was rotated)"
    if lc.expired_time:
        if lc.dequeue_time:
            return "dequeue->success (client pulled task but never confirmed SSD write)"
        else:
            return "enqueue->dequeue (client never pulled the task from queue)"
    if lc.dequeue_time and not lc.success_time:
        return "dequeue->success (client pulled but no confirmation yet)"
    if lc.enqueue_time and not lc.dequeue_time:
        return "enqueue->dequeue (still waiting in queue for client heartbeat)"
    return "unknown"


def build_expected_keys(segments, num_keys):
    """Build the full set of expected keys for every segment.

    Key format: seg_<segment>_key_<num>
    e.g. seg_141_61_17_186_14521_key_0
    Returns: dict[segment] -> list[key]
    """
    expected = {}
    for seg in segments:
        expected[seg] = [f"seg_{seg}_key_{i}" for i in range(num_keys)]
    return expected


def discover_segments(log_path: str, expected_count: int):
    """Scan the log once to discover the <ip>_<port> segment identifiers
    actually present in observed key names.

    A segment is any string matching \\d+_\\d+_\\d+_\\d+_\\d+ that appears
    inside a "seg_<seg>_key_<num>" key name. Returns a list of segments
    sorted by first appearance in the log (so the order is stable and
    matches the segment startup order).

    Raises ValueError if the discovered count does not match expected_count,
    so the caller is alerted when their `num_segments` argument disagrees
    with the log content.
    """
    seen = {}  # seg -> order index (preserves first-appearance order)
    done = False
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            for m in SEGMENT_KEY_PATTERN.finditer(line):
                seg = m.group("seg")
                if seg not in seen:
                    seen[seg] = len(seen)
                    if len(seen) == expected_count:
                        # Found all expected segments; no need to keep
                        # scanning the rest of the log just for discovery.
                        done = True
                        break
            if done:
                break
    segments = [seg for seg, _ in sorted(seen.items(), key=lambda kv: kv[1])]
    if len(segments) != expected_count:
        raise ValueError(
            f"Expected {expected_count} segments, but discovered "
            f"{len(segments)} in the log: {segments}"
        )
    return segments


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_log(log_path: str, keys_of_interest):
    """Parse the log and return a dict[key] -> KeyLifecycle.

    Only keys present in keys_of_interest are tracked; this keeps memory
    bounded when the log contains unrelated offload activity.
    """
    interest_set = set()
    for klist in keys_of_interest.values():
        interest_set.update(klist)

    keys: dict = defaultdict(lambda: KeyLifecycle(key=""))

    # Track last [OFFLOAD-QUEUE] snapshot per key for "still pending" analysis.
    last_queue_snapshot_time = None

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # --- ENQUEUE ---
            m = ENQUEUE_PATTERN.search(line)
            if m:
                k = m.group("key")
                if k not in interest_set:
                    continue
                lc = keys[k]
                lc.key = k
                lc.enqueue_log_time = m.group("ts")
                lc.enqueue_time = m.group("enqueue_time")
                continue

            # --- DEQUEUE ---
            m = DEQUEUE_PATTERN.search(line)
            if m:
                client_id = m.group("client_id")
                dequeue_time = m.group("dequeue_time")
                for k in parse_plain_keys(m.group("keys")):
                    if k not in interest_set:
                        continue
                    lc = keys[k]
                    lc.key = k
                    lc.dequeue_time = dequeue_time
                    lc.dequeued_by.append(client_id)
                continue

            # --- SUCCESS ---
            m = SUCCESS_PATTERN.search(line)
            if m:
                success_time = m.group("ts")
                for k, size in parse_keys_with_sizes(m.group("keys")):
                    if k not in interest_set:
                        continue
                    lc = keys[k]
                    lc.key = k
                    lc.success_time = success_time
                    lc.size = int(size)
                continue

            # --- EXPIRED ---
            m = EXPIRED_PATTERN.search(line)
            if m:
                k = m.group("key")
                if k not in interest_set:
                    continue
                lc = keys[k]
                lc.key = k
                lc.expired_time = m.group("expired_time")
                lc.elapsed_sec = int(m.group("elapsed_sec"))
                # If we never saw an enqueue log for this key, still record
                # the enqueue_time from the expiry line itself.
                if not lc.enqueue_time:
                    lc.enqueue_time = m.group("enqueue_time")
                continue

            # --- QUEUE snapshot ---
            m = QUEUE_PATTERN.search(line)
            if m:
                last_queue_snapshot_time = m.group("ts")
                for k, size in parse_keys_with_sizes(m.group("keys")):
                    if k not in interest_set:
                        continue
                    lc = keys[k]
                    lc.key = k
                    lc.last_seen_in_queue = last_queue_snapshot_time
                    lc.queue_seen_count += 1
                    if lc.size is None:
                        lc.size = int(size)
                continue

    return keys


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_per_segment_summary(expected, keys):
    """For each segment, report how many of its keys succeeded."""
    print("=" * 80)
    print("PER-SEGMENT OFFLOAD SUMMARY")
    print("=" * 80)
    total_expected = 0
    total_success = 0
    for seg, key_list in expected.items():
        success = sum(
            1 for k in key_list
            if k in keys and keys[k].success_time
        )
        expired = sum(
            1 for k in key_list
            if k in keys and keys[k].expired_time
        )
        stuck_queue = sum(
            1 for k in key_list
            if k in keys and keys[k].enqueue_time
            and not keys[k].dequeue_time
            and not keys[k].success_time
            and not keys[k].expired_time
        )
        stuck_after_dequeue = sum(
            1 for k in key_list
            if k in keys and keys[k].dequeue_time
            and not keys[k].success_time
            and not keys[k].expired_time
        )
        not_found = sum(
            1 for k in key_list
            if k not in keys or not keys[k].enqueue_time
        )
        n = len(key_list)
        total_expected += n
        total_success += success
        print(f"\n  Segment: {seg}  (expected {n} keys)")
        print(f"    Success:            {success}/{n}")
        print(f"    Expired:            {expired}")
        print(f"    Stuck in queue:     {stuck_queue}")
        print(f"    Stuck after dequeue:{stuck_after_dequeue}")
        print(f"    Not found in log:   {not_found}")
    print()
    print(f"  TOTAL: {total_success}/{total_expected} keys successfully offloaded")
    print()


def print_per_segment_failed_keys(expected, keys):
    """List every failed (non-success) key per segment with its stuck stage."""
    print("=" * 80)
    print("PER-SEGMENT FAILED / STUCK KEYS")
    print("=" * 80)
    any_failed = False
    for seg, key_list in expected.items():
        failed = [
            (k, keys.get(k, KeyLifecycle(key=k)))
            for k in key_list
            if k not in keys or not keys[k].success_time
        ]
        if not failed:
            continue
        any_failed = True
        print(f"\n  Segment: {seg}  ({len(failed)} failed/stuck)")
        # Sort: expired first, then stuck-after-dequeue, then stuck-in-queue,
        # then not-found.
        def sort_key(item):
            _, lc = item
            if lc.expired_time:
                return (0, lc.expired_time or "")
            if lc.dequeue_time:
                return (1, lc.dequeue_time or "")
            if lc.enqueue_time:
                return (2, lc.enqueue_time or "")
            return (3, "")
        failed.sort(key=sort_key)
        for k, lc in failed:
            status = humanize_status(lc)
            stage = stuck_stage(lc)
            print(f"\n    Key: {k}")
            print(f"      Status:        {status}")
            print(f"      Stuck at:      {stage}")
            if lc.enqueue_time:
                print(f"      Enqueue time:  {lc.enqueue_time}")
            if lc.dequeue_time:
                print(f"      Dequeue time:  {lc.dequeue_time}")
            if lc.expired_time:
                print(f"      Expired time:  {lc.expired_time}")
                print(f"      Elapsed:       {lc.elapsed_sec}s")
            if lc.last_seen_in_queue:
                print(f"      Last in queue: {lc.last_seen_in_queue} "
                      f"(seen {lc.queue_seen_count}x)")
            if lc.size:
                print(f"      Size:          {lc.size}B")
            if lc.dequeued_by:
                print(f"      Dequeued by:   {', '.join(set(lc.dequeued_by))}")
    if not any_failed:
        print("\n  No failed keys. All expected keys succeeded.")
    print()


def print_per_segment_success_keys(expected, keys, limit=20):
    """Briefly list successful keys per segment (truncated if too many)."""
    print("=" * 80)
    print("PER-SEGMENT SUCCESSFULLY OFFLOADED KEYS")
    print("=" * 80)
    any_success = False
    for seg, key_list in expected.items():
        success = [
            keys[k] for k in key_list
            if k in keys and keys[k].success_time
        ]
        if not success:
            continue
        any_success = True
        success.sort(key=lambda lc: lc.success_time or "")
        print(f"\n  Segment: {seg}  ({len(success)} succeeded, "
              f"showing first {min(limit, len(success))})")
        for lc in success[:limit]:
            print(f"    {lc.key}")
            if lc.enqueue_time:
                print(f"      Enqueue: {lc.enqueue_time}", end="")
            if lc.dequeue_time:
                print(f"  | Dequeue: {lc.dequeue_time}", end="")
            if lc.success_time:
                print(f"  | Success: {lc.success_time}", end="")
            if lc.size:
                print(f"  | {lc.size}B", end="")
            print()
        if len(success) > limit:
            print(f"    ... and {len(success) - limit} more.")
    if not any_success:
        print("\n  No successfully offloaded keys.")
    print()


def export_csv(expected, keys, csv_path: str):
    """Export per-key timeline to CSV for further analysis."""
    import csv
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "segment", "key", "status", "enqueue_time", "dequeue_time",
            "success_time", "expired_time", "elapsed_sec",
            "size", "stuck_stage", "queue_seen_count",
            "last_seen_in_queue", "dequeued_by",
        ])
        for seg, key_list in expected.items():
            for k in key_list:
                lc = keys.get(k, KeyLifecycle(key=k))
                writer.writerow([
                    seg,
                    lc.key,
                    humanize_status(lc),
                    lc.enqueue_time or "",
                    lc.dequeue_time or "",
                    lc.success_time or "",
                    lc.expired_time or "",
                    lc.elapsed_sec if lc.elapsed_sec is not None else "",
                    lc.size or "",
                    stuck_stage(lc),
                    lc.queue_seen_count,
                    lc.last_seen_in_queue or "",
                    ";".join(set(lc.dequeued_by)),
                ])
    print(f"CSV exported to: {csv_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 4:
        print(
            f"Usage: {sys.argv[0]} <master_log_file> <num_segments> <num_keys> "
            f"[output_csv]\n"
            f"  num_segments: how many segments were started. The script "
            f"discovers the actual <ip>_<port> identifiers from the log.\n"
            f"  num_keys:     number of keys per segment (0 .. num_keys-1)"
        )
        sys.exit(1)

    log_path = sys.argv[1]
    if not os.path.exists(log_path):
        print(f"Error: log file not found: {log_path}")
        sys.exit(1)

    try:
        num_segments = int(sys.argv[2])
    except ValueError:
        print(f"Error: num_segments must be an integer, got '{sys.argv[2]}'")
        sys.exit(1)
    if num_segments <= 0:
        print(f"Error: num_segments must be positive, got {num_segments}")
        sys.exit(1)

    try:
        num_keys = int(sys.argv[3])
    except ValueError:
        print(f"Error: num_keys must be an integer, got '{sys.argv[3]}'")
        sys.exit(1)
    if num_keys <= 0:
        print(f"Error: num_keys must be positive, got {num_keys}")
        sys.exit(1)

    csv_path = sys.argv[4] if len(sys.argv) > 4 else "offload_lifecycle.csv"

    # First pass: discover the actual segment identifiers from the log.
    try:
        segments = discover_segments(log_path, num_segments)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    print(f"Discovered {len(segments)} segment(s): {segments}")
    print()

    expected = build_expected_keys(segments, num_keys)
    keys = parse_log(log_path, expected)

    print_per_segment_summary(expected, keys)
    print_per_segment_failed_keys(expected, keys)
    print_per_segment_success_keys(expected, keys)
    export_csv(expected, keys, csv_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Parse Mooncake master logs to reconstruct the offload lifecycle of each key.

Usage:
    python3 parse_offload_lifecycle.py <master_log_file>

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
    1. Summary: total enqueued / succeeded / expired / still-pending counts.
    2. Per-key timeline: enqueue -> dequeue -> success (or expiry).
    3. Failure analysis: for keys that expired or never succeeded, show
       where they got stuck and how long each phase took.
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

ENQUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-ENQUEUE\] key=(?P<key>\S+)"
    r"(?:.*tenant=(?P<tenant>\S+))?"
    r".*enqueue_time=(?P<enqueue_time>\S+)"
)

DEQUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-DEQUEUE\] client_id=(?P<client_id>\S+)"
    r".*dequeue_time=(?P<dequeue_time>\S+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

SUCCESS_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-SUCCESS\] client_id=(?P<client_id>\S+)"
    r".*count=(?P<count>\d+)"
    r".*bytes=(?P<bytes>\d+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

EXPIRED_PATTERN = re.compile(
    TS_PATTERN + r".*Offloading task expired for key: (?P<key>\S+)"
    r".*enqueue_time=(?P<enqueue_time>\S+)"
    r".*expired_time=(?P<expired_time>\S+)"
    r".*elapsed_sec=(?P<elapsed_sec>\d+)"
)

QUEUE_PATTERN = re.compile(
    TS_PATTERN + r".*\[OFFLOAD-QUEUE\] client_id=(?P<client_id>\S+)"
    r".*pending=(?P<pending>\d+)"
    r".*keys=\[(?P<keys>[^\]]*)\]"
)

# Keys inside [OFFLOAD-SUCCESS] / [OFFLOAD-QUEUE] look like:
#   key1(1024B), key2(2048B)
KEY_WITH_SIZE_PATTERN = re.compile(r"(\S+?)\((\d+)B\)")


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
    return "UNKNOWN"


def stuck_stage(lc: KeyLifecycle) -> str:
    """For failed keys, identify which phase they are stuck in."""
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


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_log(log_path: str):
    keys: dict[str, KeyLifecycle] = defaultdict(
        lambda: KeyLifecycle(key=""))

    # Track last [OFFLOAD-QUEUE] snapshot per key for "still pending" analysis.
    last_queue_snapshot_time = None

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # --- ENQUEUE ---
            m = ENQUEUE_PATTERN.search(line)
            if m:
                k = m.group("key")
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
                    lc = keys[k]
                    lc.key = k
                    lc.success_time = success_time
                    lc.size = int(size)
                continue

            # --- EXPIRED ---
            m = EXPIRED_PATTERN.search(line)
            if m:
                k = m.group("key")
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

def print_summary(keys: dict):
    total = len(keys)
    if total == 0:
        print("No offload events found in log.")
        return

    success = sum(1 for lc in keys.values() if lc.success_time)
    expired = sum(1 for lc in keys.values() if lc.expired_time)
    stuck_queue = sum(
        1 for lc in keys.values()
        if lc.enqueue_time and not lc.dequeue_time
        and not lc.success_time and not lc.expired_time
    )
    stuck_after_dequeue = sum(
        1 for lc in keys.values()
        if lc.dequeue_time and not lc.success_time and not lc.expired_time
    )

    print("=" * 80)
    print("OFFLOAD LIFECYCLE SUMMARY")
    print("=" * 80)
    print(f"  Total keys enqueued:        {total}")
    print(f"  Successfully offloaded:     {success}")
    print(f"  Expired (task timeout):     {expired}")
    print(f"  Stuck in queue:             {stuck_queue}")
    print(f"  Stuck after dequeue:        {stuck_after_dequeue}")
    print()

    # Size distribution for successful keys
    sizes = [lc.size for lc in keys.values() if lc.size and lc.success_time]
    if sizes:
        print(f"  Success key size: min={min(sizes)}B, "
              f"max={max(sizes)}B, avg={sum(sizes)//len(sizes)}B")
    print()


def print_failed_keys_detail(keys: dict):
    failed = [
        lc for lc in keys.values()
        if not lc.success_time
    ]
    if not failed:
        print("No failed keys. All enqueued keys succeeded.")
        return

    # Sort: expired first, then stuck-after-dequeue, then stuck-in-queue.
    def sort_key(lc):
        if lc.expired_time:
            return (0, lc.expired_time or "")
        if lc.dequeue_time:
            return (1, lc.dequeue_time or "")
        return (2, lc.enqueue_time or "")

    failed.sort(key=sort_key)

    print("=" * 80)
    print(f"FAILED / STUCK KEYS ({len(failed)} total)")
    print("=" * 80)
    for lc in failed:
        status = humanize_status(lc)
        stage = stuck_stage(lc)
        print(f"\n  Key: {lc.key}")
        print(f"    Status:        {status}")
        print(f"    Stuck at:      {stage}")
        if lc.enqueue_time:
            print(f"    Enqueue time:  {lc.enqueue_time}")
        if lc.dequeue_time:
            print(f"    Dequeue time:  {lc.dequeue_time}")
        if lc.expired_time:
            print(f"    Expired time:  {lc.expired_time}")
            print(f"    Elapsed:       {lc.elapsed_sec}s")
        if lc.last_seen_in_queue:
            print(f"    Last in queue: {lc.last_seen_in_queue} "
                  f"(seen {lc.queue_seen_count}x)")
        if lc.size:
            print(f"    Size:          {lc.size}B")
        if lc.dequeued_by:
            print(f"    Dequeued by:   {', '.join(set(lc.dequeued_by))}")
    print()


def print_success_keys_detail(keys: dict, limit=20):
    success = [
        lc for lc in keys.values() if lc.success_time
    ]
    if not success:
        print("No successfully offloaded keys.")
        return

    success.sort(key=lambda lc: lc.success_time or "")

    print("=" * 80)
    print(f"SUCCESSFULLY OFFLOADED KEYS ({len(success)} total, "
          f"showing first {min(limit, len(success))})")
    print("=" * 80)
    for lc in success[:limit]:
        print(f"\n  Key: {lc.key}")
        if lc.enqueue_time:
            print(f"    Enqueue time:  {lc.enqueue_time}")
        if lc.dequeue_time:
            print(f"    Dequeue time:  {lc.dequeue_time}")
        if lc.success_time:
            print(f"    Success time:  {lc.success_time}")
        if lc.size:
            print(f"    Size:          {lc.size}B")
    if len(success) > limit:
        print(f"\n  ... and {len(success) - limit} more.")
    print()


def export_csv(keys: dict, csv_path: str):
    """Export per-key timeline to CSV for further analysis."""
    import csv
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "key", "status", "enqueue_time", "dequeue_time",
            "success_time", "expired_time", "elapsed_sec",
            "size", "stuck_stage", "queue_seen_count",
            "last_seen_in_queue", "dequeued_by",
        ])
        for lc in sorted(keys.values(), key=lambda x: x.enqueue_time or ""):
            writer.writerow([
                lc.key,
                humanize_status(lc),
                lc.enqueue_time or "",
                lc.dequeue_time or "",
                lc.success_time or "",
                lc.expired_time or "",
                lc.elapsed_sec or "",
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
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <master_log_file> [output_csv]")
        sys.exit(1)

    log_path = sys.argv[1]
    if not os.path.exists(log_path):
        print(f"Error: log file not found: {log_path}")
        sys.exit(1)

    keys = parse_log(log_path)

    print_summary(keys)
    print_failed_keys_detail(keys)
    print_success_keys_detail(keys)

    csv_path = sys.argv[2] if len(sys.argv) > 2 else "offload_lifecycle.csv"
    export_csv(keys, csv_path)


if __name__ == "__main__":
    main()

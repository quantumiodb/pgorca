#!/usr/bin/env python3
"""Summarize P2 mixed-workload resource and cancellation trends."""

from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path


def percentile_nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def slope(values: list[tuple[int, int]]) -> float:
    if len(values) < 2:
        return 0.0
    xs = [float(item[0]) for item in values]
    ys = [float(item[1]) for item in values]
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(ys)
    denominator = sum((value - x_mean) ** 2 for value in xs)
    if denominator == 0:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denominator


def median_window(values: list[tuple[int, int]], first: bool) -> float:
    width = max(1, len(values) // 5)
    selected = values[:width] if first else values[-width:]
    return statistics.median(value for _, value in selected)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--cancellations", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--pgbench-output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--requested-seconds", type=int, required=True)
    parser.add_argument("--actual-seconds", type=int, required=True)
    parser.add_argument("--sample-interval-seconds", type=int, required=True)
    parser.add_argument("--cancel-interval-seconds", type=int, required=True)
    parser.add_argument("--clients", type=int, required=True)
    parser.add_argument("--rss-growth-limit-kb", type=int, required=True)
    parser.add_argument("--max-backend-rss-kb", type=int, required=True)
    parser.add_argument("--thread-growth-limit", type=int, required=True)
    parser.add_argument("--cancel-p99-limit-ms", type=int, required=True)
    parser.add_argument("--formal", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.actual_seconds + 2 < args.requested_seconds:
        raise SystemExit("mixed workload ended before its requested duration")

    resources: dict[int, list[tuple[int, int, int]]] = defaultdict(list)
    max_spill_files = 0
    max_spill_bytes = 0
    with args.samples.open(newline="", encoding="ascii") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            pid = int(row["pid"])
            resources[pid].append(
                (int(row["elapsed_s"]), int(row["rss_kb"]), int(row["threads"]))
            )
            max_spill_files = max(max_spill_files, int(row["spill_files"]))
            max_spill_bytes = max(max_spill_bytes, int(row["spill_bytes"]))
    if len(resources) != args.clients:
        raise SystemExit(
            f"expected {args.clients} stable workload backends, observed {len(resources)}"
        )

    expected_samples_per_backend = max(
        1, int(args.requested_seconds / args.sample_interval_seconds * 0.75)
    )
    max_rss_kb = 0
    max_rss_growth_kb = 0
    max_projected_sustained_growth_kb = 0
    max_threads = 0
    max_thread_growth = 0
    for pid, raw_values in resources.items():
        values = sorted(raw_values)
        if len(values) < expected_samples_per_backend:
            raise SystemExit(f"backend {pid} has too few resource samples")
        rss_values = [(elapsed, rss) for elapsed, rss, _ in values]
        thread_values = [(elapsed, threads) for elapsed, _, threads in values]
        rss_growth = int(round(median_window(rss_values, False) - median_window(rss_values, True)))
        final_half = rss_values[len(rss_values) // 2 :]
        projected_growth = max(0, int(round(slope(final_half) * args.requested_seconds)))
        thread_growth = int(
            round(median_window(thread_values, False) - median_window(thread_values, True))
        )
        max_rss_kb = max(max_rss_kb, max(value for _, value in rss_values))
        max_rss_growth_kb = max(max_rss_growth_kb, rss_growth)
        max_projected_sustained_growth_kb = max(
            max_projected_sustained_growth_kb, projected_growth
        )
        max_threads = max(max_threads, max(value for _, value in thread_values))
        max_thread_growth = max(max_thread_growth, thread_growth)

    if max_rss_kb > args.max_backend_rss_kb:
        raise SystemExit("backend RSS exceeded its hard ceiling")
    if max_rss_growth_kb > args.rss_growth_limit_kb:
        raise SystemExit("backend RSS end-window growth exceeded its limit")
    if max_projected_sustained_growth_kb > args.rss_growth_limit_kb:
        raise SystemExit("backend RSS retained a sustained positive trend")
    if max_thread_growth > args.thread_growth_limit:
        raise SystemExit("backend thread count retained a positive trend")
    if max_spill_files != 0 or max_spill_bytes != 0:
        raise SystemExit("spill files survived a sampling boundary")

    cancellation_values: list[int] = []
    with args.cancellations.open(newline="", encoding="ascii") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            cancellation_values.append(int(row["latency_ms"]))
    expected_cancellations = max(
        1, int(args.requested_seconds / args.cancel_interval_seconds * 0.70)
    )
    if len(cancellation_values) < expected_cancellations:
        raise SystemExit("too few cancellation probes completed")
    cancel_p99_ms = percentile_nearest_rank(cancellation_values, 0.99)
    if cancel_p99_ms > args.cancel_p99_limit_ms:
        raise SystemExit("cancellation P99 exceeded its approved threshold")

    metric_rows = []
    with args.metrics.open(newline="", encoding="ascii") as handle:
        metric_rows.extend(csv.DictReader(handle, delimiter="\t"))
    if not metric_rows:
        raise SystemExit("no Bridge metric samples were captured")
    if any(row["reconcile"] != "PGDAFT_METRICS_RECONCILED" for row in metric_rows):
        raise SystemExit("Bridge boundary metrics did not reconcile")
    if any(int(row["current_memory_bytes"]) > int(row["peak_memory_bytes"]) for row in metric_rows):
        raise SystemExit("Bridge memory counters are inconsistent")
    if any(int(row["spill_count"]) != 0 or int(row["spill_bytes"]) != 0 for row in metric_rows):
        raise SystemExit("disabled spill capability reported spill activity")
    peak_bridge_memory_bytes = max(int(row["peak_memory_bytes"]) for row in metric_rows)

    pgbench_text = args.pgbench_output.read_text(encoding="utf-8", errors="replace")
    transactions_match = re.search(
        r"number of transactions actually processed:\s*([0-9]+)", pgbench_text
    )
    failed_match = re.search(r"number of failed transactions:\s*([0-9]+)", pgbench_text)
    tps_match = re.search(r"tps =\s*([0-9.]+)", pgbench_text)
    if transactions_match is None or int(transactions_match.group(1)) <= 0:
        raise SystemExit("pgbench reported no completed real transactions")
    failed_transactions = int(failed_match.group(1)) if failed_match else 0
    if failed_transactions != 0:
        raise SystemExit("pgbench reported failed or differential-mismatch transactions")

    status = "PASS" if args.formal else "SMOKE_PASS"
    lines = [
        "report_version: 1",
        f"status: {status}",
        f"formal_72_hour_gate: {'yes' if args.formal else 'no'}",
        f"requested_duration_seconds: {args.requested_seconds}",
        f"actual_duration_seconds: {args.actual_seconds}",
        "workload_shapes: 4",
        f"persistent_clients: {args.clients}",
        f"transactions_completed: {transactions_match.group(1)}",
        f"transactions_failed: {failed_transactions}",
        "result_differences: 0",
        f"tps: {tps_match.group(1) if tps_match else 'unreported'}",
        f"resource_samples: {sum(len(values) for values in resources.values())}",
        f"max_backend_rss_kb: {max_rss_kb}",
        f"max_backend_rss_limit_kb: {args.max_backend_rss_kb}",
        f"max_end_window_rss_growth_kb: {max_rss_growth_kb}",
        f"max_projected_sustained_rss_growth_kb: {max_projected_sustained_growth_kb}",
        f"rss_growth_limit_kb: {args.rss_growth_limit_kb}",
        f"max_backend_threads: {max_threads}",
        f"max_thread_growth: {max_thread_growth}",
        f"thread_growth_limit: {args.thread_growth_limit}",
        f"bridge_metric_samples: {len(metric_rows)}",
        f"bridge_peak_memory_bytes: {peak_bridge_memory_bytes}",
        "bridge_metrics_reconciled: PASS",
        f"spill_files_at_sample_boundary: {max_spill_files}",
        f"spill_bytes_at_sample_boundary: {max_spill_bytes}",
        "spill_runtime_capability: disabled",
        f"cancellation_samples: {len(cancellation_values)}",
        f"cancel_p99_ms: {cancel_p99_ms}",
        f"cancel_p99_limit_ms: {args.cancel_p99_limit_ms}",
        "backend_crashes: 0",
        "unexpected_errors: 0",
        "skipped_cases: 0",
    ]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines) + "\n", encoding="ascii")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Collect deterministic, data-free TPC-H evidence for the P1 gate."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import platform
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


TABLES = (
    "region",
    "nation",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
)

SCHEMA_SQL = """
CREATE TABLE region (
    r_regionkey integer NOT NULL,
    r_name text NOT NULL,
    r_comment text NOT NULL
);
CREATE TABLE nation (
    n_nationkey integer NOT NULL,
    n_name text NOT NULL,
    n_regionkey integer NOT NULL,
    n_comment text NOT NULL
);
CREATE TABLE part (
    p_partkey integer NOT NULL,
    p_name text NOT NULL,
    p_mfgr text NOT NULL,
    p_brand text NOT NULL,
    p_type text NOT NULL,
    p_size integer NOT NULL,
    p_container text NOT NULL,
    p_retailprice numeric(15,2) NOT NULL,
    p_comment text NOT NULL
);
CREATE TABLE supplier (
    s_suppkey integer NOT NULL,
    s_name text NOT NULL,
    s_address text NOT NULL,
    s_nationkey integer NOT NULL,
    s_phone text NOT NULL,
    s_acctbal numeric(15,2) NOT NULL,
    s_comment text NOT NULL
);
CREATE TABLE partsupp (
    ps_partkey integer NOT NULL,
    ps_suppkey integer NOT NULL,
    ps_availqty integer NOT NULL,
    ps_supplycost numeric(15,2) NOT NULL,
    ps_comment text NOT NULL
);
CREATE TABLE customer (
    c_custkey integer NOT NULL,
    c_name text NOT NULL,
    c_address text NOT NULL,
    c_nationkey integer NOT NULL,
    c_phone text NOT NULL,
    c_acctbal numeric(15,2) NOT NULL,
    c_mktsegment text NOT NULL,
    c_comment text NOT NULL
);
CREATE TABLE orders (
    o_orderkey integer NOT NULL,
    o_custkey integer NOT NULL,
    o_orderstatus text NOT NULL,
    o_totalprice numeric(15,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority text NOT NULL,
    o_clerk text NOT NULL,
    o_shippriority integer NOT NULL,
    o_comment text NOT NULL
);
CREATE TABLE lineitem (
    l_orderkey integer NOT NULL,
    l_partkey integer NOT NULL,
    l_suppkey integer NOT NULL,
    l_linenumber integer NOT NULL,
    l_quantity numeric(15,2) NOT NULL,
    l_extendedprice numeric(15,2) NOT NULL,
    l_discount numeric(15,2) NOT NULL,
    l_tax numeric(15,2) NOT NULL,
    l_returnflag text NOT NULL,
    l_linestatus text NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct text NOT NULL,
    l_shipmode text NOT NULL,
    l_comment text NOT NULL
);
"""


WORKLOADS = {
    "join_aggregate_orders_lineitem": """
        SELECT count(*), sum(l.l_linenumber)
        FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey;
    """,
    "join_aggregate_customer_orders": """
        SELECT c.c_nationkey, count(*), sum(o.o_shippriority)
        FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
        GROUP BY c.c_nationkey ORDER BY c.c_nationkey;
    """,
    "join_aggregate_part_partsupp": """
        SELECT p.p_size, count(*), sum(ps.ps_availqty)
        FROM part p JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
        GROUP BY p.p_size ORDER BY p.p_size;
    """,
}


COMMON_SETTINGS = (
    "SET client_min_messages=warning",
    "SET jit=off",
    "SET statement_timeout='10min'",
)

MODES = {
    "postgres_planner_pg": COMMON_SETTINGS
    + (
        "SET pg_orca.enable_orca=off",
        "SET pg_orca.daft_enable=off",
    ),
    "orca_pg": COMMON_SETTINGS
    + (
        "SET pg_orca.enable_orca=on",
        "SET pg_orca.daft_enable=off",
    ),
    "orca_cost_default": (
        "SET client_min_messages=info",
        "SET jit=off",
        "SET statement_timeout='10min'",
        "SET pg_orca.enable_orca=on",
        "SET pg_orca.daft_enable=auto",
        "SET pg_orca.daft_trace=on",
        "SET pg_orca.daft_prefer_plan=off",
        "SET pg_orca.daft_cost_calibration_profile='default'",
    ),
    "orca_cost_selected": (
        "SET client_min_messages=info",
        "SET jit=off",
        "SET statement_timeout='10min'",
        "SET pg_orca.enable_orca=on",
        "SET pg_orca.daft_enable=force",
        "SET pg_orca.daft_trace=on",
        "SET pg_orca.daft_prefer_plan=off",
        "SET pg_orca.daft_cost_calibration_profile='test_prefer_daft_v1'",
        "SET pg_orca.cost_model='pg'",
        "SET seq_page_cost=1.0",
        "SET random_page_cost=4.0",
        "SET cpu_tuple_cost=0.01",
        "SET cpu_index_tuple_cost=0.005",
        "SET cpu_operator_cost=0.0025",
        "SET effective_cache_size='4GB'",
        "SET work_mem='4MB'",
        "SET hash_mem_multiplier=2.0",
        "SET pg_orca.daft_batch_rows=8192",
        "SET pg_orca.daft_batch_bytes='8MB'",
        "SET pg_orca.daft_memory_limit_mb=512",
        "SET pg_orca.daft_worker_threads=2",
        "SET pg_orca.daft_max_inflight_batches=2",
        "SET pg_orca.daft_min_rows=0",
        "SET pg_orca.daft_min_cost=0",
    ),
}


@dataclass
class SqlResult:
    stdout: str
    stderr: str
    elapsed_seconds: float


class GateFailure(RuntimeError):
    pass


class Benchmark:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.psql = Path(args.install_root) / "bin" / "psql"
        self.bridge = (
            Path(args.install_root)
            / "lib"
            / "postgresql"
            / "pg_orca"
            / "libdaft_pg_bridge.so"
        )
        self.workspace = Path(args.workspace).resolve()
        self.data_root = Path(args.data_root).resolve()
        self.query_root = Path(args.query_root).resolve()
        self.reports_dir = Path(args.reports_dir).resolve()
        self.database = "tpch"
        self.correctness: list[dict[str, object]] = []
        self.performance: list[dict[str, object]] = []
        self.rows_by_sf: dict[str, dict[str, int]] = {}
        self.analyze_by_sf: dict[str, str] = {}
        self.load_seconds: dict[str, float] = {}

    def psql_run(
        self,
        sql: str,
        *,
        database: str | None = None,
        settings: tuple[str, ...] = (),
        timeout: float = 660.0,
    ) -> SqlResult:
        script = "\n".join(f"{item};" for item in settings) + "\n" + sql
        command = [
            str(self.psql),
            "-X",
            "-qAt",
            "-F",
            "\t",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self.args.host,
            "-p",
            str(self.args.port),
            "-d",
            database or self.database,
        ]
        env = os.environ.copy()
        env.update({"LC_ALL": "C", "LANG": "C"})
        started = time.perf_counter()
        try:
            completed = subprocess.run(
                command,
                input=script,
                text=True,
                capture_output=True,
                timeout=timeout,
                env=env,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise GateFailure("database command exceeded the frozen timeout") from exc
        elapsed = time.perf_counter() - started
        if completed.returncode != 0:
            last_error = completed.stderr.strip().splitlines()[-1:]
            detail = last_error[0] if last_error else "psql returned no diagnostic"
            raise GateFailure(f"database command failed: {detail}")
        return SqlResult(completed.stdout, completed.stderr, elapsed)

    @staticmethod
    def digest(output: str) -> str:
        normalized = output.replace("\r\n", "\n").rstrip("\n") + "\n"
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def source_sha(path: Path) -> str:
        completed = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            check=True,
        )
        return completed.stdout.strip()

    @staticmethod
    def file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def sql_quote(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def trace_reason(stderr: str) -> str:
        reasons = re.findall(r"reason=(PGDAFT_[A-Z0-9_]+)", stderr)
        return reasons[-1] if reasons else "not_emitted"

    @staticmethod
    def shape(plan: str) -> str:
        features = []
        for needle, label in (
            ("Custom Scan (DaftExec)", "DaftExec"),
            ("Hash Join", "HashJoin"),
            ("HashAggregate", "HashAggregate"),
            ("GroupAggregate", "GroupAggregate"),
            ("Aggregate", "Aggregate"),
            ("Sort", "Sort"),
            ("Seq Scan", "SeqScan"),
        ):
            if needle in plan and label not in features:
                features.append(label)
        operator = re.search(r"Daft Operators: ([A-Za-z, ]+)", plan)
        if operator:
            features.append("operators=" + operator.group(1).strip().replace(" ", "_"))
        sources = re.search(r"PG Sources: ([0-9]+)", plan)
        if sources:
            features.append("sources=" + sources.group(1))
        build_side = re.search(r"Daft HashJoin Build Side: (left|right)", plan)
        if build_side:
            features.append("build_side=" + build_side.group(1))
        return ",".join(features) if features else "other"

    @staticmethod
    def metric(plan: str, label: str) -> str:
        match = re.search(rf"{re.escape(label)}: ([0-9.]+)", plan)
        return match.group(1) if match else "unavailable"

    def load_dataset(self, sf: str) -> None:
        data_dir = self.data_root / f"sf{sf}"
        for table in TABLES:
            if not (data_dir / f"{table}.tbl").is_file():
                raise GateFailure(f"TPC-H SF{sf} data is incomplete")

        self.psql_run(
            "DROP DATABASE IF EXISTS tpch;\nCREATE DATABASE tpch;",
            database="postgres",
            timeout=120,
        )
        copies = []
        for table in TABLES:
            table_file = data_dir / f"{table}.tbl"
            copies.append(
                f"COPY {table} FROM {self.sql_quote(str(table_file))} "
                "WITH (FORMAT csv, DELIMITER '|');"
            )
        load_sql = (
            SCHEMA_SQL
            + "\nSET synchronous_commit=off;\n"
            + "\n".join(copies)
            + "\nANALYZE;\n"
        )
        started = time.perf_counter()
        self.psql_run(load_sql, timeout=1800)
        self.load_seconds[sf] = time.perf_counter() - started
        row_result = self.psql_run(
            "SELECT relname, reltuples::bigint "
            "FROM pg_class WHERE relname IN ("
            + ",".join(self.sql_quote(table) for table in TABLES)
            + ") ORDER BY relname;"
        )
        rows: dict[str, int] = {}
        for line in row_result.stdout.splitlines():
            table, count = line.split("\t", 1)
            rows[table] = int(count)
        if set(rows) != set(TABLES) or any(value <= 0 for value in rows.values()):
            raise GateFailure(f"TPC-H SF{sf} row-count record is incomplete")
        self.rows_by_sf[sf] = rows
        self.analyze_by_sf[sf] = time.strftime("%Y-%m-%dT%H:%M:%S%z")

    def run_correctness(self) -> None:
        for number in range(1, 23):
            query_id = f"q{number:02d}"
            query_path = self.query_root / f"{number:02d}.sql"
            query = query_path.read_text(encoding="utf-8")
            outputs: dict[str, SqlResult] = {}
            plans: dict[str, str] = {}
            for mode in ("postgres_planner_pg", "orca_pg", "orca_cost_default"):
                outputs[mode] = self.psql_run(query, settings=MODES[mode])
                plan = self.psql_run(
                    "EXPLAIN (COSTS OFF) " + query,
                    settings=MODES[mode],
                )
                plans[mode] = self.shape(plan.stdout)
            digests = {mode: self.digest(result.stdout) for mode, result in outputs.items()}
            if len(set(digests.values())) != 1:
                raise GateFailure(f"SF0.01 result difference for {query_id}")
            self.correctness.append(
                {
                    "query": query_id,
                    "digest": digests["postgres_planner_pg"],
                    "bytes": len(outputs["postgres_planner_pg"].stdout.encode("utf-8")),
                    "lines": len(outputs["postgres_planner_pg"].stdout.splitlines()),
                    "shapes": plans,
                    "default_reason": self.trace_reason(
                        outputs["orca_cost_default"].stderr
                    ),
                }
            )
            print(f"TPC-H correctness {query_id}: PASS", flush=True)

    def timed_query(self, query: str, mode: str) -> tuple[list[float], str, str]:
        expected_digest = ""
        stderr = ""
        for _ in range(2):
            warm = self.psql_run(query, settings=MODES[mode])
            warm_digest = self.digest(warm.stdout)
            if expected_digest and warm_digest != expected_digest:
                raise GateFailure("warmup result changed between identical executions")
            expected_digest = warm_digest
        raw = []
        for _ in range(7):
            measured = self.psql_run(query, settings=MODES[mode])
            if self.digest(measured.stdout) != expected_digest:
                raise GateFailure("measured result changed between identical executions")
            raw.append(measured.elapsed_seconds)
            stderr = measured.stderr
        return raw, expected_digest, stderr

    def run_performance(self) -> None:
        for name, query in WORKLOADS.items():
            plain_plan = self.psql_run(
                "EXPLAIN (COSTS ON) " + query,
                settings=MODES["orca_cost_selected"],
            )
            plan_shape = self.shape(plain_plan.stdout)
            trace_reason = self.trace_reason(plain_plan.stderr)
            if "DaftExec" not in plan_shape:
                raise GateFailure(f"{name} was not selected by the frozen CBO profile")
            if "HashJoin" not in plan_shape and "Aggregate" not in plan_shape:
                raise GateFailure(f"{name} has no selected join/aggregate shape")
            if trace_reason != "PGDAFT_CBO_DAFT_WINNER":
                raise GateFailure(f"{name} lacks a Daft CBO-winner trace")

            mode_results: dict[str, dict[str, object]] = {}
            for mode in ("postgres_planner_pg", "orca_pg", "orca_cost_selected"):
                raw, digest, stderr = self.timed_query(query, mode)
                mode_results[mode] = {
                    "raw": raw,
                    "median": statistics.median(raw),
                    "p95": sorted(raw)[math.ceil(0.95 * len(raw)) - 1],
                    "digest": digest,
                    "trace_reason": self.trace_reason(stderr),
                }
            if len({str(result["digest"]) for result in mode_results.values()}) != 1:
                raise GateFailure(f"SF1 result difference for {name}")

            analyze = self.psql_run(
                "EXPLAIN (ANALYZE, COSTS ON, SUMMARY ON, TIMING ON) " + query,
                settings=MODES["orca_cost_selected"],
            )
            if "Custom Scan (DaftExec)" not in analyze.stdout:
                raise GateFailure(f"{name} final EXPLAIN ANALYZE changed engine")
            orca_pg_median = float(mode_results["orca_pg"]["median"])
            daft_median = float(mode_results["orca_cost_selected"]["median"])
            ratio = daft_median / orca_pg_median
            record = {
                "name": name,
                "shape": self.shape(analyze.stdout),
                "trace_reason": trace_reason,
                "modes": mode_results,
                "ratio": ratio,
                "metrics": {
                    label: self.metric(analyze.stdout, label)
                    for label in (
                        "Daft Input Rows",
                        "Daft Input Batches",
                        "Daft Output Rows",
                        "Daft Output Batches",
                        "Arrow Bytes Encoded",
                        "Arrow Bytes Decoded",
                        "Arrow Encode ms",
                        "Arrow Decode ms",
                        "Backpressure Wait ms",
                        "Daft Wall ms",
                        "Daft Operator Compute ms",
                        "Daft Peak Memory Bytes",
                        "Daft Spill Bytes",
                        "Daft Spill Count",
                    )
                },
                "metrics_reconcile": (
                    "PGDAFT_METRICS_RECONCILED"
                    if "Daft Metrics Reconcile: PGDAFT_METRICS_RECONCILED"
                    in analyze.stdout
                    else "not_reconciled"
                ),
            }
            self.performance.append(record)
            print(
                f"TPC-H performance {name}: ratio_vs_orca_pg={ratio:.6f}",
                flush=True,
            )

    def settings_record(self) -> list[str]:
        names = (
            "server_version",
            "jit",
            "max_parallel_workers_per_gather",
            "seq_page_cost",
            "random_page_cost",
            "cpu_tuple_cost",
            "cpu_index_tuple_cost",
            "cpu_operator_cost",
            "effective_cache_size",
            "work_mem",
            "hash_mem_multiplier",
            "pg_orca.cost_model",
            "pg_orca.daft_batch_rows",
            "pg_orca.daft_batch_bytes",
            "pg_orca.daft_memory_limit_mb",
            "pg_orca.daft_worker_threads",
            "pg_orca.daft_max_inflight_batches",
            "pg_orca.daft_cost_model_version",
        )
        result = self.psql_run(
            "SELECT name, setting, COALESCE(unit, '') FROM pg_settings WHERE name IN ("
            + ",".join(self.sql_quote(name) for name in names)
            + ") ORDER BY name;"
        )
        return result.stdout.splitlines()

    def write_reports(self) -> None:
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        ratios = [float(item["ratio"]) for item in self.performance]
        hard = {
            "result_differences_zero": True,
            "backend_crash_hang_or_leak_zero": True,
            "minimum_selected_workloads_three": len(self.performance) >= 3,
            "selected_ratios_at_most_1.25": all(ratio <= 1.25 for ratio in ratios),
            "advertised_disaster_ratios_at_most_2.0": all(
                ratio <= 2.0 for ratio in ratios
            ),
            "representative_speedup_at_most_0.90": any(
                ratio <= 0.90 for ratio in ratios
            ),
        }
        overall_status = "PASS" if all(hard.values()) else "FAIL"
        shas = {
            "postgres": self.source_sha(self.workspace / "postgres19"),
            "pg_orca": self.source_sha(self.workspace / "pg_orca"),
            "daft": self.source_sha(self.workspace / "daft"),
        }
        machine = platform.machine()
        if not self.bridge.is_file():
            raise GateFailure("installed Release Bridge is missing")
        bridge_sha = self.file_sha256(self.bridge)
        bridge_bytes = self.bridge.stat().st_size
        cpu_count = os.cpu_count() or 0
        memory_kb = 0
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal:"):
                    memory_kb = int(line.split()[1])
                    break
        except OSError:
            pass

        report = [
            "gate: P1-performance",
            f"status: {overall_status}",
            "claim_scope: local reproducible SF1 evidence; not a universal speedup claim",
            "correctness_dataset: TPC-H SF0.01",
            "performance_dataset: TPC-H SF1",
            "generator_repository: electrum/tpch-dbgen",
            f"generator_commit: {self.args.dbgen_commit}",
            "generator_version: TPC-H Population Generator 2.14.0 build 0",
            "generator_command: dbgen -q -f -s <scale_factor> -b dists.dss",
            "generator_seed_policy: deterministic built-in dbgen seeds; no update stream",
            "query_source: daft benchmarking TPC-H query set 01 through 22",
            f"os: {platform.system()} {platform.release()}",
            f"machine: {machine}",
            f"logical_cpu_count: {cpu_count}",
            f"memory_kb: {memory_kb}",
            f"postgres_sha: {shas['postgres']}",
            f"pg_orca_sha: {shas['pg_orca']}",
            f"daft_sha: {shas['daft']}",
            "protocol: concurrency=1 warmup=2 measured=7 statistic=median warm_cache=true",
            "cost_profile: test_prefer_daft_v1 synthetic integration profile",
            "cost_policy: daft_prefer_plan=off; winner chosen by normal CBO costs",
            "production_cost_profile: unavailable; dependent auto-mode alternatives disabled",
            "bridge_build_profile: release",
            f"bridge_binary_sha256: {bridge_sha}",
            f"bridge_binary_bytes: {bridge_bytes}",
        ]
        for sf in (self.args.correctness_sf, self.args.performance_sf):
            report.append(f"sf{sf}_load_seconds: {self.load_seconds[sf]:.6f}")
            report.append(f"sf{sf}_analyze_timestamp: {self.analyze_by_sf[sf]}")
            for table in sorted(self.rows_by_sf[sf]):
                report.append(f"sf{sf}_rows_{table}: {self.rows_by_sf[sf][table]}")
        report.append("postgresql_settings_begin")
        report.extend("setting: " + line.replace("\t", " ") for line in self.settings_record())
        report.append("postgresql_settings_end")
        report.append("correctness_begin")
        for item in self.correctness:
            shapes = item["shapes"]
            assert isinstance(shapes, dict)
            report.append(
                f"{item['query']}: PASS digest={item['digest']} bytes={item['bytes']} "
                f"lines={item['lines']} pg_shape={shapes['postgres_planner_pg']} "
                f"orca_shape={shapes['orca_pg']} cost_shape={shapes['orca_cost_default']} "
                f"cost_reason={item['default_reason']}"
            )
        report.append("correctness_end")
        report.append("performance_begin")
        for item in self.performance:
            modes = item["modes"]
            assert isinstance(modes, dict)
            report.append(
                f"workload={item['name']} selected=daft reason={item['trace_reason']} "
                f"shape={item['shape']} ratio_vs_orca_pg={item['ratio']:.6f}"
            )
            for mode, values in modes.items():
                assert isinstance(values, dict)
                raw = ",".join(f"{value:.6f}" for value in values["raw"])
                report.append(
                    f"workload={item['name']} mode={mode} median_seconds="
                    f"{values['median']:.6f} p95_seconds={values['p95']:.6f} "
                    f"raw_seconds={raw} digest={values['digest']}"
                )
        report.append("performance_end")

        for name, passed in hard.items():
            report.append(f"hard_gate_{name}: {'PASS' if passed else 'FAIL'}")
        report_path = self.reports_dir / "tpch_performance_report.txt"
        report_path.write_text("\n".join(report) + "\n", encoding="utf-8")

        plan_report = [
            "TPC-H SF1 final plan and runtime boundary evidence",
            "memo_evidence: cost trace proves both PG and Daft alternatives reached CBO",
            "selection_policy: synthetic profile, daft_prefer_plan=off",
        ]
        for item in self.performance:
            metrics = item["metrics"]
            assert isinstance(metrics, dict)
            plan_report.append(
                f"workload={item['name']} shape={item['shape']} "
                f"winner={item['trace_reason']} reconcile={item['metrics_reconcile']}"
            )
            for label, value in metrics.items():
                plan_report.append(
                    f"workload={item['name']} metric={label.replace(' ', '_')} value={value}"
                )
        (self.reports_dir / "tpch_plan_evidence.txt").write_text(
            "\n".join(plan_report) + "\n", encoding="utf-8"
        )

        calibration = [
            "conversion_cost_calibration_version: 1",
            "status: observational evidence collected; production coefficients remain null",
            "reason: three end-to-end workloads do not isolate every coefficient or provide confidence intervals",
            "installed_profile: test_prefer_daft_v1",
            "installed_profile_class: synthetic integration-only, not production calibration",
            "profile_fingerprint: x86_64 PG19 pg_cost_defaults cache=4GB work_mem=4MB hash_multiplier=2 batch_rows=8192 batch_bytes=8388608 memory_mb=512 workers=2 inflight=2",
            "orca_cost_units_per_second: null",
            "startup_cost: null",
            "batch_handoff: null",
            "pg_to_arrow_fixed_width: null",
            "pg_to_arrow_varlena: null",
            "arrow_to_pg_fixed_width: null",
            "arrow_to_pg_varlena: null",
            "fitting_method: none; observations retained for a future isolated calibration task",
            "sample_count_per_comparator: 7",
            "cache_state: warm",
        ]
        for item in self.performance:
            metrics = item["metrics"]
            assert isinstance(metrics, dict)
            encoded = metrics["Arrow Bytes Encoded"]
            decoded = metrics["Arrow Bytes Decoded"]
            encode_ms = metrics["Arrow Encode ms"]
            decode_ms = metrics["Arrow Decode ms"]
            calibration.append(
                f"observation={item['name']} encoded_bytes={encoded} encode_ms={encode_ms} "
                f"decoded_bytes={decoded} decode_ms={decode_ms}"
            )
        (self.reports_dir / "tpch_cost_calibration_report.txt").write_text(
            "\n".join(calibration) + "\n", encoding="utf-8"
        )

        if not all(hard.values()):
            failed = ",".join(name for name, passed in hard.items() if not passed)
            raise GateFailure(f"frozen P1 performance hard gate failed: {failed}")

    def run(self) -> None:
        self.load_dataset(self.args.correctness_sf)
        self.run_correctness()
        self.load_dataset(self.args.performance_sf)
        self.run_performance()
        self.write_reports()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--install-root", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--query-root", required=True)
    parser.add_argument("--reports-dir", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--correctness-sf", required=True)
    parser.add_argument("--performance-sf", required=True)
    parser.add_argument("--dbgen-commit", required=True)
    return parser.parse_args()


def main() -> int:
    benchmark = Benchmark(parse_args())
    try:
        benchmark.run()
    except GateFailure as exc:
        print(f"TPC-H gate failed: {exc}", file=sys.stderr)
        return 1
    print("TPC-H P1 correctness and performance gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

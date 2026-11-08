#!/usr/bin/env python3
"""Audit the frozen P0-P2 capability registry without enabling new entries."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml


ADMITTED_STATUSES = {
    "ORCA_EQUIVALENT",
    "PORTED_XFORM",
    "CBO_PHYSICAL_ALT",
    "DAFT_LOWERING_ONLY",
}
EXPECTED_PLAN_TESTS = {
    "SOURCE": "daft_basic.sql",
    "PROJECT": "daft_basic.sql",
    "LIMIT": "daft_basic.sql",
    "FILTER": "daft_filter.sql",
    "HASH_JOIN": "daft_join.sql",
    "HASH_AGGREGATE": "daft_aggregate.sql",
    "SORT": "daft_sort_topn.sql",
    "TOP_N": "daft_sort_topn.sql",
}


def fail(message: str) -> None:
    raise SystemExit(f"P2 capability audit failed: {message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    registry_path = args.workspace / "pg_orca/spec/pg_daft_capabilities.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if registry.get("runtime_policy", {}).get("default_enabled") is not False:
        fail("runtime default must remain disabled")
    if registry.get("runtime_policy", {}).get("unknown_is_reject") is not True:
        fail("unknown capabilities must fail closed")
    if registry.get("runtime_policy", {}).get("registry_row_is_not_runtime_bit") is not True:
        fail("registry rows must not be treated as one-to-one runtime bits")

    bit_entries = registry.get("runtime_capability_bits", [])
    bit_ids = [entry.get("id") for entry in bit_entries]
    if len(bit_ids) != len(set(bit_ids)) or len(bit_ids) != 19:
        fail("runtime bit catalog must contain 19 unique entries")
    enabled_bits = {
        entry["id"] for entry in bit_entries if entry.get("initial_enabled") is True
    }
    disabled_bits = set(bit_ids) - enabled_bits

    plan_nodes = registry.get("plan_nodes", [])
    plan_ids = {entry.get("id") for entry in plan_nodes}
    if plan_ids != set(EXPECTED_PLAN_TESTS):
        fail("enabled plan-node catalog differs from the frozen eight-node set")
    for entry in plan_nodes:
        plan_id = entry["id"]
        if entry.get("status") not in ADMITTED_STATUSES:
            fail(f"plan node {plan_id} is not explicitly admitted")
        required_bits = set(entry.get("requires_runtime_bits", []))
        if not required_bits or not required_bits <= enabled_bits:
            fail(f"plan node {plan_id} requires a missing or disabled runtime bit")
        if not entry.get("implementation_evidence"):
            fail(f"plan node {plan_id} has no implementation evidence")
        sql_path = args.workspace / "pg_orca/test/sql" / EXPECTED_PLAN_TESTS[plan_id]
        expected_path = args.workspace / "pg_orca/test/expected" / EXPECTED_PLAN_TESTS[
            plan_id
        ].replace(".sql", ".out")
        if not sql_path.is_file() or not expected_path.is_file():
            fail(f"plan node {plan_id} has no checked-in differential regression")

    types = registry.get("types", [])
    if len(types) != 10 or any(entry.get("status") not in ADMITTED_STATUSES for entry in types):
        fail("the frozen ten-type lowering catalog is incomplete")

    expressions = registry.get("expression_families", [])
    blocked_expressions = {
        entry.get("id") for entry in expressions if entry.get("status") == "BLOCKED"
    }
    if blocked_expressions != {"ARITHMETIC", "CAST"}:
        fail("expression allow/reject catalog drifted")

    rules = registry.get("optimizer_rules", [])
    if not rules:
        fail("optimizer-rule catalog is empty")
    for rule in rules:
        name = rule.get("name", "(unnamed)")
        status = rule.get("current_status")
        if status not in ADMITTED_STATUSES | {"BLOCKED", "NOT_APPLICABLE"}:
            fail(f"optimizer rule {name} has an unknown status")
        if not rule.get("test_ids"):
            fail(f"optimizer rule {name} has no differential/audit test id")
        if status == "BLOCKED" and not str(rule.get("blocked_reason_code", "")).startswith(
            "PGDAFT_"
        ):
            fail(f"optimizer rule {name} has no stable reject reason")
        if rule.get("requires_runtime_bits"):
            fail(f"optimizer rule {name} illegally owns a runtime bit")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    report_lines = [
        "report_version: 1",
        "status: PASS",
        f"runtime_bits_total: {len(bit_ids)}",
        f"runtime_bits_enabled: {len(enabled_bits)}",
        f"runtime_bits_disabled: {len(disabled_bits)}",
        f"enabled_plan_nodes: {len(plan_nodes)}",
        f"admitted_types: {len(types)}",
        f"expression_families: {len(expressions)}",
        f"blocked_expression_families: {len(blocked_expressions)}",
        f"optimizer_rules_audited: {len(rules)}",
        "optimizer_rule_runtime_bits: 0",
        "stable_reject_policy: PASS",
        "differential_suite_mapping: PASS",
        "skipped_cases: 0",
    ]
    args.report.write_text("\n".join(report_lines) + "\n", encoding="ascii")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Generate C++ and Rust capability tables from pg_daft_capabilities.yaml

This generator creates deterministic outputs from a single source of truth.
The generated files include:
- C++ capability table (pg_orca/include/pg_orca/daft/capabilities_generated.h)
- Rust capability table (daft/src/daft-pg-bridge/src/capabilities.rs)
- SHA-256 digest of the registry
"""

import copy
import hashlib
import json
import re
import sys
import yaml
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
DAFT_OPTIMIZER_SOURCE = Path(
    "daft/src/daft-logical-plan/src/optimization/optimizer.rs"
)
DAFT_OPTIMIZER_RULES_ROOT = Path(
    "daft/src/daft-logical-plan/src/optimization/rules"
)
REQUIRED_PHYSICAL_DECISIONS = {
    "determine_join_strategy": Path(
        "daft/src/daft-distributed/src/pipeline_node/join/translate_join.rs"
    ),
}
DAFT_DIRECT_LOWERING_SOURCES = (
    Path("daft/src/daft-pg-bridge/src/plan.rs"),
    Path("daft/src/daft-pg-bridge/src/execution.rs"),
)
ORCA_XFORM_IDS_SOURCE = Path("pg_orca/libgpopt/include/gpopt/xforms/CXform.h")
RULE_REGRESSION_SOURCE = Path("pg_orca/test/sql/daft_rules.sql")
RULE_CLASSIFICATIONS = {
    "ORCA_EQUIVALENT",
    "PORTED_XFORM",
    "CBO_PHYSICAL_ALT",
    "DAFT_LOWERING_ONLY",
    "NOT_APPLICABLE",
    "BLOCKED",
}
ADMITTED_RULE_STATUSES = {
    "ORCA_EQUIVALENT",
    "PORTED_XFORM",
    "CBO_PHYSICAL_ALT",
    "DAFT_LOWERING_ONLY",
}
LOWERING_INVARIANT_ORDER = (
    "preserves_global_winner",
    "preserves_cardinality",
    "preserves_order",
    "preserves_distribution",
    "preserves_engine_boundaries",
)
LOWERING_INVARIANTS = set(LOWERING_INVARIANT_ORDER)


class CapabilityRegistryError(ValueError):
    """Raised when the canonical registry and frozen source disagree."""


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise CapabilityRegistryError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_capabilities(yaml_path: Path) -> Dict[str, Any]:
    """Load and parse the capabilities YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        capabilities = yaml.load(f, Loader=UniqueKeyLoader)
    if not isinstance(capabilities, dict):
        raise CapabilityRegistryError("capability registry must be a YAML mapping")
    return capabilities


def _extract_braced_block(source: str, open_brace: int) -> Tuple[str, int]:
    """Return a Rust braced block and its closing-brace offset."""
    if open_brace >= len(source) or source[open_brace] != '{':
        raise CapabilityRegistryError("internal source discovery error: expected '{'")

    depth = 0
    for offset in range(open_brace, len(source)):
        char = source[offset]
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                return source[open_brace + 1:offset], offset
    raise CapabilityRegistryError("unterminated Rust braced block during rule discovery")


def _discover_optimizer_stages(optimizer_source: Path) -> Dict[str, Set[str]]:
    """Discover every production OptimizerBuilder stage and its rule types."""
    source = optimizer_source.read_text(encoding='utf-8')
    # Test-only OptimizerRule implementations and ad-hoc RuleBatch values are not
    # part of the production rule catalog.
    production_source = source.split("\n#[cfg(test)]", 1)[0]
    impl_match = re.search(r"\bimpl\s+OptimizerBuilder\s*\{", production_source)
    if impl_match is None:
        raise CapabilityRegistryError(
            f"cannot find impl OptimizerBuilder in {optimizer_source}"
        )
    impl_open = production_source.find('{', impl_match.start())
    impl_body, _ = _extract_braced_block(production_source, impl_open)

    stages: Dict[str, Set[str]] = {}
    method_pattern = re.compile(r"\bpub\s+fn\s+([a-z][A-Za-z0-9_]*)\s*\(")
    for method_match in method_pattern.finditer(impl_body):
        method_name = method_match.group(1)
        method_open = impl_body.find('{', method_match.end())
        if method_open < 0:
            raise CapabilityRegistryError(
                f"cannot find body for OptimizerBuilder::{method_name}"
            )
        method_body, _ = _extract_braced_block(impl_body, method_open)
        if "RuleBatch::new" not in method_body:
            continue

        rules = set(re.findall(
            r"Box\s*::\s*new\s*\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*"
            r"([A-Z][A-Za-z0-9_]*)",
            method_body,
        ))
        if not rules:
            raise CapabilityRegistryError(
                f"OptimizerBuilder::{method_name} creates RuleBatch values "
                "but no rule constructors were discovered"
            )
        stages[method_name] = rules

    if "with_default_optimizations" not in stages:
        raise CapabilityRegistryError(
            "with_default_optimizations is missing from the discovered rule stages"
        )
    return stages


def _discover_optimizer_rule_implementations(rules_root: Path) -> Dict[str, str]:
    """Discover concrete OptimizerRule implementations below the rules root."""
    if not rules_root.is_dir():
        raise CapabilityRegistryError(f"optimizer rules root is missing: {rules_root}")

    implementations: List[Tuple[str, str]] = []
    for source_path in sorted(rules_root.rglob('*.rs')):
        source = source_path.read_text(encoding='utf-8')
        relative_path = source_path.relative_to(WORKSPACE_ROOT).as_posix()
        implementations.extend(
            (name, relative_path)
            for name in re.findall(
                r"\bimpl\s+OptimizerRule\s+for\s+([A-Z][A-Za-z0-9_]*)\b",
                source,
            )
        )

    duplicates = sorted(
        name
        for name, count in Counter(name for name, _ in implementations).items()
        if count > 1
    )
    if duplicates:
        raise CapabilityRegistryError(
            f"duplicate OptimizerRule implementations discovered: {duplicates}"
        )
    if not implementations:
        raise CapabilityRegistryError(
            f"no OptimizerRule implementations discovered below {rules_root}"
        )
    return dict(implementations)


def _unique_named_entries(
    capabilities: Dict[str, Any], section: str
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    entries = capabilities.get(section)
    if not isinstance(entries, list):
        raise CapabilityRegistryError(f"{section} must be a YAML list")
    if any(not isinstance(entry, dict) for entry in entries):
        raise CapabilityRegistryError(f"every {section} entry must be a mapping")

    names = [entry.get('name') for entry in entries]
    if any(not isinstance(name, str) or not name for name in names):
        raise CapabilityRegistryError(f"every {section} entry must have a name")
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise CapabilityRegistryError(f"duplicate {section} entries: {duplicates}")
    return entries, set(names)


def _require_unique_string_list(entry: Dict[str, Any], field: str) -> List[str]:
    values = entry.get(field)
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
    ):
        raise CapabilityRegistryError(
            f"registry entry {entry.get('name')} must have non-empty {field}"
        )
    if len(values) != len(set(values)):
        raise CapabilityRegistryError(
            f"registry entry {entry.get('name')} has duplicate {field}"
        )
    return values


def _known_xform_ids() -> Set[str]:
    xform_source = WORKSPACE_ROOT / ORCA_XFORM_IDS_SOURCE
    if not xform_source.is_file():
        raise CapabilityRegistryError(
            f"ORCA Xform ID source is missing: {ORCA_XFORM_IDS_SOURCE}"
        )
    return set(re.findall(
        r"^\s*(Exf[A-Za-z0-9_]+)\s*(?:=\s*[^,]+)?\s*,",
        xform_source.read_text(encoding='utf-8'),
        re.MULTILINE,
    ))


def _validate_source_evidence(name: str, evidence: List[str]) -> None:
    for source_ref in evidence:
        source_path = source_ref.split('#', 1)[0]
        if not (WORKSPACE_ROOT / source_path).is_file():
            raise CapabilityRegistryError(
                f"{name} source evidence is missing: {source_path}"
            )


def _validate_rule_evidence(
    capabilities: Dict[str, Any],
    entries: List[Dict[str, Any]],
    implementations: Dict[str, str],
) -> None:
    known_xforms = _known_xform_ids()
    daft_sha = capabilities.get('source_lock', {}).get('daft')
    if not isinstance(daft_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", daft_sha):
        raise CapabilityRegistryError("source_lock.daft must be a 40-digit SHA")

    for entry in entries:
        name = entry['name']
        status = entry.get('current_status')
        target = entry.get('target_classification')
        if status not in RULE_CLASSIFICATIONS or target not in RULE_CLASSIFICATIONS:
            raise CapabilityRegistryError(
                f"optimizer rule {name} has an invalid status/classification"
            )
        if entry.get('daft_source_file') != implementations[name]:
            raise CapabilityRegistryError(
                f"optimizer rule {name} source mismatch: "
                f"expected {implementations[name]}, got {entry.get('daft_source_file')}"
            )
        if entry.get('daft_git_sha') != daft_sha:
            raise CapabilityRegistryError(
                f"optimizer rule {name} must bind frozen Daft SHA {daft_sha}"
            )
        if not isinstance(entry.get('requires_stats'), bool):
            raise CapabilityRegistryError(
                f"optimizer rule {name} requires_stats must be boolean"
            )
        if not isinstance(entry.get('changes_cardinality_or_order'), bool):
            raise CapabilityRegistryError(
                f"optimizer rule {name} changes_cardinality_or_order must be boolean"
            )

        _require_unique_string_list(entry, 'semantic_preconditions')
        _require_unique_string_list(entry, 'supported_execution_modes')
        test_ids = _require_unique_string_list(entry, 'test_ids')
        if any(not test_id.startswith('P1-004-C/daft_rules/') for test_id in test_ids):
            raise CapabilityRegistryError(
                f"optimizer rule {name} test IDs must use P1-004-C/daft_rules/"
            )
        mapping = entry.get('orca_mapping')
        if not isinstance(mapping, dict):
            raise CapabilityRegistryError(
                f"optimizer rule {name} must have an orca_mapping"
            )
        xform_ids = mapping.get('xform_ids')
        if not isinstance(xform_ids, list) or any(
            not isinstance(xform_id, str) for xform_id in xform_ids
        ):
            raise CapabilityRegistryError(
                f"optimizer rule {name} orca_mapping.xform_ids must be a list"
            )
        if len(xform_ids) != len(set(xform_ids)):
            raise CapabilityRegistryError(
                f"optimizer rule {name} has duplicate ORCA Xform IDs"
            )
        unknown_xforms = sorted(set(xform_ids) - known_xforms)
        if unknown_xforms:
            raise CapabilityRegistryError(
                f"optimizer rule {name} references unknown Xform IDs: {unknown_xforms}"
            )
        if status == 'ORCA_EQUIVALENT' and not xform_ids:
            raise CapabilityRegistryError(
                f"ORCA_EQUIVALENT rule {name} must name concrete Xform IDs"
            )

        bridge = mapping.get('cost_or_stats_bridge')
        if not isinstance(bridge, str) or not bridge:
            raise CapabilityRegistryError(
                f"optimizer rule {name} must name cost_or_stats_bridge or NONE"
            )
        source_evidence = _require_unique_string_list(mapping, 'source_evidence')
        if entry['daft_source_file'] not in {
            evidence.split('#', 1)[0] for evidence in source_evidence
        }:
            raise CapabilityRegistryError(
                f"optimizer rule {name} source evidence must include its Daft file"
            )
        _validate_source_evidence(f"optimizer rule {name}", source_evidence)

        if entry.get('requires_runtime_bits'):
            raise CapabilityRegistryError(
                f"optimizer rule {name} cannot own runtime capability bits"
            )

        reason = entry.get('blocked_reason_code')
        if status == 'BLOCKED':
            if not isinstance(reason, str) or re.fullmatch(
                r"PGDAFT_RULE_[A-Z0-9_]+", reason
            ) is None:
                raise CapabilityRegistryError(
                    f"BLOCKED optimizer rule {name} needs a stable PGDAFT_RULE reason"
                )
        elif reason is not None:
            raise CapabilityRegistryError(
                f"admitted optimizer rule {name} must not retain a blocked reason"
            )

        if entry['requires_stats']:
            if status == 'DAFT_LOWERING_ONLY' or target == 'DAFT_LOWERING_ONLY':
                raise CapabilityRegistryError(
                    f"requires_stats optimizer rule {name} cannot be DAFT_LOWERING_ONLY"
                )
            if bridge == 'NONE' and status != 'BLOCKED':
                raise CapabilityRegistryError(
                    f"requires_stats optimizer rule {name} needs a cost/stats bridge"
                )

        lowering_contract = entry.get('lowering_contract')
        if target == 'DAFT_LOWERING_ONLY' or status == 'DAFT_LOWERING_ONLY':
            if entry['requires_stats'] or entry['changes_cardinality_or_order']:
                raise CapabilityRegistryError(
                    f"lowering-only optimizer rule {name} cannot affect CBO/cardinality/order"
                )
            if not isinstance(lowering_contract, dict) or set(lowering_contract) != LOWERING_INVARIANTS:
                raise CapabilityRegistryError(
                    f"lowering-only optimizer rule {name} has incomplete invariants"
                )
            if any(lowering_contract[invariant] is not True for invariant in LOWERING_INVARIANTS):
                raise CapabilityRegistryError(
                    f"lowering-only optimizer rule {name} must preserve every invariant"
                )
        elif lowering_contract is not None:
            raise CapabilityRegistryError(
                f"non-lowering optimizer rule {name} must not have lowering_contract"
            )
        if status in ADMITTED_RULE_STATUSES:
            test_source_path = WORKSPACE_ROOT / RULE_REGRESSION_SOURCE
            if not test_source_path.is_file():
                raise CapabilityRegistryError(
                    f"admitted optimizer rule {name} has no rule regression source"
                )
            test_source = test_source_path.read_text(encoding='utf-8')
            missing_test_ids = [
                test_id for test_id in test_ids if test_id not in test_source
            ]
            if missing_test_ids:
                raise CapabilityRegistryError(
                    f"admitted optimizer rule {name} has unbound test IDs: "
                    f"{missing_test_ids}"
                )


def _validate_physical_decision_evidence(
    capabilities: Dict[str, Any], entry: Dict[str, Any]
) -> None:
    name = entry['name']
    status = entry.get('current_status')
    target = entry.get('target_classification')
    if status not in RULE_CLASSIFICATIONS or target != 'CBO_PHYSICAL_ALT':
        raise CapabilityRegistryError(
            f"physical decision {name} must target CBO_PHYSICAL_ALT"
        )
    if entry.get('requires_stats') is not True:
        raise CapabilityRegistryError(
            f"physical decision {name} must declare requires_stats: true"
        )
    if not isinstance(entry.get('changes_cardinality_or_order'), bool):
        raise CapabilityRegistryError(
            f"physical decision {name} changes_cardinality_or_order must be boolean"
        )
    daft_sha = capabilities.get('source_lock', {}).get('daft')
    if entry.get('daft_git_sha') != daft_sha:
        raise CapabilityRegistryError(
            f"physical decision {name} must bind frozen Daft SHA {daft_sha}"
        )
    _require_unique_string_list(entry, 'semantic_preconditions')
    _require_unique_string_list(entry, 'supported_execution_modes')
    test_ids = _require_unique_string_list(entry, 'test_ids')
    if any(not test_id.startswith('P1-004-C/daft_rules/') for test_id in test_ids):
        raise CapabilityRegistryError(
            f"physical decision {name} test IDs must use P1-004-C/daft_rules/"
        )
    mapping = entry.get('orca_mapping')
    if not isinstance(mapping, dict):
        raise CapabilityRegistryError(
            f"physical decision {name} must have an orca_mapping"
        )
    xform_ids = mapping.get('xform_ids')
    if not isinstance(xform_ids, list) or not xform_ids or any(
        not isinstance(xform_id, str) for xform_id in xform_ids
    ):
        raise CapabilityRegistryError(
            f"physical decision {name} must name candidate ORCA Xform IDs"
        )
    if len(xform_ids) != len(set(xform_ids)):
        raise CapabilityRegistryError(
            f"physical decision {name} has duplicate ORCA Xform IDs"
        )
    unknown_xforms = sorted(set(xform_ids) - _known_xform_ids())
    if unknown_xforms:
        raise CapabilityRegistryError(
            f"physical decision {name} references unknown Xform IDs: {unknown_xforms}"
        )
    bridge = mapping.get('cost_or_stats_bridge')
    if not isinstance(bridge, str) or bridge == 'NONE':
        raise CapabilityRegistryError(
            f"requires_stats physical decision {name} needs a cost/stats bridge"
        )
    source_evidence = _require_unique_string_list(mapping, 'source_evidence')
    if entry['source_file'] not in {
        evidence.split('#', 1)[0] for evidence in source_evidence
    }:
        raise CapabilityRegistryError(
            f"physical decision {name} source evidence must include its Daft file"
        )
    _validate_source_evidence(f"physical decision {name}", source_evidence)

    reason = entry.get('blocked_reason_code')
    if status == 'BLOCKED':
        if not isinstance(reason, str) or re.fullmatch(
            r"PGDAFT_RULE_[A-Z0-9_]+", reason
        ) is None:
            raise CapabilityRegistryError(
                f"BLOCKED physical decision {name} needs a stable PGDAFT_RULE reason"
            )
    elif reason is not None:
        raise CapabilityRegistryError(
            f"admitted physical decision {name} must not retain a blocked reason"
        )
    if status in ADMITTED_RULE_STATUSES:
        test_source_path = WORKSPACE_ROOT / RULE_REGRESSION_SOURCE
        if not test_source_path.is_file():
            raise CapabilityRegistryError(
                f"admitted physical decision {name} has no rule regression source"
            )
        test_source = test_source_path.read_text(encoding='utf-8')
        missing_test_ids = [
            test_id for test_id in test_ids if test_id not in test_source
        ]
        if missing_test_ids:
            raise CapabilityRegistryError(
                f"admitted physical decision {name} has unbound test IDs: "
                f"{missing_test_ids}"
            )


def _validate_rule_catalog_sets(
    staged_rules: Set[str], implemented_rules: Set[str], yaml_rules: Set[str]
) -> None:
    if staged_rules != implemented_rules:
        raise CapabilityRegistryError(
            "Daft OptimizerRule implementation/stage mismatch: "
            f"not_in_any_stage={sorted(implemented_rules - staged_rules)} "
            f"stage_without_implementation={sorted(staged_rules - implemented_rules)}"
        )
    if yaml_rules != staged_rules:
        raise CapabilityRegistryError(
            "Daft optimizer rule source/YAML mismatch: "
            f"source_only={sorted(staged_rules - yaml_rules)} "
            f"yaml_only={sorted(yaml_rules - staged_rules)}"
        )


def _validate_direct_lowering_boundary() -> int:
    forbidden = (
        r"\bOptimizerBuilder\b",
        r"\bwith_[a-z0-9_]*optimizations\b",
        r"daft_logical_plan::optimization",
        r"\.optimize\s*\(",
    )
    sources = {}
    for relative_path in DAFT_DIRECT_LOWERING_SOURCES:
        source_path = WORKSPACE_ROOT / relative_path
        if not source_path.is_file():
            raise CapabilityRegistryError(
                f"direct-lowering source is missing: {relative_path}"
            )
        sources[relative_path] = source_path.read_text(encoding='utf-8')
    for relative_path, source in sources.items():
        for pattern in forbidden:
            if re.search(pattern, source):
                raise CapabilityRegistryError(
                    "embedded bridge must not invoke the Daft logical optimizer: "
                    f"{relative_path} matches {pattern}"
                )
    plan_source = sources[DAFT_DIRECT_LOWERING_SOURCES[0]]
    execution_source = sources[DAFT_DIRECT_LOWERING_SOURCES[1]]
    if "pub fn translate_plan(" not in plan_source or "LocalPhysicalPlan" not in plan_source:
        raise CapabilityRegistryError(
            "plan.rs must directly lower canonical IR to LocalPhysicalPlan"
        )
    if "plan::translate_plan" not in execution_source or "translate_plan(&ir)" not in execution_source:
        raise CapabilityRegistryError(
            "execution.rs must call the audited direct-lowering entry point"
        )
    return len(sources)


def validate_source_catalog(capabilities: Dict[str, Any]) -> Tuple[int, int, int, int]:
    """Bidirectionally validate Rust rule sources against canonical YAML."""
    optimizer_source = WORKSPACE_ROOT / DAFT_OPTIMIZER_SOURCE
    rules_root = WORKSPACE_ROOT / DAFT_OPTIMIZER_RULES_ROOT
    if not optimizer_source.is_file():
        raise CapabilityRegistryError(
            f"optimizer source is missing: {DAFT_OPTIMIZER_SOURCE}"
        )

    stages = _discover_optimizer_stages(optimizer_source)
    staged_rules = set().union(*stages.values())
    implementations = _discover_optimizer_rule_implementations(rules_root)
    implemented_rules = set(implementations)
    rule_entries, yaml_rules = _unique_named_entries(capabilities, 'optimizer_rules')
    _validate_rule_catalog_sets(staged_rules, implemented_rules, yaml_rules)
    _validate_rule_evidence(capabilities, rule_entries, implementations)

    physical_entries, yaml_physical = _unique_named_entries(
        capabilities, 'physical_decisions'
    )
    required_physical = set(REQUIRED_PHYSICAL_DECISIONS)
    if yaml_physical != required_physical:
        raise CapabilityRegistryError(
            "Daft physical decision source/YAML mismatch: "
            f"source_only={sorted(required_physical - yaml_physical)} "
            f"yaml_only={sorted(yaml_physical - required_physical)}"
        )

    for entry in physical_entries:
        name = entry['name']
        expected_source = REQUIRED_PHYSICAL_DECISIONS[name]
        if entry.get('source_file') != expected_source.as_posix():
            raise CapabilityRegistryError(
                f"physical decision {name} must reference {expected_source}"
            )
        if entry.get('source_kind') != 'distributed_translator_function_not_OptimizerRule':
            raise CapabilityRegistryError(
                f"physical decision {name} must remain separate from OptimizerRule"
            )
        source_path = WORKSPACE_ROOT / expected_source
        if not source_path.is_file():
            raise CapabilityRegistryError(
                f"physical decision source is missing: {expected_source}"
            )
        source = source_path.read_text(encoding='utf-8')
        if re.search(rf"\bfn\s+{re.escape(name)}\s*\(", source) is None:
            raise CapabilityRegistryError(
                f"physical decision {name} is absent from {expected_source}"
            )
        if name in implemented_rules:
            raise CapabilityRegistryError(
                f"physical decision {name} must not be an OptimizerRule"
            )
        _validate_physical_decision_evidence(capabilities, entry)

    optional_stage_count = len(stages) - 1
    direct_lowering_source_count = _validate_direct_lowering_boundary()
    return (
        len(yaml_rules),
        optional_stage_count,
        len(yaml_physical),
        direct_lowering_source_count,
    )


def compute_digest(capabilities: Dict[str, Any]) -> str:
    """Compute SHA-256 digest of the capability registry.

    Uses canonical YAML serialization for deterministic output.
    """
    # Serialize in canonical form
    canonical = yaml.dump(
        capabilities,
        default_flow_style=False,
        sort_keys=True,
        allow_unicode=True
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _string_literal(value: str) -> str:
    """Return a deterministic string literal accepted by C++ and Rust."""
    return json.dumps(value, ensure_ascii=True)


def _joined_literal(values: List[str]) -> str:
    return _string_literal("|".join(values))


def _lowering_invariant_mask(rule: Dict[str, Any]) -> int:
    contract = rule.get('lowering_contract', {})
    return sum(
        1 << bit
        for bit, invariant in enumerate(LOWERING_INVARIANT_ORDER)
        if contract.get(invariant) is True
    )


def _run_negative_ci_selftests(capabilities: Dict[str, Any]) -> int:
    """Prove the catalog/evidence guards reject representative mutations."""
    rejected = 0

    def expect_error(label: str, expected: str, callback) -> None:
        nonlocal rejected
        try:
            callback()
        except CapabilityRegistryError as exc:
            if expected not in str(exc):
                raise CapabilityRegistryError(
                    f"negative CI {label} raised an unexpected error: {exc}"
                ) from exc
            rejected += 1
            return
        raise CapabilityRegistryError(
            f"negative CI {label} accepted an invalid catalog mutation"
        )

    yaml_rules = {entry['name'] for entry in capabilities['optimizer_rules']}
    synthetic_source_rules = yaml_rules | {'SyntheticRustOptimizerRule'}
    expect_error(
        'rust-catalog-without-yaml',
        'source/YAML mismatch',
        lambda: _validate_rule_catalog_sets(
            synthetic_source_rules, synthetic_source_rules, yaml_rules
        ),
    )

    def mutated_rule(name: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        mutated = copy.deepcopy(capabilities)
        entry = next(
            item for item in mutated['optimizer_rules'] if item['name'] == name
        )
        return mutated, entry

    mutated, entry = mutated_rule('DropRepartition')
    entry['requires_stats'] = True
    expect_error(
        'stats-dependent-lowering',
        'cannot be DAFT_LOWERING_ONLY',
        lambda: validate_source_catalog(mutated),
    )

    mutated, entry = mutated_rule('DropIntoBatches')
    entry['lowering_contract']['preserves_global_winner'] = False
    expect_error(
        'lowering-winner-change',
        'must preserve every invariant',
        lambda: validate_source_catalog(mutated),
    )

    mutated, entry = mutated_rule('LiftProjectFromAgg')
    entry['current_status'] = 'ORCA_EQUIVALENT'
    entry['test_ids'] = []
    expect_error(
        'enabled-without-test',
        'must have non-empty test_ids',
        lambda: validate_source_catalog(mutated),
    )

    mutated, entry = mutated_rule('LiftProjectFromAgg')
    entry['current_status'] = 'ORCA_EQUIVALENT'
    entry.pop('blocked_reason_code')
    expect_error(
        'enabled-with-unbound-test-id',
        'has unbound test IDs',
        lambda: validate_source_catalog(mutated),
    )

    mutated, entry = mutated_rule('EliminateSubqueryAliasRule')
    entry['current_status'] = 'ORCA_EQUIVALENT'
    expect_error(
        'equivalent-without-xform',
        'must name concrete Xform IDs',
        lambda: validate_source_catalog(mutated),
    )

    mutated, entry = mutated_rule('PushDownAggregation')
    entry['current_status'] = 'CBO_PHYSICAL_ALT'
    entry.pop('blocked_reason_code')
    entry['orca_mapping']['cost_or_stats_bridge'] = 'NONE'
    expect_error(
        'stats-choice-without-cost-bridge',
        'needs a cost/stats bridge',
        lambda: validate_source_catalog(mutated),
    )

    mutated = copy.deepcopy(capabilities)
    physical = mutated['physical_decisions'][0]
    physical['orca_mapping']['cost_or_stats_bridge'] = 'NONE'
    expect_error(
        'physical-choice-without-cost-bridge',
        'needs a cost/stats bridge',
        lambda: validate_source_catalog(mutated),
    )
    return rejected


def generate_cpp_header(capabilities: Dict[str, Any], digest: str, output_path: Path):
    """Generate C++ capability table header."""
    lines = [
        "// GENERATED FILE - DO NOT EDIT",
        "// Generated from pg_daft_capabilities.yaml",
        "// Generator: pg_orca/tools/gen_daft_capabilities.py",
        "",
        "#ifndef PG_ORCA_DAFT_CAPABILITIES_GENERATED_H",
        "#define PG_ORCA_DAFT_CAPABILITIES_GENERATED_H",
        "",
        "#include <cstddef>",
        "#include <cstdint>",
        "#include <string_view>",
        "",
        "namespace pg_orca {",
        "namespace daft {",
        "",
        f"constexpr uint32_t REGISTRY_VERSION = {capabilities['registry_version']}u;",
        f"constexpr uint32_t IR_MAJOR = {capabilities['protocol']['ir_major']}u;",
        f"constexpr uint32_t IR_MINOR = {capabilities['protocol']['ir_minor']}u;",
        f"constexpr uint32_t ABI_MAJOR = {capabilities['protocol']['abi_major']}u;",
        f"constexpr uint32_t ABI_MINOR = {capabilities['protocol']['abi_minor']}u;",
        "",
        f'constexpr std::string_view CAPABILITY_DIGEST = "{digest}";',
        "",
        "// Runtime capability bits",
        "enum class RuntimeCapability : uint64_t {",
    ]

    for bit_entry in capabilities['runtime_capability_bits']:
        bit = bit_entry['bit']
        id_name = bit_entry['id']
        lines.append(f"    {id_name} = (1ULL << {bit}),")

    lines.extend([
        "};",
        "",
        "struct TypeCapabilityEntry {",
        "    const char* id;",
        "    uint32_t pg_oid;",
        "    const char* arrow_format;",
        "    const char* daft_dtype;",
        "    const char* status;",
        "    const char* phase;",
        "};",
        "",
        "constexpr TypeCapabilityEntry TYPE_CAPABILITIES[] = {",
    ])

    for entry in capabilities.get('types', []):
        lines.append(
            f'    {{"{entry["id"]}", {entry["pg_oid"]}u, '
            f'"{entry["arrow_format"]}", "{entry["daft_dtype"]}", '
            f'"{entry["status"]}", "{entry.get("phase", "P0")}"}},'
        )

    lines.extend([
        "};",
        "",
        "constexpr size_t NUM_TYPE_CAPABILITIES =",
        "    sizeof(TYPE_CAPABILITIES) / sizeof(TypeCapabilityEntry);",
        "",
        "struct RegistryCapabilityEntry {",
        "    const char* id;",
        "    const char* status;",
        "    const char* phase;",
        "    uint64_t required_runtime_bits;",
        "};",
        "",
        "constexpr RegistryCapabilityEntry PLAN_NODE_CAPABILITIES[] = {",
    ])

    for entry in capabilities.get('plan_nodes', []):
        required_bits = entry.get('requires_runtime_bits', [])
        bits_expr = ' | '.join(
            f'static_cast<uint64_t>(RuntimeCapability::{b})'
            for b in required_bits
        ) if required_bits else '0'
        lines.append(
            f'    {{"{entry["id"]}", "{entry["status"]}", '
            f'"{entry.get("phase", "P0")}", {bits_expr}}},'
        )

    lines.extend([
        "};",
        "",
        "constexpr size_t NUM_PLAN_NODE_CAPABILITIES =",
        "    sizeof(PLAN_NODE_CAPABILITIES) / sizeof(RegistryCapabilityEntry);",
        "",
        "constexpr RegistryCapabilityEntry EXPRESSION_CAPABILITIES[] = {",
    ])

    for entry in capabilities.get('expression_families', []):
        required_bits = entry.get('requires_runtime_bits', [])
        bits_expr = ' | '.join(
            f'static_cast<uint64_t>(RuntimeCapability::{b})'
            for b in required_bits
        ) if required_bits else '0'
        lines.append(
            f'    {{"{entry["id"]}", "{entry["status"]}", '
            f'"{entry.get("phase", "P0")}", {bits_expr}}},'
        )

    lines.extend([
        "};",
        "",
        "constexpr size_t NUM_EXPRESSION_CAPABILITIES =",
        "    sizeof(EXPRESSION_CAPABILITIES) / sizeof(RegistryCapabilityEntry);",
        "",
        "// Optimizer rule registry",
        "struct OptimizerRuleEntry {",
        "    const char* rule_name;",
        "    const char* status;",
        "    const char* target_classification;",
        "    const char* phase;",
        "    uint64_t required_runtime_bits;",
        "    bool requires_stats;",
        "    bool changes_cardinality_or_order;",
        "    uint8_t lowering_invariant_mask;",
        "    const char* daft_source_file;",
        "    const char* daft_git_sha;",
        "    const char* semantic_preconditions;",
        "    const char* supported_execution_modes;",
        "    const char* orca_xform_ids;",
        "    const char* cost_or_stats_bridge;",
        "    const char* test_ids;",
        "    const char* source_evidence;",
        "    const char* blocked_reason_code;",
        "};",
        "",
        f"constexpr OptimizerRuleEntry OPTIMIZER_RULES[] = {{",
    ])

    for rule in capabilities.get('optimizer_rules', []):
        rule_name = rule['name']
        status = rule['current_status']
        phase = rule.get('phase', 'P0')
        required_bits = rule.get('requires_runtime_bits', [])
        bits_expr = ' | '.join(f'static_cast<uint64_t>(RuntimeCapability::{b})' for b in required_bits) if required_bits else '0'
        mapping = rule['orca_mapping']
        lines.append(
            "    {"
            f"{_string_literal(rule_name)}, {_string_literal(status)}, "
            f"{_string_literal(rule['target_classification'])}, "
            f"{_string_literal(phase)}, {bits_expr}, "
            f"{str(rule['requires_stats']).lower()}, "
            f"{str(rule['changes_cardinality_or_order']).lower()}, "
            f"{_lowering_invariant_mask(rule)}u, "
            f"{_string_literal(rule['daft_source_file'])}, "
            f"{_string_literal(rule['daft_git_sha'])}, "
            f"{_joined_literal(rule['semantic_preconditions'])}, "
            f"{_joined_literal(rule['supported_execution_modes'])}, "
            f"{_joined_literal(mapping['xform_ids'])}, "
            f"{_string_literal(mapping['cost_or_stats_bridge'])}, "
            f"{_joined_literal(rule['test_ids'])}, "
            f"{_joined_literal(mapping['source_evidence'])}, "
            f"{_string_literal(rule.get('blocked_reason_code', ''))}"
            "},"
        )

    lines.extend([
        "};",
        "",
        f"constexpr size_t NUM_OPTIMIZER_RULES = sizeof(OPTIMIZER_RULES) / sizeof(OptimizerRuleEntry);",
        "",
        "}  // namespace daft",
        "}  // namespace pg_orca",
        "",
        "#endif  // PG_ORCA_DAFT_CAPABILITIES_GENERATED_H",
        ""
    ])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('\n'.join(lines))


def generate_rust_module(capabilities: Dict[str, Any], digest: str, output_path: Path):
    """Generate Rust capability module."""
    lines = [
        "// GENERATED FILE - DO NOT EDIT",
        "// Generated from pg_daft_capabilities.yaml",
        "// Generator: pg_orca/tools/gen_daft_capabilities.py",
        "",
        "//! Capability registry types and validation",
        "",
        f"pub const REGISTRY_VERSION: u32 = {capabilities['registry_version']};",
        f"pub const IR_MAJOR: u32 = {capabilities['protocol']['ir_major']};",
        f"pub const IR_MINOR: u32 = {capabilities['protocol']['ir_minor']};",
        f"pub const ABI_MAJOR: u32 = {capabilities['protocol']['abi_major']};",
        f"pub const ABI_MINOR: u32 = {capabilities['protocol']['abi_minor']};",
        "",
        'pub const CAPABILITY_DIGEST: &str =',
        f'    "{digest}";',
        "",
        "/// Runtime capability bits",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "#[repr(u64)]",
        "pub enum RuntimeCapability {",
    ]

    for bit_entry in capabilities['runtime_capability_bits']:
        bit = bit_entry['bit']
        id_name = bit_entry['id']
        lines.append(f"    {id_name} = 1u64 << {bit},")

    lines.extend([
        "}",
        "",
        "/// Generated type-capability row.",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub struct TypeCapabilityEntry {",
        "    pub id: &'static str,",
        "    pub pg_oid: u32,",
        "    pub arrow_format: &'static str,",
        "    pub daft_dtype: &'static str,",
        "    pub status: RuleStatus,",
        "    pub phase: &'static str,",
        "}",
        "",
        "/// Generated plan-node or expression-family capability row.",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub struct RegistryCapabilityEntry {",
        "    pub id: &'static str,",
        "    pub status: RuleStatus,",
        "    pub phase: &'static str,",
        "    pub required_runtime_bits: u64,",
        "}",
        "",
        "pub const TYPE_CAPABILITIES: &[TypeCapabilityEntry] = &[",
    ])

    for entry in capabilities.get('types', []):
        lines.append("    TypeCapabilityEntry {")
        lines.append(f'        id: "{entry["id"]}",')
        lines.append(f'        pg_oid: {entry["pg_oid"]},')
        lines.append(f'        arrow_format: "{entry["arrow_format"]}",')
        lines.append(f'        daft_dtype: "{entry["daft_dtype"]}",')
        lines.append(f'        status: RuleStatus::{entry["status"]},')
        lines.append(f'        phase: "{entry.get("phase", "P0")}",')
        lines.append("    },")

    lines.extend([
        "];",
        "",
        "pub const PLAN_NODE_CAPABILITIES: &[RegistryCapabilityEntry] = &[",
    ])

    for entry in capabilities.get('plan_nodes', []):
        required_bits = entry.get('requires_runtime_bits', [])
        lines.append("    RegistryCapabilityEntry {")
        lines.append(f'        id: "{entry["id"]}",')
        lines.append(f'        status: RuleStatus::{entry["status"]},')
        lines.append(f'        phase: "{entry.get("phase", "P0")}",')
        if required_bits:
            lines.append(
                f'        required_runtime_bits: RuntimeCapability::{required_bits[0]} as u64'
            )
            for bit in required_bits[1:]:
                lines.append(f'            | RuntimeCapability::{bit} as u64')
            lines[-1] += ','
        else:
            lines.append('        required_runtime_bits: 0,')
        lines.append("    },")

    lines.extend([
        "];",
        "",
        "pub const EXPRESSION_CAPABILITIES: &[RegistryCapabilityEntry] = &[",
    ])

    for entry in capabilities.get('expression_families', []):
        required_bits = entry.get('requires_runtime_bits', [])
        lines.append("    RegistryCapabilityEntry {")
        lines.append(f'        id: "{entry["id"]}",')
        lines.append(f'        status: RuleStatus::{entry["status"]},')
        lines.append(f'        phase: "{entry.get("phase", "P0")}",')
        if required_bits:
            lines.append(
                f'        required_runtime_bits: RuntimeCapability::{required_bits[0]} as u64'
            )
            for bit in required_bits[1:]:
                lines.append(f'            | RuntimeCapability::{bit} as u64')
            lines[-1] += ','
        else:
            lines.append('        required_runtime_bits: 0,')
        lines.append("    },")

    lines.extend([
        "];",
        "",
        "/// Optimizer rule status",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum RuleStatus {",
    ])

    for status in capabilities['status_values']:
        lines.append(f"    {status},")

    lines.extend([
        "}",
        "",
        "/// Optimizer rule entry",
        "#[derive(Debug, Clone)]",
        "pub struct OptimizerRuleEntry {",
        "    pub rule_name: &'static str,",
        "    pub status: RuleStatus,",
        "    pub target_classification: &'static str,",
        "    pub phase: &'static str,",
        "    pub required_runtime_bits: u64,",
        "    pub requires_stats: bool,",
        "    pub changes_cardinality_or_order: bool,",
        "    pub lowering_invariant_mask: u8,",
        "    pub daft_source_file: &'static str,",
        "    pub daft_git_sha: &'static str,",
        "    pub semantic_preconditions: &'static str,",
        "    pub supported_execution_modes: &'static str,",
        "    pub orca_xform_ids: &'static str,",
        "    pub cost_or_stats_bridge: &'static str,",
        "    pub test_ids: &'static str,",
        "    pub source_evidence: &'static str,",
        "    pub blocked_reason_code: &'static str,",
        "}",
        "",
        "pub const OPTIMIZER_RULES: &[OptimizerRuleEntry] = &[",
    ])

    for rule in capabilities.get('optimizer_rules', []):
        rule_name = rule['name']
        status = rule['current_status']
        phase = rule.get('phase', 'P0')
        required_bits = rule.get('requires_runtime_bits', [])
        bits_expr = ' | '.join(f'RuntimeCapability::{b} as u64' for b in required_bits) if required_bits else '0'
        mapping = rule['orca_mapping']
        lines.append(f'    OptimizerRuleEntry {{')
        lines.append(f'        rule_name: {_string_literal(rule_name)},')
        lines.append(f'        status: RuleStatus::{status},')
        lines.append(
            f'        target_classification: '
            f'{_string_literal(rule["target_classification"])},'
        )
        lines.append(f'        phase: {_string_literal(phase)},')
        lines.append(f'        required_runtime_bits: {bits_expr},')
        lines.append(f'        requires_stats: {str(rule["requires_stats"]).lower()},')
        lines.append(
            '        changes_cardinality_or_order: '
            f'{str(rule["changes_cardinality_or_order"]).lower()},'
        )
        lines.append(
            f'        lowering_invariant_mask: {_lowering_invariant_mask(rule)},'
        )
        lines.append(
            f'        daft_source_file: {_string_literal(rule["daft_source_file"])},'
        )
        lines.append(f'        daft_git_sha: {_string_literal(rule["daft_git_sha"])},')
        lines.append(
            f'        semantic_preconditions: '
            f'{_joined_literal(rule["semantic_preconditions"])},'
        )
        lines.append(
            f'        supported_execution_modes: '
            f'{_joined_literal(rule["supported_execution_modes"])},'
        )
        lines.append(
            f'        orca_xform_ids: {_joined_literal(mapping["xform_ids"])},'
        )
        lines.append(
            f'        cost_or_stats_bridge: '
            f'{_string_literal(mapping["cost_or_stats_bridge"])},'
        )
        lines.append(f'        test_ids: {_joined_literal(rule["test_ids"])},')
        lines.append(
            f'        source_evidence: {_joined_literal(mapping["source_evidence"])},'
        )
        lines.append(
            f'        blocked_reason_code: '
            f'{_string_literal(rule.get("blocked_reason_code", ""))},'
        )
        lines.append(f'    }},')

    lines.extend([
        "];",
        ""
    ])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('\n'.join(lines))


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generate capability tables from YAML')
    parser.add_argument('--check', action='store_true', help='Verify generated files match source')
    parser.add_argument('--yaml', type=Path, default=Path('pg_orca/spec/pg_daft_capabilities.yaml'))
    args = parser.parse_args()

    try:
        capabilities = load_capabilities(args.yaml)
        (
            rule_count,
            optional_stage_count,
            physical_count,
            direct_lowering_source_count,
        ) = validate_source_catalog(capabilities)
        digest = compute_digest(capabilities)

        cpp_output = Path('pg_orca/include/pg_orca/daft/capabilities_generated.h')
        rust_output = Path('daft/src/daft-pg-bridge/src/capabilities.rs')

        if args.check:
            negative_ci_count = _run_negative_ci_selftests(capabilities)
            # Verify mode: check that generated files match
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_cpp = Path(tmpdir) / 'capabilities_generated.h'
                tmp_rust = Path(tmpdir) / 'capabilities.rs'

                generate_cpp_header(capabilities, digest, tmp_cpp)
                generate_rust_module(capabilities, digest, tmp_rust)

                if cpp_output.exists() and cpp_output.read_text() != tmp_cpp.read_text():
                    print(
                        f"ERROR: {cpp_output} does not match generated output",
                        file=sys.stderr,
                    )
                    return 1

                if rust_output.exists() and rust_output.read_text() != tmp_rust.read_text():
                    print(
                        f"ERROR: {rust_output} does not match generated output",
                        file=sys.stderr,
                    )
                    return 1

            print(
                "PASS: Daft source catalog matches canonical YAML "
                f"({rule_count} rules; 1 default stage; "
                f"{optional_stage_count} optional stages; "
                f"{physical_count} physical decision)"
            )
            rules = capabilities['optimizer_rules']
            physical = capabilities['physical_decisions']
            print(
                "PASS: Rule evidence is complete "
                f"({len(rules) + len(physical)} entries; "
                f"{sum(entry['requires_stats'] for entry in rules + physical)} "
                "require stats; "
                f"{sum(entry['target_classification'] == 'DAFT_LOWERING_ONLY' for entry in rules)} "
                "lowering-only; "
                f"{sum(entry['current_status'] == 'BLOCKED' for entry in rules + physical)} "
                "blocked with reason codes)"
            )
            print(
                "PASS: Embedded bridge directly lowers the selected fragment "
                f"({direct_lowering_source_count} audited sources; "
                "no Daft logical optimizer entry point)"
            )
            print(
                "PASS: Catalog mutation negative CI "
                f"({negative_ci_count} invalid mutations rejected)"
            )
            print(f"PASS: Generated files match source (digest: {digest})")
            return 0

        # Generate mode
        generate_cpp_header(capabilities, digest, cpp_output)
        generate_rust_module(capabilities, digest, rust_output)
        print(f"Generated {cpp_output}")
        print(f"Generated {rust_output}")
        print(f"Capability digest: {digest}")
        return 0
    except (CapabilityRegistryError, OSError, yaml.YAMLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

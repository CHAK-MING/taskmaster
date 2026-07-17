#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import re


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
EXPORTER = REPOSITORY_ROOT / "src/dagforge/app/metrics_exporter.cpp"
RULES = REPOSITORY_ROOT / "docs/templates/prometheus-rules.yml"
DASHBOARD = REPOSITORY_ROOT / "docs/templates/grafana-dashboard.json"
PROBES = REPOSITORY_ROOT / "docs/templates/kubernetes-probes.yaml"

METRIC_NAME = re.compile(r'\.Name\("(dagforge_[a-zA-Z0-9_:]+)"\)')
METRIC_REFERENCE = re.compile(r"\b(dagforge_[a-zA-Z0-9_:]+)\b")
HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
FORBIDDEN_LABELS = {
    "run_id",
    "workflow_id",
    "plan_id",
    "node_id",
    "attempt_id",
    "artifact_id",
    "trace_id",
    "span_id",
    "principal",
    "error_message",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def normalize_metric(reference: str, exported: set[str]) -> str:
    if reference in exported:
        return reference
    for suffix in HISTOGRAM_SUFFIXES:
        if reference.endswith(suffix) and reference[: -len(suffix)] in exported:
            return reference[: -len(suffix)]
    return reference


def references_in(value: object) -> set[str]:
    return set(METRIC_REFERENCE.findall(json.dumps(value, ensure_ascii=False)))


def main() -> None:
    for path in (EXPORTER, RULES, DASHBOARD, PROBES):
        require(path.is_file(), f"missing observability asset: {path}")

    exporter_text = EXPORTER.read_text(encoding="utf-8")
    exported = set(METRIC_NAME.findall(exporter_text))
    require(len(exported) >= 20, "workflow observability metric surface is incomplete")

    for label in sorted(FORBIDDEN_LABELS):
        require(
            f'{{"{label}",' not in exporter_text,
            f"high-cardinality Prometheus label is forbidden: {label}",
        )

    rules_text = RULES.read_text(encoding="utf-8")
    require("groups:" in rules_text, "Prometheus rules have no groups")
    require(rules_text.count("- alert:") >= 8, "Prometheus rules are incomplete")
    rule_references = set(METRIC_REFERENCE.findall(rules_text))

    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    require(dashboard.get("title") == "DAGForge Observability", "unexpected dashboard title")
    require(len(dashboard.get("panels", [])) >= 10, "Grafana dashboard is incomplete")
    dashboard_references = references_in(dashboard)

    unknown = {
        reference
        for reference in rule_references | dashboard_references
        if normalize_metric(reference, exported) not in exported
    }
    require(not unknown, f"observability assets reference unknown metrics: {sorted(unknown)}")

    probes = PROBES.read_text(encoding="utf-8")
    require("/api/health" in probes, "Kubernetes liveness probe is missing")
    require("/api/ready" in probes, "Kubernetes readiness probe is missing")

    print(
        "observability asset check passed "
        f"({len(exported)} metrics, {rules_text.count('- alert:')} alerts, "
        f"{len(dashboard['panels'])} panels)"
    )


if __name__ == "__main__":
    main()

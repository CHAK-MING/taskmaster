#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import statistics
import subprocess
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
import urllib.error
import urllib.request
import uuid


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PLAN_TEMPLATE = REPOSITORY_ROOT / "dags/order_fulfillment.json"
TERMINAL_STATES = {"succeeded", "failed", "cancelled"}
FANOUT_NODES = (
    "calculate_price",
    "fraud_check",
    "inventory_preview",
    "shipping_quote",
)

ORDER = {
    "order_id": "ORD-20260717-10086",
    "customer_id": "CUS-2048",
    "currency": "CNY",
    "items": [
        {"sku": "PHONE-001", "quantity": 1, "unit_price": 4999},
        {"sku": "CASE-007", "quantity": 2, "unit_price": 99},
    ],
    "coupon": "SUMMER200",
    "shipping_address": {
        "province": "广东省",
        "city": "深圳市",
        "postal_code": "518000",
    },
}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="运行企业订单履约 DAG，验证中间输出、并行速度、补偿和 Repair Run。"
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--benchmark-runs", type=int, default=3)
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def default_binary() -> Path:
    candidates = (
        Path.home()
        / ".local/share/build2-configs/dagforge-gcc/dagforge/bin/dagforge",
        REPOSITORY_ROOT / "build/bin/dagforge",
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return candidates[0]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def percentile(values: list[float], quantile: float) -> float:
    require(values, "percentile requires at least one value")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * quantile) - 1))
    return ordered[index]


class OrderServiceState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.call_counts: Counter[str] = Counter()
        self.intervals: dict[str, list[tuple[float, float]]] = defaultdict(list)

    def record(self, endpoint: str, started: float, finished: float) -> None:
        with self.lock:
            self.call_counts[endpoint] += 1
            self.intervals[endpoint].append((started, finished))

    def counts(self) -> dict[str, int]:
        with self.lock:
            return dict(self.call_counts)


class OrderService(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port: int, state: OrderServiceState) -> None:
        super().__init__(("127.0.0.1", port), OrderServiceHandler)
        self.state = state


class OrderServiceHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    DELAYS_SEC = {
        "/validate-order": 0.12,
        "/calculate-price": 0.35,
        "/fraud-check": 0.45,
        "/inventory-preview": 0.40,
        "/shipping-quote": 0.20,
        "/make-decision": 0.08,
        "/authorize-payment": 0.10,
        "/reserve-inventory": 0.10,
        "/create-shipment": 0.12,
        "/confirm-order": 0.08,
        "/audit-package": 0.10,
        "/void-payment": 0.08,
        "/compensate-order": 0.05,
    }

    @property
    def order_service(self) -> OrderService:
        server = self.server
        if not isinstance(server, OrderService):
            raise RuntimeError("unexpected order service")
        return server

    def read_body(self) -> str:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length).decode("utf-8") if length else ""

    def send_payload(
        self,
        status: int,
        payload: str | dict[str, Any],
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        body = payload.encode("utf-8") if isinstance(payload, str) else compact_json(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        endpoint = parsed.path
        started = time.monotonic()
        try:
            delay = self.DELAYS_SEC.get(endpoint)
            if delay is None:
                self.send_payload(404, {"error": "unknown_endpoint"})
                return
            time.sleep(delay)
            body = self.read_body()
            query = parse_qs(parsed.query)
            self.handle_endpoint(endpoint, query, body)
        finally:
            self.order_service.state.record(endpoint, started, time.monotonic())

    def handle_endpoint(
        self, endpoint: str, query: dict[str, list[str]], body: str
    ) -> None:
        if endpoint == "/validate-order":
            order = json.loads(body)
            gross_amount = sum(
                item["quantity"] * item["unit_price"] for item in order["items"]
            )
            self.send_payload(
                200,
                {
                    "order_id": order["order_id"],
                    "customer_id": order["customer_id"],
                    "currency": order["currency"],
                    "item_count": sum(item["quantity"] for item in order["items"]),
                    "gross_amount": gross_amount,
                    "validation": "passed",
                },
            )
            return
        if endpoint == "/calculate-price":
            order = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": order["order_id"],
                    "subtotal": order["gross_amount"],
                    "discount": 200,
                    "shipping": 0,
                    "payable": order["gross_amount"] - 200,
                    "currency": order["currency"],
                },
            )
            return
        if endpoint == "/fraud-check":
            order = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": order["order_id"],
                    "risk_score": 18,
                    "decision": "allow",
                    "signals": ["known_device", "stable_address"],
                },
            )
            return
        if endpoint == "/inventory-preview":
            order = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": order["order_id"],
                    "available": True,
                    "warehouse": "SZX-01",
                    "reservation_ttl_sec": 300,
                },
            )
            return
        if endpoint == "/shipping-quote":
            mode = query.get("mode", ["ok"])[0]
            if mode == "system_failure":
                self.send_payload(503, {"error": "carrier_gateway_unavailable"})
                return
            order = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": order["order_id"],
                    "carrier": "SF",
                    "service": "next_day",
                    "fee": 0,
                    "estimated_delivery": "2026-07-19",
                },
            )
            return
        if endpoint == "/make-decision":
            price = json.loads(body)
            fraud = json.loads(self.headers["X-Fraud-Result"])
            inventory = json.loads(self.headers["X-Inventory-Result"])
            shipping = json.loads(self.headers["X-Shipping-Result"])
            decision = (
                "continue"
                if fraud["decision"] == "allow"
                and inventory["available"]
                and shipping["carrier"]
                and price["payable"] > 0
                else "reject"
            )
            self.send_payload(200, decision, "text/plain; charset=utf-8")
            return
        if endpoint == "/authorize-payment":
            self.send_payload(
                200,
                {
                    "order_id": body,
                    "authorization_id": "PAY-AUTH-91827",
                    "status": "authorized",
                    "amount": 4997,
                    "currency": "CNY",
                },
            )
            return
        if endpoint == "/reserve-inventory":
            mode = query.get("mode", ["ok"])[0]
            result = "reservation_failed" if mode == "business_failure" else "reserved"
            self.send_payload(200, result, "text/plain; charset=utf-8")
            return
        if endpoint == "/create-shipment":
            self.send_payload(
                200,
                {
                    "order_id": body,
                    "shipment_id": "SHIP-10001",
                    "warehouse": "SZX-01",
                    "carrier": "SF",
                    "status": "created",
                },
            )
            return
        if endpoint == "/confirm-order":
            shipment = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": shipment["order_id"],
                    "status": "confirmed",
                    "shipment_id": shipment["shipment_id"],
                    "payable": 4997,
                    "currency": "CNY",
                },
            )
            return
        if endpoint == "/audit-package":
            confirmation = json.loads(body)
            events = [
                {
                    "sequence": index,
                    "type": "fulfillment_trace",
                    "order_id": confirmation["order_id"],
                    "service": f"service-{index % 11}",
                    "status": "observed",
                    "message": "deterministic enterprise audit event",
                }
                for index in range(1800)
            ]
            self.send_payload(
                200,
                {
                    "order_id": confirmation["order_id"],
                    "status": "complete",
                    "event_count": len(events),
                    "events": events,
                },
            )
            return
        if endpoint == "/void-payment":
            self.send_payload(
                200,
                {
                    "order_id": body,
                    "authorization_id": "PAY-AUTH-91827",
                    "status": "voided",
                },
            )
            return
        if endpoint == "/compensate-order":
            payment = json.loads(body)
            self.send_payload(
                200,
                {
                    "order_id": payment["order_id"],
                    "status": "compensated",
                    "reason": "inventory_reservation_failed",
                    "payment_status": payment["status"],
                },
            )
            return
        self.send_payload(404, {"error": "unknown_endpoint"})

    def log_message(self, format: str, *args: Any) -> None:
        del format, args


class ManagedOrderService:
    def __init__(self, port: int, state: OrderServiceState) -> None:
        self.server = OrderService(port, state)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class DagforgeService:
    def __init__(self, binary: Path, config: Path, port: int) -> None:
        self.base_url = f"http://127.0.0.1:{port}"
        self.process = subprocess.Popen(
            [str(binary), "serve", str(config)],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ.copy(),
        )

    def wait_until_ready(self) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("DAGForge startup failed:\n" + self.read_output())
            try:
                status, _ = self.request_json("GET", "/api/health")
                if status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("DAGForge did not become ready")

    def request_json(
        self, method: str, path: str, body: Any | None = None
    ) -> tuple[int, Any]:
        payload = None if body is None else compact_json(body).encode("utf-8")
        request = urllib.request.Request(self.base_url + path, data=payload, method=method)
        if payload is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                data = response.read()
                return response.status, json.loads(data) if data else None
        except urllib.error.HTTPError as error:
            data = error.read()
            return error.code, json.loads(data) if data else None

    def request_bytes(self, path: str) -> tuple[int, bytes]:
        request = urllib.request.Request(self.base_url + path, method="GET")
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read()

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def read_output(self) -> str:
        if self.process.stdout is None:
            return ""
        return self.process.stdout.read()


def write_config(path: Path, root: Path, api_port: int, target_port: int) -> None:
    minijail_root = Path.home() / ".local/libexec/dagforge/minijail"
    config = {
        "workflow": {"enabled": True},
        "executors": {
            "command": {
                "policy": {
                    "allow_unlisted_programs": False,
                    "allow_unlisted_environment": False,
                    "require_trusted_programs": True,
                    "programs": [],
                    "allowed_programs": ["/bin/true"],
                    "allowed_environment": [],
                    "inherited_environment": ["LANG", "LC_ALL", "LC_CTYPE", "TERM"],
                },
                "minijail": {
                    "executable": str(minijail_root / "minijail0"),
                    "seccomp_bpf_path": str(minijail_root / "dagforge_command.bpf"),
                    "execution_root": str(root / "workspaces"),
                    "max_memory_bytes": 1073741824,
                    "max_file_bytes": 67108864,
                    "tmp_bytes": 67108864,
                    "max_stdout_bytes": 10485760,
                    "max_stderr_bytes": 10485760,
                    "max_stream_line_bytes": 65536,
                    "max_processes": 128,
                    "max_open_files": 256,
                    "require_trusted_files": True,
                    "retain_workdirs": False,
                },
            },
            "http": {
                "enabled": True,
                "egress": {
                    "allow_plaintext": True,
                    "deny_private_networks": True,
                    "allowed_origins": [f"http://127.0.0.1:{target_port}"],
                    "allowed_ip_cidrs": ["127.0.0.0/8"],
                    "max_request_headers": 64,
                    "max_request_header_bytes": 65536,
                    "max_request_body_bytes": 1048576,
                    "max_response_headers": 64,
                    "max_response_header_bytes": 65536,
                    "max_response_body_bytes": 1048576,
                    "max_concurrent_requests_per_shard": 16,
                    "max_concurrent_requests": 64,
                    "dns_timeout_ms": 1000,
                    "connect_timeout_ms": 1000,
                    "tls_handshake_timeout_ms": 1000,
                    "write_timeout_ms": 2000,
                    "first_byte_timeout_ms": 2000,
                    "read_timeout_ms": 2000,
                    "idle_connection_timeout_ms": 30000,
                    "max_idle_connections_per_origin": 8,
                    "max_idle_connections_per_shard": 16,
                    "tls_min_version": "1.2",
                    "tls_ca_file": "",
                    "tls_client_cert_file": "",
                    "tls_client_key_file": "",
                },
            },
        },
        "admission": {
            "allow_unlisted_executors": False,
            "allowed_executors": ["http"],
            "max_nodes": 64,
            "max_parallel_nodes": 16,
            "max_total_output_bytes": 16777216,
            "max_run_duration_sec": 120,
        },
        "storage": {
            "enabled": True,
            "directory": str(root / "state"),
            "max_completed_runs": 1000,
            "max_evidence_records": 10000,
            "max_plan_bytes": 8388608,
            "max_checkpoint_bytes": 67108864,
            "max_evidence_file_bytes": 268435456,
            "max_evidence_record_bytes": 1048576,
            "max_artifact_metadata_bytes": 1048576,
            "max_artifact_bytes": 268435456,
        },
        "runtime": {
            "shards": 4,
            "pin_shards_to_cores": False,
            "cpu_affinity_offset": 0,
        },
        "api": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": api_port,
            "reuse_port": False,
            "tls_enabled": False,
            "tls_cert_file": "",
            "tls_key_file": "",
            "tls_min_version": "1.2",
            "bearer_token_env": "",
            "max_request_header_bytes": 65536,
            "max_request_body_bytes": 1048576,
            "connection_idle_timeout_ms": 30000,
            "max_connections": 128,
            "max_requests_per_connection": 100,
            "max_concurrent_requests": 128,
        },
    }
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def replace_strings(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for source, target in replacements.items():
            value = value.replace(source, target)
        return value
    if isinstance(value, list):
        return [replace_strings(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: replace_strings(item, replacements) for key, item in value.items()}
    return value


def materialize_plan(
    target_port: int, shipping_mode: str = "ok", reserve_mode: str = "ok"
) -> dict[str, Any]:
    template = json.loads(PLAN_TEMPLATE.read_text(encoding="utf-8"))
    return replace_strings(
        template,
        {
            "__TARGET_PORT__": str(target_port),
            "__ORDER_PAYLOAD__": compact_json(ORDER),
            "__ORDER_ID__": ORDER["order_id"],
            "__SHIPPING_MODE__": shipping_mode,
            "__RESERVE_MODE__": reserve_mode,
        },
    )


def register_plan(service: DagforgeService, plan: dict[str, Any]) -> dict[str, Any]:
    status, response = service.request_json("POST", "/api/v1/workflows/plans", plan)
    require(status == 201, f"plan registration failed: {response}")
    return response


def start_run(
    service: DagforgeService,
    plan_id: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], float]:
    started_at = time.monotonic()
    status, response = service.request_json(
        "POST",
        "/api/v1/workflows/enterprise-order-fulfillment/runs",
        {
            "plan_id": plan_id,
            "source": "enterprise-demo",
            "event_type": "order_submitted",
            "payload": ORDER,
            "idempotency_key": idempotency_key,
            "principal": {"subject": "demo-operator", "roles": ["operator"]},
        },
    )
    require(status == 202, f"run start failed: {response}")
    return response, started_at


def wait_for_terminal(
    service: DagforgeService, run_id: str, started_at: float
) -> tuple[dict[str, Any], float]:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        status, snapshot = service.request_json(
            "GET", f"/api/v1/workflow-runs/{run_id}"
        )
        require(status == 200, f"snapshot failed: {snapshot}")
        if snapshot["state"] in TERMINAL_STATES:
            return snapshot, (time.monotonic() - started_at) * 1000.0
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} did not become terminal")


def find_task(snapshot: dict[str, Any], node_id: str) -> dict[str, Any]:
    for item in snapshot["tasks"]:
        if item["node_id"] == node_id:
            return item
    raise AssertionError(f"task {node_id} is missing")


def output_value(
    service: DagforgeService, run_id: str, node_id: str, port: str = "result"
) -> Any:
    status, response = service.request_json(
        "GET", f"/api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}"
    )
    require(status == 200, f"output {node_id}.{port} failed: {response}")
    return response["value"]


def json_output(service: DagforgeService, run_id: str, node_id: str) -> Any:
    value = output_value(service, run_id, node_id)
    require(isinstance(value, str), f"{node_id} did not return inline JSON")
    return json.loads(value)


def evidence_counts(service: DagforgeService, run_id: str) -> dict[str, int]:
    status, response = service.request_json(
        "GET", f"/api/v1/workflow-runs/{run_id}/evidence?offset=0&limit=1000"
    )
    require(status == 200, f"evidence query failed: {response}")
    return dict(Counter(item["type"] for item in response["evidence"]))


def failure_report(service: DagforgeService, run_id: str) -> dict[str, Any]:
    status, response = service.request_json(
        "GET", f"/api/v1/workflow-runs/{run_id}/failures"
    )
    require(status == 200, f"failure report query failed: {response}")
    return response


def task_interval_ms(task_snapshot: dict[str, Any]) -> tuple[float, float]:
    attempts = task_snapshot.get("attempts", [])
    require(attempts, f"task {task_snapshot['node_id']} has no attempts")
    attempt = attempts[-1]
    started = attempt.get("started_at_ms", attempt.get("created_at_ms"))
    finished = attempt.get("finished_at_ms")
    require(started is not None and finished is not None, f"missing attempt timestamps: {attempt}")
    return float(started), float(finished)


def fanout_metrics(snapshot: dict[str, Any]) -> dict[str, float]:
    intervals = [task_interval_ms(find_task(snapshot, node_id)) for node_id in FANOUT_NODES]
    sequential_ms = sum(finished - started for started, finished in intervals)
    wall_ms = max(finished for _, finished in intervals) - min(started for started, _ in intervals)
    require(wall_ms > 0, "fan-out wall time must be positive")
    speedup = sequential_ms / wall_ms
    require(speedup >= 2.0, f"fan-out did not run concurrently enough: {speedup:.2f}x")
    return {
        "sequential_ms": round(sequential_ms, 2),
        "wall_ms": round(wall_ms, 2),
        "speedup": round(speedup, 2),
    }


def inspect_success_run(
    service: DagforgeService,
    run_id: str,
    snapshot: dict[str, Any],
    include_fanout_metrics: bool = True,
) -> dict[str, Any]:
    require(snapshot["state"] == "succeeded", compact_json(snapshot))
    intermediate = {
        "validate_order": json_output(service, run_id, "validate_order"),
        "calculate_price": json_output(service, run_id, "calculate_price"),
        "fraud_check": json_output(service, run_id, "fraud_check"),
        "inventory_preview": json_output(service, run_id, "inventory_preview"),
        "shipping_quote": json_output(service, run_id, "shipping_quote"),
        "make_decision": output_value(service, run_id, "make_decision"),
        "authorize_payment": json_output(service, run_id, "authorize_payment"),
        "reserve_inventory": output_value(service, run_id, "reserve_inventory"),
        "create_shipment": json_output(service, run_id, "create_shipment"),
        "confirm_order": json_output(service, run_id, "confirm_order"),
    }
    require(intermediate["validate_order"]["validation"] == "passed", compact_json(intermediate))
    require(intermediate["calculate_price"]["payable"] == 4997, compact_json(intermediate))
    require(intermediate["fraud_check"]["decision"] == "allow", compact_json(intermediate))
    require(intermediate["inventory_preview"]["available"], compact_json(intermediate))
    require(intermediate["make_decision"] == "continue", compact_json(intermediate))
    require(intermediate["reserve_inventory"] == "reserved", compact_json(intermediate))
    require(intermediate["confirm_order"]["status"] == "confirmed", compact_json(intermediate))

    artifact = output_value(service, run_id, "build_audit_package")
    require(isinstance(artifact, dict) and artifact.get("type") == "artifact", compact_json(artifact))
    status, content = service.request_bytes(
        f"/api/v1/artifacts/{artifact['artifact_id']}"
    )
    require(status == 200, "audit Artifact download failed")
    audit = json.loads(content)
    require(audit["event_count"] == 1800, compact_json(audit))
    require(audit["order_id"] == ORDER["order_id"], compact_json(audit))
    result = {
        "intermediate_outputs": intermediate,
        "audit_artifact": {
            "artifact_id": artifact["artifact_id"],
            "size_bytes": artifact["size_bytes"],
            "digest": artifact["digest"],
            "event_count": audit["event_count"],
        },
        "evidence_counts": evidence_counts(service, run_id),
    }
    if include_fanout_metrics:
        result["fanout"] = fanout_metrics(snapshot)
    return result


def run_success_benchmark(
    service: DagforgeService, plan_id: str, runs: int
) -> tuple[dict[str, Any], float]:
    require(runs > 0, "benchmark-runs must be positive")
    durations: list[float] = []
    inspection: dict[str, Any] | None = None
    for index in range(runs):
        started, started_at = start_run(
            service, plan_id, f"success-benchmark-{index}-{uuid.uuid4().hex}"
        )
        snapshot, duration_ms = wait_for_terminal(service, started["run_id"], started_at)
        durations.append(duration_ms)
        if inspection is None:
            inspection = inspect_success_run(service, started["run_id"], snapshot)
    require(inspection is not None, "success benchmark produced no run")
    median_ms = statistics.median(durations)
    return {
        "runs": runs,
        "durations_ms": [round(value, 2) for value in durations],
        "median_ms": round(median_ms, 2),
        "p95_ms": round(percentile(durations, 0.95), 2),
        **inspection,
    }, median_ms


def run_business_compensation(
    service: DagforgeService, plan_id: str
) -> dict[str, Any]:
    started, started_at = start_run(
        service, plan_id, f"business-compensation-{uuid.uuid4().hex}"
    )
    snapshot, duration_ms = wait_for_terminal(service, started["run_id"], started_at)
    require(snapshot["state"] == "succeeded", compact_json(snapshot))
    require(output_value(service, started["run_id"], "reserve_inventory") == "reservation_failed", compact_json(snapshot))
    voided = json_output(service, started["run_id"], "void_payment")
    compensated = json_output(service, started["run_id"], "compensate_order")
    require(voided["status"] == "voided", compact_json(voided))
    require(compensated["status"] == "compensated", compact_json(compensated))
    require(find_task(snapshot, "create_shipment")["state"] == "skipped", compact_json(snapshot))
    require(find_task(snapshot, "confirm_order")["state"] == "skipped", compact_json(snapshot))
    return {
        "run_id": started["run_id"],
        "duration_ms": round(duration_ms, 2),
        "reserve_result": "reservation_failed",
        "void_payment": voided,
        "final_output": compensated,
        "skipped_nodes": ["create_shipment", "confirm_order", "build_audit_package"],
        "evidence_counts": evidence_counts(service, started["run_id"]),
    }


def run_failure_and_repair(
    service: DagforgeService,
    failed_plan: dict[str, Any],
    repaired_plan: dict[str, Any],
    state: OrderServiceState,
    baseline_median_ms: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    failed_registration = register_plan(service, failed_plan)
    started, started_at = start_run(
        service,
        failed_registration["plan_id"],
        f"system-failure-{uuid.uuid4().hex}",
    )
    parent, duration_ms = wait_for_terminal(service, started["run_id"], started_at)
    require(parent["state"] == "failed", compact_json(parent))
    shipping_task = find_task(parent, "shipping_quote")
    require(shipping_task["state"] == "failed", compact_json(parent))
    require(shipping_task["attempt_count"] == 2, compact_json(shipping_task))
    require(find_task(parent, "make_decision")["state"] == "skipped", compact_json(parent))

    retained_outputs = {
        "calculate_price": json_output(service, started["run_id"], "calculate_price"),
        "fraud_check": json_output(service, started["run_id"], "fraud_check"),
        "inventory_preview": json_output(service, started["run_id"], "inventory_preview"),
    }
    report = failure_report(service, started["run_id"])
    failure_counts = evidence_counts(service, started["run_id"])
    require(failure_counts.get("task_failed", 0) >= 1, compact_json(failure_counts))

    counts_before_repair = state.counts()
    repair_started_at = time.monotonic()
    status, repair_response = service.request_json(
        "POST",
        f"/api/v1/workflow-runs/{started['run_id']}/repairs",
        {
            "reason": "物流网关恢复，修复 shipping_quote 并复用有效中间结果",
            "idempotency_key": f"repair-{uuid.uuid4().hex}",
            "plan": repaired_plan,
        },
    )
    require(status == 202, f"repair start failed: {repair_response}")
    repaired, repair_duration_ms = wait_for_terminal(
        service, repair_response["run_id"], repair_started_at
    )
    require(repaired["state"] == "succeeded", compact_json(repaired))

    reuse_by_node = {
        item["node_id"]: {
            "reused": item["reused"],
            "reason": item["reason"],
        }
        for item in repair_response["nodes"]
    }
    for node_id in (
        "validate_order",
        "calculate_price",
        "fraud_check",
        "inventory_preview",
    ):
        require(reuse_by_node[node_id]["reused"], compact_json(reuse_by_node))
        task_snapshot = find_task(repaired, node_id)
        require(task_snapshot["attempt_count"] == 0, compact_json(task_snapshot))
        require(task_snapshot["reused_from_run_id"] == started["run_id"], compact_json(task_snapshot))
    require(not reuse_by_node["shipping_quote"]["reused"], compact_json(reuse_by_node))
    require(not reuse_by_node["make_decision"]["reused"], compact_json(reuse_by_node))

    counts_after_repair = state.counts()
    call_count_delta = {
        endpoint: counts_after_repair.get(endpoint, 0) - counts_before_repair.get(endpoint, 0)
        for endpoint in sorted(set(counts_before_repair) | set(counts_after_repair))
    }
    for endpoint in (
        "/validate-order",
        "/calculate-price",
        "/fraud-check",
        "/inventory-preview",
    ):
        require(call_count_delta.get(endpoint, 0) == 0, compact_json(call_count_delta))
    require(call_count_delta.get("/shipping-quote", 0) == 1, compact_json(call_count_delta))
    require(repair_duration_ms < baseline_median_ms, f"repair {repair_duration_ms:.2f}ms was not faster than full run {baseline_median_ms:.2f}ms")

    repaired_output = inspect_success_run(
        service,
        repair_response["run_id"],
        repaired,
        include_fanout_metrics=False,
    )
    return (
        {
            "run_id": started["run_id"],
            "duration_ms": round(duration_ms, 2),
            "failed_node": "shipping_quote",
            "attempt_count": shipping_task["attempt_count"],
            "failure": report["failure"],
            "retained_intermediate_outputs": retained_outputs,
            "evidence_counts": failure_counts,
        },
        {
            "run_id": repair_response["run_id"],
            "parent_run_id": started["run_id"],
            "duration_ms": round(repair_duration_ms, 2),
            "faster_than_full_run": True,
            "reuse_decisions": reuse_by_node,
            "call_count_delta": call_count_delta,
            "final_output": repaired_output["intermediate_outputs"]["confirm_order"],
            "audit_artifact": repaired_output["audit_artifact"],
            "evidence_counts": repaired_output["evidence_counts"],
        },
    )


def run_demo(binary: Path, benchmark_runs: int) -> dict[str, Any]:
    binary = binary.expanduser().resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), f"DAGForge binary is unavailable: {binary}")
    minijail = Path.home() / ".local/libexec/dagforge/minijail/minijail0"
    require(minijail.is_file() and os.access(minijail, os.X_OK), f"Minijail is unavailable: {minijail}")

    root = Path.home() / ".cache/dagforge-order-demo" / uuid.uuid4().hex
    root.mkdir(parents=True)
    api_port = reserve_port()
    target_port = reserve_port()
    config_path = root / "system_config.json"
    write_config(config_path, root, api_port, target_port)

    state = OrderServiceState()
    order_service = ManagedOrderService(target_port, state)
    dagforge = DagforgeService(binary, config_path, api_port)
    order_service.start()
    try:
        dagforge.wait_until_ready()
        healthy_plan = materialize_plan(target_port)
        healthy_registration = register_plan(dagforge, healthy_plan)
        success, baseline_median_ms = run_success_benchmark(
            dagforge, healthy_registration["plan_id"], benchmark_runs
        )

        compensation_plan = materialize_plan(target_port, reserve_mode="business_failure")
        compensation_registration = register_plan(dagforge, compensation_plan)
        compensation = run_business_compensation(
            dagforge, compensation_registration["plan_id"]
        )

        failed_plan = materialize_plan(target_port, shipping_mode="system_failure")
        failure, repair = run_failure_and_repair(
            dagforge,
            failed_plan,
            healthy_plan,
            state,
            baseline_median_ms,
        )
        return {
            "workflow": "enterprise-order-fulfillment",
            "order_id": ORDER["order_id"],
            "success": success,
            "business_compensation": compensation,
            "system_failure": failure,
            "repair": repair,
            "mock_service_call_counts": state.counts(),
        }
    finally:
        dagforge.stop()
        order_service.stop()
        shutil.rmtree(root, ignore_errors=True)


def print_summary(report: dict[str, Any]) -> None:
    success = report["success"]
    compensation = report["business_compensation"]
    failure = report["system_failure"]
    repair = report["repair"]
    print("企业订单履约 DAG 验证通过")
    print(
        f"完整成功运行：{success['runs']} 次，median={success['median_ms']}ms，p95={success['p95_ms']}ms"
    )
    print(
        f"并行 fan-out：串行工作量={success['fanout']['sequential_ms']}ms，实际墙钟={success['fanout']['wall_ms']}ms，speedup={success['fanout']['speedup']}x"
    )
    print(
        f"中间产物：10 个节点输出已核验，审计 Artifact={success['audit_artifact']['size_bytes']} bytes / {success['audit_artifact']['event_count']} events"
    )
    print(
        f"业务补偿：reserve={compensation['reserve_result']}，final={compensation['final_output']['status']}"
    )
    print(
        f"系统失败：node={failure['failed_node']}，attempts={failure['attempt_count']}，failure={failure['failure']['code']}"
    )
    reused = [node for node, decision in repair["reuse_decisions"].items() if decision["reused"]]
    print(
        f"Repair Run：duration={repair['duration_ms']}ms，复用={','.join(reused)}，最终={repair['final_output']['status']}"
    )


def main() -> None:
    arguments = parse_arguments()
    report = run_demo(arguments.binary or default_binary(), arguments.benchmark_runs)
    print_summary(report)
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        arguments.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"报告：{arguments.report}")


if __name__ == "__main__":
    main()

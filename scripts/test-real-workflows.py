#!/usr/bin/env python3

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.request
import uuid


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run real Workflow JSON files through DAGForge and Minijail."
    )
    parser.add_argument(
        "--binary",
        type=Path,
        required=True,
        help="Path to the built dagforge executable.",
    )
    return parser.parse_args()


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def generate_tls_certificate(state_root: Path) -> tuple[Path, Path]:
    certificate = state_root / "localhost.crt"
    private_key = state_root / "localhost.key"
    result = subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(private_key),
            "-out",
            str(certificate),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    require(
        result.returncode == 0,
        f"failed to generate TLS certificate:\n{result.stdout}{result.stderr}",
    )
    return certificate, private_key


def write_config(
    path: Path,
    state_root: Path,
    port: int,
    http_target_port: int,
    https_target_port: int,
    tls_silent_port: int,
    api_tls_certificate: Path | None = None,
    api_tls_key: Path | None = None,
) -> None:
    minijail_root = Path.home() / ".local/libexec/dagforge/minijail"
    api_tls_enabled = api_tls_certificate is not None and api_tls_key is not None
    config = {
        "workflow": {"enabled": True},
        "executors": {
            "command": {
                "policy": {
                    "allow_unlisted_programs": False,
                    "allow_unlisted_environment": False,
                    "require_trusted_programs": True,
                    "programs": [],
                    "allowed_programs": [
                        "/bin/echo",
                        "/bin/printf",
                        "/bin/sh",
                        "/bin/cat",
                        "/bin/true",
                        "/usr/bin/python3",
                    ],
                    "allowed_environment": [
                        "CUSTOM_VALUE",
                        "DAGFORGE_INPUT",
                        "FINAL_VALUE",
                        "HTTP_RESPONSE",
                        "LEFT_VALUE",
                        "RIGHT_VALUE",
                        "ROOT_VALUE",
                    ],
                    "inherited_environment": ["LANG", "LC_ALL", "LC_CTYPE", "TERM"],
                },
                "minijail": {
                    "executable": str(minijail_root / "minijail0"),
                    "seccomp_bpf_path": str(minijail_root / "dagforge_command.bpf"),
                    "execution_root": str(state_root / "workspaces"),
                    "max_memory_bytes": 1073741824,
                    "max_file_bytes": 67108864,
                    "tmp_bytes": 67108864,
                    "max_stdout_bytes": 10485760,
                    "max_stderr_bytes": 10485760,
                    "max_stream_line_bytes": 1048576,
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
                    "allowed_origins": [
                        f"http://127.0.0.1:{http_target_port}",
                        f"https://localhost:{https_target_port}",
                        f"https://localhost:{tls_silent_port}",
                    ],
                    "allowed_ip_cidrs": ["127.0.0.0/8", "::1/128"],
                    "max_request_headers": 64,
                    "max_request_header_bytes": 65536,
                    "max_request_body_bytes": 1024,
                    "max_response_headers": 64,
                    "max_response_header_bytes": 4096,
                    "max_response_body_bytes": 4096,
                    "max_concurrent_requests_per_shard": 1,
                    "max_concurrent_requests": 2,
                    "dns_timeout_ms": 1000,
                    "connect_timeout_ms": 1000,
                    "tls_handshake_timeout_ms": 1000,
                    "write_timeout_ms": 1000,
                    "first_byte_timeout_ms": 500,
                    "read_timeout_ms": 1000,
                    "idle_connection_timeout_ms": 30000,
                    "max_idle_connections_per_origin": 2,
                    "max_idle_connections_per_shard": 4,
                    "tls_min_version": "1.2",
                    "tls_ca_file": "",
                    "tls_client_cert_file": "",
                    "tls_client_key_file": "",
                },
            },
        },
        "admission": {
            "allow_unlisted_executors": False,
            "allowed_executors": ["command", "http", "transform"],
            "max_nodes": 256,
            "max_parallel_nodes": 32,
            "max_total_output_bytes": 67108864,
            "max_run_duration_sec": 3600,
        },
        "storage": {
            "enabled": False,
            "directory": str(state_root / "state"),
            "max_completed_runs": 10000,
            "max_evidence_records": 100000,
        },
        "runtime": {
            "shards": 2,
            "pin_shards_to_cores": False,
            "cpu_affinity_offset": 0,
        },
        "api": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": port,
            "reuse_port": False,
            "tls_enabled": api_tls_enabled,
            "tls_cert_file": str(api_tls_certificate or ""),
            "tls_key_file": str(api_tls_key or ""),
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
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


class DagforgeService:
    def __init__(
        self,
        binary: Path,
        config: Path,
        port: int,
        environment: dict[str, str],
        tls_ca: Path | None = None,
    ) -> None:
        scheme = "https" if tls_ca is not None else "http"
        host = "localhost" if tls_ca is not None else "127.0.0.1"
        self._base_url = f"{scheme}://{host}:{port}"
        self._ssl_context = (
            ssl.create_default_context(cafile=str(tls_ca))
            if tls_ca is not None
            else None
        )
        self._process = subprocess.Popen(
            [str(binary), "serve", str(config)],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=environment,
        )

    def wait_until_ready(self) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"dagforge exited during startup:\n{self._read_output()}"
                )
            try:
                status, _ = self.request_json("GET", "/api/health")
                if status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("dagforge did not become ready")

    def request_json(
        self, method: str, path: str, body: Any | None = None
    ) -> tuple[int, Any]:
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self._base_url + path, data=payload, method=method
        )
        if payload is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(
                request, timeout=5, context=self._ssl_context
            ) as response:
                data = response.read()
                return response.status, json.loads(data) if data else None
        except urllib.error.HTTPError as error:
            data = error.read()
            return error.code, json.loads(data) if data else None

    def request_bytes(self, path: str) -> tuple[int, bytes]:
        request = urllib.request.Request(self._base_url + path, method="GET")
        with urllib.request.urlopen(
            request, timeout=5, context=self._ssl_context
        ) as response:
            return response.status, response.read()

    def stop(self, require_graceful: bool = False) -> None:
        if self._process.poll() is not None:
            return
        self._process.send_signal(signal.SIGTERM)
        timeout = float(os.environ.get("DAGFORGE_E2E_STOP_TIMEOUT", "5"))
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
            if require_graceful:
                raise AssertionError(
                    "dagforge did not stop gracefully:\n" + self._read_output()
                )

    def _read_output(self) -> str:
        if self._process.stdout is None:
            return ""
        return self._process.stdout.read()


class HttpTargetState:
    def __init__(self) -> None:
        self.retry_count = 0
        self.retry_connection_ports: set[int] = set()
        self.lock = threading.Lock()
        self.cancel_started = threading.Event()


class StatefulThreadingHttpServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state: HttpTargetState) -> None:
        super().__init__(address, HttpTargetHandler)
        self.state = state

    def handle_error(
        self, request: socket.socket, client_address: tuple[str, int]
    ) -> None:
        error = sys.exc_info()[1]
        if isinstance(error, (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


class HttpTargetHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def target_state(self) -> HttpTargetState:
        server = self.server
        if not isinstance(server, StatefulThreadingHttpServer):
            raise RuntimeError("unexpected HTTP target server")
        return server.state

    def send_payload(
        self,
        status: int,
        body: bytes,
        content_type: str = "text/plain; charset=utf-8",
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        for name, value in headers or []:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_POST(self) -> None:
        if self.path != "/transform?mode=test":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(length).decode("utf-8")
        token = self.headers.get("X-Token", "missing")
        response_body = f"http:{request_body}|token:{token}".encode("utf-8")
        self.send_payload(
            201,
            response_body,
            headers=[("X-Target", "loopback"), ("X-Duplicate", "first"),
                     ("X-Duplicate", "second")],
        )

    def do_GET(self) -> None:
        if self.path == "/not-found":
            self.send_payload(404, b"missing")
            return
        if self.path == "/retry":
            with self.target_state.lock:
                self.target_state.retry_count += 1
                self.target_state.retry_connection_ports.add(self.client_address[1])
                attempt = self.target_state.retry_count
            if attempt < 3:
                self.send_payload(503, b"retry")
            else:
                self.send_payload(200, b"recovered")
            return
        if self.path == "/large":
            self.send_payload(200, b"x" * 8192)
            return
        if self.path == "/large-header":
            self.send_payload(200, b"ok", headers=[("X-Large", "x" * 8192)])
            return
        if self.path == "/slow":
            time.sleep(2)
            self.send_payload(200, b"late")
            return
        if self.path == "/cancel":
            self.target_state.cancel_started.set()
            time.sleep(10)
            self.send_payload(200, b"cancelled-too-late")
            return
        if self.path == "/invalid-utf8":
            self.send_payload(200, b"\xff\xfe", "application/octet-stream")
            return
        if self.path == "/invalid-header-utf8":
            self.send_payload(200, b"ok", headers=[("X-Binary", "\xff")])
            return
        if self.path == "/forbidden":
            self.send_payload(403, b"forbidden")
            return
        if self.path == "/hold":
            time.sleep(0.4)
            self.send_payload(200, b"held")
            return
        if self.path == "/tls":
            self.send_payload(200, b"secure")
            return
        self.send_payload(404, b"unknown")

    def log_message(self, format: str, *args: Any) -> None:
        del format, args


class HttpTargetService:
    def __init__(
        self,
        port: int,
        state: HttpTargetState,
        tls_certificate: Path | None = None,
        tls_key: Path | None = None,
    ) -> None:
        self._server = StatefulThreadingHttpServer(("127.0.0.1", port), state)
        if tls_certificate is not None and tls_key is not None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(tls_certificate, tls_key)
            self._server.socket = context.wrap_socket(
                self._server.socket, server_side=True
            )
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class SilentTcpService:
    def __init__(self, port: int) -> None:
        self.accepted = threading.Event()
        self._stop = threading.Event()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", port))
        self._listener.listen(4)
        self._listener.settimeout(0.1)
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self) -> None:
        connections: list[socket.socket] = []
        try:
            while not self._stop.is_set():
                try:
                    connection, _ = self._listener.accept()
                except (TimeoutError, OSError):
                    continue
                connections.append(connection)
                self.accepted.set()
        finally:
            for connection in connections:
                connection.close()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join(timeout=5)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def replace_strings(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for source, target in replacements.items():
            value = value.replace(source, target)
        return value
    if isinstance(value, list):
        return [replace_strings(item, replacements) for item in value]
    if isinstance(value, dict):
        return {
            key: replace_strings(item, replacements) for key, item in value.items()
        }
    return value


def load_plan(
    name: str, replacements: dict[str, str] | None = None
) -> dict[str, Any]:
    path = REPOSITORY_ROOT / "dags" / name
    plan = json.loads(path.read_text(encoding="utf-8"))
    return replace_strings(plan, replacements or {})


def validate_plan(binary: Path, name: str) -> None:
    path = REPOSITORY_ROOT / "dags" / name
    result = subprocess.run(
        [str(binary), "validate", str(path)],
        cwd=REPOSITORY_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    require(
        result.returncode == 0,
        f"validation failed for {name}:\n{result.stdout}{result.stderr}",
    )


def validate_materialized_plan(
    binary: Path,
    config: Path,
    state_root: Path,
    name: str,
    replacements: dict[str, str],
) -> None:
    materialized = state_root / f"validated-{name}"
    materialized.write_text(
        json.dumps(load_plan(name, replacements), indent=2), encoding="utf-8"
    )
    result = subprocess.run(
        [
            str(binary),
            "validate",
            str(materialized),
            "--config",
            str(config),
        ],
        cwd=REPOSITORY_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    require(
        result.returncode == 0,
        f"policy-aware validation failed for {name}:\n"
        f"{result.stdout}{result.stderr}",
    )


def register_plan(
    service: DagforgeService,
    name: str,
    replacements: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan = load_plan(name, replacements)
    status, registered = service.request_json(
        "POST", "/api/v1/workflows/plans", plan
    )
    require(status == 201, f"registration failed for {name}: {registered}")
    return plan, registered


def run_plan(
    service: DagforgeService,
    name: str,
    replacements: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan, registered = register_plan(service, name, replacements)

    workflow_id = plan["workflow_id"]
    status, started = service.request_json(
        "POST",
        f"/api/v1/workflows/{workflow_id}/runs",
        {"plan_id": registered["plan_id"]},
    )
    require(status == 202, f"start failed for {name}: {started}")

    run_id = started["run_id"]
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        status, snapshot = service.request_json(
            "GET", f"/api/v1/workflow-runs/{run_id}"
        )
        require(status == 200, f"snapshot failed for {name}: {snapshot}")
        if snapshot["state"] in {"succeeded", "failed", "cancelled"}:
            return started, snapshot
        time.sleep(0.02)
    raise AssertionError(f"run timed out for {name}")


def wait_for_terminal(
    service: DagforgeService, run_id: str, name: str
) -> dict[str, Any]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        status, snapshot = service.request_json(
            "GET", f"/api/v1/workflow-runs/{run_id}"
        )
        require(status == 200, f"snapshot failed for {name}: {snapshot}")
        if snapshot["state"] in {"succeeded", "failed", "cancelled"}:
            return snapshot
        time.sleep(0.02)
    raise AssertionError(f"run timed out for {name}")


def task(snapshot: dict[str, Any], node_id: str) -> dict[str, Any]:
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


def verify_hello_world(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "hello_world.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "start")
        == "hello from DAGForge\n",
        "hello-world output mismatch",
    )


def verify_dataflow(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "dataflow.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "publish")
        == "published:received:hello",
        "dataflow output mismatch",
    )


def verify_sanitized_environment(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "sanitized_environment.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "inspect")
        == "visible|/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin|/tmp|unset",
        "sandbox environment was not sanitized",
    )
    require(
        output_value(service, started["run_id"], "inspect", "exit_code") == 0,
        "exit_code was not returned as an integer",
    )


def verify_fanout_fanin(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "fanout_fanin.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(task(snapshot, "left")["state"] == "succeeded", str(snapshot))
    require(task(snapshot, "right")["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "join")
        == "left:seed|right:seed",
        "fanout/fanin output mismatch",
    )


def verify_conditional(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "conditional.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(task(snapshot, "selected")["state"] == "succeeded", str(snapshot))
    require(task(snapshot, "rejected")["state"] == "skipped", str(snapshot))
    require(
        output_value(service, started["run_id"], "selected") == "selected",
        "conditional output mismatch",
    )


def verify_retry_failure(service: DagforgeService) -> None:
    _, snapshot = run_plan(service, "retry_failure.json")
    unstable = task(snapshot, "unstable")
    require(snapshot["state"] == "failed", str(snapshot))
    require(unstable["state"] == "failed", str(snapshot))
    require(unstable["attempt_count"] == 3, str(snapshot))
    require(len(unstable["attempts"]) == 3, str(snapshot))


def verify_fail_fast(service: DagforgeService) -> None:
    _, snapshot = run_plan(service, "fail_fast.json")
    require(snapshot["state"] == "failed", str(snapshot))
    require(task(snapshot, "fail")["state"] == "failed", str(snapshot))
    require(task(snapshot, "slow")["state"] == "cancelled", str(snapshot))


def verify_large_artifact(service: DagforgeService) -> None:
    started, snapshot = run_plan(service, "large_artifact.json")
    require(snapshot["state"] == "succeeded", str(snapshot))
    artifact = output_value(service, started["run_id"], "generate")
    require(artifact["type"] == "artifact", str(artifact))
    require(artifact["size_bytes"] == 300000, str(artifact))
    status, content = service.request_bytes(
        f"/api/v1/artifacts/{artifact['artifact_id']}"
    )
    require(status == 200, "artifact download failed")
    require(len(content) == 300000, f"artifact size was {len(content)}")
    require(content == b"x" * 300000, "artifact content mismatch")


def verify_missing_published_output(service: DagforgeService) -> None:
    _, snapshot = run_plan(service, "missing_published_output.json")
    require(snapshot["state"] == "failed", str(snapshot))
    require(task(snapshot, "publish")["state"] == "skipped", str(snapshot))
    require(snapshot["failure"]["kind"] == "incomplete", str(snapshot))
    require(snapshot["failure"]["code"] == "required_output_missing", str(snapshot))
    require(snapshot["failure"]["details"]["node_id"] == "publish", str(snapshot))
    require(snapshot["failure"]["details"]["port"] == "result", str(snapshot))


def verify_http_pipeline(service: DagforgeService, target_port: int) -> None:
    started, snapshot = run_plan(
        service,
        "http_pipeline.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "request", "status") == 201,
        "HTTP status output mismatch",
    )
    require(
        output_value(service, started["run_id"], "request", "body")
        == "http:hello|token:secret",
        "HTTP body output mismatch",
    )
    headers = output_value(service, started["run_id"], "request", "headers")
    duplicate_values = [
        item["value"] for item in headers if item["name"].lower() == "x-duplicate"
    ]
    require(
        duplicate_values == ["first", "second"],
        f"duplicate HTTP response headers were not preserved: {headers}",
    )
    require(
        output_value(service, started["run_id"], "publish")
        == "published:http:hello|token:secret",
        "mixed executor output mismatch",
    )


def verify_http_accepted_status(service: DagforgeService, target_port: int) -> None:
    started, snapshot = run_plan(
        service,
        "http_accepted_status.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "request", "status") == 404,
        "accepted HTTP status output mismatch",
    )
    require(
        output_value(service, started["run_id"], "request") == "missing",
        "accepted HTTP response body mismatch",
    )


def verify_http_retry(
    service: DagforgeService, target_port: int, target_state: HttpTargetState
) -> None:
    started, snapshot = run_plan(
        service,
        "http_retry.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_task = task(snapshot, "request")
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(request_task["attempt_count"] == 3, str(snapshot))
    require(len(request_task["attempts"]) == 3, str(snapshot))
    require(
        output_value(service, started["run_id"], "request") == "recovered",
        "HTTP retry output mismatch",
    )
    with target_state.lock:
        connection_count = len(target_state.retry_connection_ports)
    require(
        connection_count == 1,
        f"HTTP retries used {connection_count} TCP connections instead of one",
    )


def verify_http_response_limit(service: DagforgeService, target_port: int) -> None:
    _, snapshot = run_plan(
        service,
        "http_response_limit.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    require(snapshot["state"] == "failed", str(snapshot))
    require(task(snapshot, "request")["state"] == "failed", str(snapshot))


def verify_http_request_body_limit(
    service: DagforgeService, target_port: int
) -> None:
    _, snapshot = run_plan(
        service,
        "http_request_body_limit.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_task = task(snapshot, "request")
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_task["failure"]["kind"] == "resource_exhausted", str(snapshot))
    require(request_task["failure"]["code"] == "executor_start_failed", str(snapshot))


def verify_http_response_header_limit(
    service: DagforgeService, target_port: int
) -> None:
    _, snapshot = run_plan(
        service,
        "http_response_header_limit.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    require(snapshot["state"] == "failed", str(snapshot))
    require(task(snapshot, "request")["state"] == "failed", str(snapshot))


def verify_http_timeout(service: DagforgeService, target_port: int) -> None:
    _, snapshot = run_plan(
        service,
        "http_timeout.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_task = task(snapshot, "request")
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_task["attempts"][0]["state"] == "timed_out", str(snapshot))


def verify_http_invalid_utf8(service: DagforgeService, target_port: int) -> None:
    _, snapshot = run_plan(
        service,
        "http_invalid_utf8.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_attempt = task(snapshot, "request")["attempts"][0]
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_attempt["state"] == "failed", str(snapshot))
    require(request_attempt["failure"]["kind"] == "protocol_error", str(snapshot))
    require(request_attempt["failure"]["code"] == "http_invalid_response", str(snapshot))


def verify_http_invalid_header_utf8(
    service: DagforgeService, target_port: int
) -> None:
    _, snapshot = run_plan(
        service,
        "http_invalid_header_utf8.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_attempt = task(snapshot, "request")["attempts"][0]
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_attempt["state"] == "failed", str(snapshot))
    require(request_attempt["failure"]["kind"] == "protocol_error", str(snapshot))
    require(request_attempt["failure"]["code"] == "http_invalid_response", str(snapshot))


def verify_http_forbidden(service: DagforgeService, target_port: int) -> None:
    _, snapshot = run_plan(
        service,
        "http_forbidden.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_task = task(snapshot, "request")
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_task["attempt_count"] == 1, str(snapshot))
    request_attempt = request_task["attempts"][0]
    require(request_attempt["state"] == "failed", str(snapshot))
    require(request_attempt["failure"]["kind"] == "unauthorized", str(snapshot))
    require(request_attempt["failure"]["code"] == "http_status_rejected", str(snapshot))


def verify_http_concurrency_limit(
    service: DagforgeService, target_port: int
) -> None:
    _, snapshot = run_plan(
        service,
        "http_concurrency_limit.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    states = {task(snapshot, "first")["state"], task(snapshot, "second")["state"]}
    require(snapshot["state"] == "failed", str(snapshot))
    require(states == {"succeeded", "failed"}, str(snapshot))
    failed = next(item for item in snapshot["tasks"] if item["state"] == "failed")
    require(failed["failure"]["kind"] == "queue_full", str(snapshot))
    require(failed["failure"]["code"] == "executor_start_failed", str(snapshot))


def verify_http_header_limit(service: DagforgeService, target_port: int) -> None:
    _, snapshot = run_plan(
        service,
        "http_header_limit.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    request_task = task(snapshot, "request")
    require(snapshot["state"] == "failed", str(snapshot))
    require(request_task["attempt_count"] == 1, str(snapshot))
    require(request_task["failure"]["kind"] == "resource_exhausted", str(snapshot))
    require(request_task["failure"]["code"] == "executor_start_failed", str(snapshot))


def verify_http_tls(service: DagforgeService, target_port: int) -> None:
    started, snapshot = run_plan(
        service,
        "http_tls.json",
        {"__HTTPS_TARGET_PORT__": str(target_port)},
    )
    require(snapshot["state"] == "succeeded", str(snapshot))
    require(
        output_value(service, started["run_id"], "request") == "secure",
        "TLS HTTP output mismatch",
    )


def verify_http_cancel(
    service: DagforgeService, target_port: int, target_state: HttpTargetState
) -> None:
    target_state.cancel_started.clear()
    plan, registered = register_plan(
        service,
        "http_cancel.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    status, started = service.request_json(
        "POST",
        f"/api/v1/workflows/{plan['workflow_id']}/runs",
        {"plan_id": registered["plan_id"]},
    )
    require(status == 202, f"HTTP cancel run start failed: {started}")
    require(target_state.cancel_started.wait(timeout=5), "HTTP request did not start")
    status, response = service.request_json(
        "POST", f"/api/v1/workflow-runs/{started['run_id']}/cancel"
    )
    require(status == 202, f"HTTP cancel request failed: {response}")
    snapshot = wait_for_terminal(service, started["run_id"], "http_cancel.json")
    require(snapshot["state"] == "cancelled", str(snapshot))
    require(task(snapshot, "request")["state"] == "cancelled", str(snapshot))


def verify_http_tls_handshake_cancel(
    service: DagforgeService, silent_port: int, silent: SilentTcpService
) -> None:
    silent.accepted.clear()
    plan, registered = register_plan(
        service,
        "http_tls_cancel.json",
        {"__TLS_SILENT_PORT__": str(silent_port)},
    )
    status, started = service.request_json(
        "POST",
        f"/api/v1/workflows/{plan['workflow_id']}/runs",
        {"plan_id": registered["plan_id"]},
    )
    require(status == 202, f"TLS cancellation run start failed: {started}")
    require(silent.accepted.wait(timeout=5), "TLS connection was not accepted")
    status, response = service.request_json(
        "POST", f"/api/v1/workflow-runs/{started['run_id']}/cancel"
    )
    require(status == 202, f"TLS cancellation request failed: {response}")
    snapshot = wait_for_terminal(
        service, started["run_id"], "http_tls_cancel.json"
    )
    require(snapshot["state"] == "cancelled", str(snapshot))
    require(task(snapshot, "request")["state"] == "cancelled", str(snapshot))


def verify_http_service_shutdown(
    service: DagforgeService, target_port: int, target_state: HttpTargetState
) -> None:
    target_state.cancel_started.clear()
    plan, registered = register_plan(
        service,
        "http_cancel.json",
        {"__HTTP_TARGET_PORT__": str(target_port)},
    )
    status, started = service.request_json(
        "POST",
        f"/api/v1/workflows/{plan['workflow_id']}/runs",
        {"plan_id": registered["plan_id"]},
    )
    require(status == 202, f"HTTP shutdown run start failed: {started}")
    require(
        target_state.cancel_started.wait(timeout=5),
        "HTTP shutdown request did not start",
    )
    service.stop(require_graceful=True)


def verify_http_plan_rejections(service: DagforgeService, target_port: int) -> None:
    status, response = service.request_json(
        "POST", "/api/v1/workflows/plans", load_plan("http_unlisted_origin.json")
    )
    require(status == 403, f"unlisted HTTP origin was not rejected: {response}")
    error = response.get("error", {}) if isinstance(response, dict) else {}
    require(
        error.get("code") == "http_target_not_allowed"
        and error.get("path") == "/nodes/0/config/url",
        f"unlisted HTTP origin returned the wrong diagnostic: {response}",
    )
    status, response = service.request_json(
        "POST",
        "/api/v1/workflows/plans",
        load_plan(
            "http_unsafe_header.json",
            {"__HTTP_TARGET_PORT__": str(target_port)},
        ),
    )
    require(status == 400, f"unsafe HTTP header was not rejected: {response}")
    for name in (
        "http_undeclared_input.json",
        "http_get_body.json",
        "http_duplicate_header.json",
    ):
        status, response = service.request_json(
            "POST",
            "/api/v1/workflows/plans",
            load_plan(name, {"__HTTP_TARGET_PORT__": str(target_port)}),
        )
        require(status == 400, f"invalid HTTP plan {name} was accepted: {response}")


def verify_tls_only_api(
    binary: Path,
    config: Path,
    port: int,
    environment: dict[str, str],
    certificate: Path,
) -> None:
    service = DagforgeService(
        binary, config, port, environment, tls_ca=certificate
    )
    try:
        service.wait_until_ready()
        status, response = service.request_json("GET", "/api/health")
        require(status == 200, f"TLS API health failed: {response}")

        plaintext = b""
        with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
            connection.settimeout(2)
            connection.sendall(
                b"GET /api/health HTTP/1.1\r\n"
                b"Host: localhost\r\nConnection: close\r\n\r\n"
            )
            try:
                plaintext = connection.recv(4096)
            except (ConnectionResetError, TimeoutError, socket.timeout):
                plaintext = b""
        require(
            not plaintext.startswith(b"HTTP/"),
            f"TLS-only listener accepted plaintext: {plaintext!r}",
        )
    finally:
        service.stop(require_graceful=True)


def main() -> int:
    arguments = parse_arguments()
    binary = arguments.binary.resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), "binary is not executable")

    command_plan_names = [
        "hello_world.json",
        "dataflow.json",
        "sanitized_environment.json",
        "fanout_fanin.json",
        "conditional.json",
        "retry_failure.json",
        "fail_fast.json",
        "large_artifact.json",
        "missing_published_output.json",
    ]
    for name in command_plan_names:
        validate_plan(binary, name)

    state_root = Path.home() / ".cache/dagforge-real-e2e" / uuid.uuid4().hex
    state_root.mkdir(parents=True)
    config_path = state_root / "system_config.json"
    port = reserve_port()
    target_port = reserve_port()
    tls_target_port = reserve_port()
    tls_silent_port = reserve_port()
    certificate, private_key = generate_tls_certificate(state_root)
    write_config(
        config_path,
        state_root,
        port,
        target_port,
        tls_target_port,
        tls_silent_port,
    )

    http_replacements = {
        "__HTTP_TARGET_PORT__": str(target_port),
        "__HTTPS_TARGET_PORT__": str(tls_target_port),
        "__TLS_SILENT_PORT__": str(tls_silent_port),
    }
    http_plan_names = [
        "http_pipeline.json",
        "http_accepted_status.json",
        "http_retry.json",
        "http_response_limit.json",
        "http_request_body_limit.json",
        "http_response_header_limit.json",
        "http_timeout.json",
        "http_cancel.json",
        "http_invalid_utf8.json",
        "http_invalid_header_utf8.json",
        "http_forbidden.json",
        "http_concurrency_limit.json",
        "http_header_limit.json",
        "http_tls.json",
        "http_tls_cancel.json",
    ]
    for name in http_plan_names:
        validate_materialized_plan(
            binary, config_path, state_root, name, http_replacements
        )

    target_state = HttpTargetState()
    target = HttpTargetService(target_port, target_state)
    tls_target = HttpTargetService(
        tls_target_port, target_state, certificate, private_key
    )
    silent_tls = SilentTcpService(tls_silent_port)
    environment = os.environ.copy()
    environment["SSL_CERT_FILE"] = str(certificate)
    service = DagforgeService(binary, config_path, port, environment)
    try:
        target.start()
        tls_target.start()
        silent_tls.start()
        service.wait_until_ready()
        checks = [
            ("hello world", verify_hello_world),
            ("dataflow", verify_dataflow),
            ("sanitized environment", verify_sanitized_environment),
            ("fanout/fanin", verify_fanout_fanin),
            ("conditional", verify_conditional),
            ("retry failure", verify_retry_failure),
            ("fail fast", verify_fail_fast),
            ("large artifact", verify_large_artifact),
            ("missing published output", verify_missing_published_output),
            (
                "HTTP pipeline",
                lambda current: verify_http_pipeline(current, target_port),
            ),
            (
                "HTTP accepted status",
                lambda current: verify_http_accepted_status(current, target_port),
            ),
            (
                "HTTP retry",
                lambda current: verify_http_retry(
                    current, target_port, target_state
                ),
            ),
            (
                "HTTP response limit",
                lambda current: verify_http_response_limit(current, target_port),
            ),
            (
                "HTTP request body limit",
                lambda current: verify_http_request_body_limit(
                    current, target_port
                ),
            ),
            (
                "HTTP response header limit",
                lambda current: verify_http_response_header_limit(
                    current, target_port
                ),
            ),
            (
                "HTTP timeout",
                lambda current: verify_http_timeout(current, target_port),
            ),
            (
                "HTTP invalid UTF-8",
                lambda current: verify_http_invalid_utf8(current, target_port),
            ),
            (
                "HTTP invalid header UTF-8",
                lambda current: verify_http_invalid_header_utf8(
                    current, target_port
                ),
            ),
            (
                "HTTP forbidden",
                lambda current: verify_http_forbidden(current, target_port),
            ),
            (
                "HTTP concurrency limit",
                lambda current: verify_http_concurrency_limit(
                    current, target_port
                ),
            ),
            (
                "HTTP header limit",
                lambda current: verify_http_header_limit(current, target_port),
            ),
            (
                "HTTP TLS",
                lambda current: verify_http_tls(current, tls_target_port),
            ),
            (
                "HTTP cancellation",
                lambda current: verify_http_cancel(
                    current, target_port, target_state
                ),
            ),
            (
                "HTTP TLS handshake cancellation",
                lambda current: verify_http_tls_handshake_cancel(
                    current, tls_silent_port, silent_tls
                ),
            ),
            (
                "HTTP plan rejection",
                lambda current: verify_http_plan_rejections(current, target_port),
            ),
        ]
        for label, check in checks:
            check(service)
            print(f"PASS {label}")
        verify_http_service_shutdown(service, target_port, target_state)
        print("PASS HTTP service shutdown")

        tls_api_port = reserve_port()
        tls_api_config = state_root / "tls_api_config.json"
        write_config(
            tls_api_config,
            state_root,
            tls_api_port,
            target_port,
            tls_target_port,
            tls_silent_port,
            certificate,
            private_key,
        )
        verify_tls_only_api(
            binary, tls_api_config, tls_api_port, environment, certificate
        )
        print("PASS API TLS-only listener")
    except BaseException:
        if service._process.poll() is not None:
            output = service._read_output()
            if output:
                print("dagforge process output:\n" + output, file=sys.stderr)
        raise
    finally:
        service.stop()
        target.stop()
        tls_target.stop()
        silent_tls.stop()
        shutil.rmtree(state_root, ignore_errors=True)

    validated_count = len(command_plan_names) + len(http_plan_names)
    print(f"PASS validated {validated_count} real Workflow JSON plans")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, RuntimeError) as error:
        print(f"FAIL {error}", file=sys.stderr)
        raise SystemExit(1)

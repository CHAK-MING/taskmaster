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
import tempfile
import threading
import time


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def generate_tls_certificate(root: Path) -> tuple[Path, Path]:
    certificate = root / "cli-localhost.crt"
    private_key = root / "cli-localhost.key"
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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to generate CLI TLS certificate:\n"
            f"{result.stdout}{result.stderr}"
        )
    return certificate, private_key


def write_config(path: Path, root: Path, port: int) -> None:
    config = json.loads(
        (REPOSITORY_ROOT / "system_config.json").read_text(encoding="utf-8")
    )
    config["executors"]["command"]["minijail"]["execution_root"] = str(
        root / "executions"
    )
    config["storage"]["directory"] = str(root / "state")
    config["api"]["port"] = port
    config["runtime"]["shards"] = 2
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


def run(
    binary: Path,
    *arguments: str,
    expected: int,
    timeout: float = 15,
    environment: dict[str, str] | None = None,
    stdin_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(binary), *arguments],
        cwd=REPOSITORY_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        input=stdin_text,
        timeout=timeout,
        check=False,
    )
    if result.returncode != expected:
        raise AssertionError(
            f"command returned {result.returncode}, expected {expected}:\n"
            f"{binary} {' '.join(arguments)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def verify_raw_api_transport(
    binary: Path,
    root: Path,
    environment: dict[str, str],
) -> None:
    requests: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: object) -> None:
            del format, args

        def handle_request(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length else b""
            requests.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                }
            )

            status = 418 if self.path == "/failure" else 200
            response_body = b"teapot" if status == 418 else b"response:" + body
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("X-DAGForge-Test", "raw-api")
            self.send_header("Content-Length", str(len(response_body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response_body)

        do_GET = handle_request
        do_POST = handle_request
        do_PUT = handle_request
        do_DELETE = handle_request
        do_PATCH = handle_request
        do_OPTIONS = handle_request
        do_HEAD = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = int(server.server_address[1])
    endpoint = f"http://127.0.0.1:{port}"
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    binary_body = root / "raw-request.bin"
    binary_body.write_bytes(b"\x00binary\xff")
    output_file = root / "raw-response.bin"

    try:
        included = run(
            binary,
            "api",
            "request",
            "post",
            "/literal",
            '{"value":1}',
            "--header",
            " X-Trace : observed ",
            "--token",
            "secret-token",
            "--include",
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )
        if "HTTP/1.1 200 OK" not in included.stdout:
            raise AssertionError(included.stdout)
        if "X-DAGForge-Test: raw-api" not in included.stdout:
            raise AssertionError(included.stdout)

        run(
            binary,
            "api",
            "request",
            "put",
            "/file",
            f"@{binary_body}",
            "--header",
            "Content-Type: application/x-dagforge-test",
            "--output",
            str(output_file),
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )
        if output_file.read_bytes() != b"response:\x00binary\xff":
            raise AssertionError("raw API output file changed response bytes")

        run(
            binary,
            "api",
            "request",
            "patch",
            "/stdin",
            "-",
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
            stdin_text='{"stdin":true}',
        )
        run(
            binary,
            "api",
            "request",
            "options",
            "/options",
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )
        run(
            binary,
            "api",
            "request",
            "head",
            "/head",
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )
        failed = run(
            binary,
            "api",
            "request",
            "delete",
            "/failure",
            "--endpoint",
            endpoint,
            expected=1,
            environment=environment,
        )
        if "HTTP 418" not in failed.stderr or failed.stdout != "teapot":
            raise AssertionError(
                f"unexpected non-success response\nstdout={failed.stdout!r}\n"
                f"stderr={failed.stderr!r}"
            )

        run(
            binary,
            "api",
            "request",
            "get",
            "/output-error",
            "--output",
            str(root / "missing-parent" / "response.bin"),
            "--endpoint",
            endpoint,
            expected=1,
            environment=environment,
        )
        if Path("/dev/full").exists():
            run(
                binary,
                "api",
                "request",
                "get",
                "/device-full",
                "--output",
                "/dev/full",
                "--endpoint",
                endpoint,
                expected=1,
                environment=environment,
            )

        run(
            binary,
            "api",
            "request",
            "post",
            "/invalid-body",
            "@",
            "--endpoint",
            endpoint,
            expected=2,
            environment=environment,
        )
        run(
            binary,
            "api",
            "request",
            "post",
            "/invalid-header",
            "{}",
            "--header",
            " : value",
            "--endpoint",
            endpoint,
            expected=2,
            environment=environment,
        )
        run(
            binary,
            "api",
            "request",
            "post",
            "/missing-body",
            f"@{root / 'missing-request.json'}",
            "--endpoint",
            endpoint,
            expected=2,
            environment=environment,
        )
        run(
            binary,
            "api",
            "request",
            "post",
            "/framing-header",
            "{}",
            "--header",
            "Content-Length: 2",
            "--endpoint",
            endpoint,
            expected=2,
            environment=environment,
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    expected_methods = ["POST", "PUT", "PATCH", "OPTIONS", "HEAD", "DELETE", "GET"]
    if Path("/dev/full").exists():
        expected_methods.append("GET")
    observed_methods = [str(request["method"]) for request in requests]
    if observed_methods != expected_methods:
        raise AssertionError(f"raw API methods: {observed_methods}")

    literal_headers = requests[0]["headers"]
    if not isinstance(literal_headers, dict):
        raise AssertionError(literal_headers)
    if literal_headers.get("content-type") != "application/json":
        raise AssertionError(literal_headers)
    if literal_headers.get("authorization") != "Bearer secret-token":
        raise AssertionError(literal_headers)
    if literal_headers.get("x-trace") != "observed":
        raise AssertionError(literal_headers)
    if requests[0]["body"] != b'{"value":1}':
        raise AssertionError(requests[0])

    file_headers = requests[1]["headers"]
    if not isinstance(file_headers, dict):
        raise AssertionError(file_headers)
    if file_headers.get("content-type") != "application/x-dagforge-test":
        raise AssertionError(file_headers)
    if requests[1]["body"] != b"\x00binary\xff":
        raise AssertionError(requests[1])

    stdin_headers = requests[2]["headers"]
    if not isinstance(stdin_headers, dict):
        raise AssertionError(stdin_headers)
    if stdin_headers.get("content-type") != "application/json":
        raise AssertionError(stdin_headers)
    if requests[2]["body"] != b'{"stdin":true}':
        raise AssertionError(requests[2])

    certificate, private_key = generate_tls_certificate(root)
    tls_server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls_context.load_cert_chain(certificate, private_key)
    tls_server.socket = tls_context.wrap_socket(tls_server.socket, server_side=True)
    tls_port = int(tls_server.server_address[1])
    tls_thread = threading.Thread(target=tls_server.serve_forever, daemon=True)
    tls_thread.start()
    try:
        result = run(
            binary,
            "api",
            "request",
            "get",
            "/tls",
            "--ca-file",
            str(certificate),
            "--endpoint",
            f"https://localhost:{tls_port}",
            expected=0,
            environment=environment,
        )
        if result.stdout != "response:":
            raise AssertionError(result.stdout)
    finally:
        tls_server.shutdown()
        tls_server.server_close()
        tls_thread.join(timeout=5)


def wait_for_listener(process: subprocess.Popen[str], port: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                f"serve exited before becoming ready\nstdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError("serve did not open its configured listener")


def verify_ipv6_endpoint(
    binary: Path, environment: dict[str, str]
) -> None:
    try:
        listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        listener.bind(("::1", 0))
        listener.listen(1)
        listener.settimeout(5)
    except OSError:
        print("SKIP CLI IPv6 endpoint: IPv6 loopback is unavailable")
        return

    port = int(listener.getsockname()[1])
    observed_request: list[bytes] = []
    server_errors: list[BaseException] = []

    def serve_once() -> None:
        try:
            connection, _ = listener.accept()
            with connection:
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    request += chunk
                observed_request.append(request)
                body = b'{"status":"healthy"}'
                connection.sendall(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: "
                    + str(len(body)).encode("ascii")
                    + b"\r\nConnection: close\r\n\r\n"
                    + body
                )
        except BaseException as error:
            server_errors.append(error)

    server = threading.Thread(target=serve_once, daemon=True)
    server.start()
    try:
        result = run(
            binary,
            "api",
            "health",
            "--endpoint",
            f"http://[::1]:{port}",
            expected=0,
            environment=environment,
        )
        if parse_json_output(result).get("status") != "healthy":
            raise AssertionError(result.stdout)
    finally:
        listener.close()
        server.join(timeout=5)

    if server_errors:
        raise AssertionError(f"IPv6 test server failed: {server_errors[0]}")
    if not observed_request:
        raise AssertionError("IPv6 endpoint received no request")
    expected_host = f"Host: [::1]:{port}".encode("ascii")
    if expected_host not in observed_request[0]:
        raise AssertionError(observed_request[0])


def parse_json_output(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(f"invalid JSON output:\n{result.stdout}") from error
    if not isinstance(parsed, dict):
        raise AssertionError(f"expected JSON object, got: {parsed!r}")
    return parsed


def wait_for_run(
    binary: Path,
    endpoint: str,
    run_id: str,
    environment: dict[str, str],
) -> dict[str, object]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snapshot = parse_json_output(
            run(
                binary,
                "api",
                "run",
                "get",
                run_id,
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if snapshot.get("state") in {"succeeded", "failed", "cancelled"}:
            return snapshot
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} did not reach a terminal state")


def verify_service_commands(
    binary: Path,
    config: Path,
    port: int,
    root: Path,
    environment: dict[str, str],
) -> None:
    service_config = root / "service-storage.json"
    service_document = json.loads(config.read_text(encoding="utf-8"))
    service_document["storage"]["enabled"] = True
    service_document["storage"]["directory"] = str(root / "service-state")
    service_config.write_text(
        json.dumps(service_document, indent=2) + "\n", encoding="utf-8"
    )
    process = subprocess.Popen(
        [str(binary), "serve", str(service_config)],
        cwd=REPOSITORY_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_listener(process, port)
        contended = run(
            binary,
            "validate",
            "dags/hello_world.json",
            "--config",
            str(service_config),
            expected=1,
            environment=environment,
        )
        if "already exists" not in contended.stderr:
            raise AssertionError(contended.stderr)
        endpoint = f"http://127.0.0.1:{port}"

        health = parse_json_output(
            run(
                binary,
                "api",
                "health",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if health.get("status") != "healthy":
            raise AssertionError(health)

        status = parse_json_output(
            run(
                binary,
                "api",
                "request",
                "get",
                "/api/status",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if status.get("runtime") != "running":
            raise AssertionError(status)

        registered = parse_json_output(
            run(
                binary,
                "api",
                "plan",
                "add",
                "dags/hello_world.json",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        plan_id = registered.get("plan_id")
        if not isinstance(plan_id, str) or not plan_id:
            raise AssertionError(registered)

        listed = parse_json_output(
            run(
                binary,
                "api",
                "plan",
                "ls",
                "--limit",
                "1",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if listed.get("total") != 1:
            raise AssertionError(listed)
        shown = parse_json_output(
            run(
                binary,
                "api",
                "plan",
                "show",
                plan_id,
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if shown.get("plan_id") != plan_id:
            raise AssertionError(shown)

        started = parse_json_output(
            run(
                binary,
                "api",
                "run",
                "start",
                "hello-world",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        run_id = started.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise AssertionError(started)
        snapshot = wait_for_run(binary, endpoint, run_id, environment)
        if snapshot.get("state") != "succeeded":
            raise AssertionError(snapshot)

        failure_report = parse_json_output(
            run(
                binary,
                "api",
                "run",
                "failures",
                run_id,
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if failure_report.get("run_id") != run_id:
            raise AssertionError(failure_report)

        run(
            binary,
            "api",
            "run",
            "repair",
            run_id,
            '{"reason":"terminal run cannot be repaired"}',
            "--endpoint",
            endpoint,
            expected=1,
            environment=environment,
        )
        for action in ("pause", "resume", "cancel"):
            run(
                binary,
                "api",
                "run",
                action,
                run_id,
                "--endpoint",
                endpoint,
                expected=1,
                environment=environment,
            )

        output = parse_json_output(
            run(
                binary,
                "api",
                "run",
                "output",
                run_id,
                "start",
                "stdout",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if "hello from DAGForge" not in json.dumps(output):
            raise AssertionError(output)
        evidence = parse_json_output(
            run(
                binary,
                "api",
                "run",
                "evidence",
                run_id,
                "--limit",
                "1",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        if evidence.get("total", 0) == 0:
            raise AssertionError(evidence)

        artifact_source = root / "artifact.txt"
        artifact_output = root / "artifact-output.txt"
        artifact_source.write_text("cli artifact\n", encoding="utf-8")
        stored = parse_json_output(
            run(
                binary,
                "api",
                "artifact",
                "put",
                str(artifact_source),
                "--type",
                "text/plain",
                "--endpoint",
                endpoint,
                expected=0,
                environment=environment,
            )
        )
        artifact_id = stored.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id:
            raise AssertionError(stored)
        run(
            binary,
            "api",
            "artifact",
            "get",
            artifact_id,
            "--output",
            str(artifact_output),
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )
        if artifact_output.read_text(encoding="utf-8") != "cli artifact\n":
            raise AssertionError("artifact download changed content")
        run(
            binary,
            "api",
            "artifact",
            "rm",
            artifact_id,
            "--endpoint",
            endpoint,
            expected=0,
            environment=environment,
        )

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        if process.returncode != 0:
            raise AssertionError(
                f"serve shutdown returned {process.returncode}\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        run(
            binary,
            "validate",
            "dags/hello_world.json",
            "--config",
            str(service_config),
            expected=0,
            environment=environment,
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise FileNotFoundError(binary)

    root = Path(
        tempfile.mkdtemp(
            prefix="dagforge-cli-scenarios-", dir=REPOSITORY_ROOT / ".git"
        )
    )
    config = root / "system.json"
    disabled_config = root / "workflow-disabled.json"
    busy_config = root / "busy-port.json"
    invalid_config = root / "invalid-config.json"
    invalid_plan = root / "invalid.json"
    http_plan = root / "http.json"
    http_missing_url_plan = root / "http-missing-url.json"
    http_relative_url_plan = root / "http-relative-url.json"
    http_malformed_url_plan = root / "http-malformed-url.json"
    port = reserve_port()
    write_config(config, root, port)
    disabled_document = json.loads(config.read_text(encoding="utf-8"))
    disabled_document["workflow"]["enabled"] = False
    disabled_config.write_text(
        json.dumps(disabled_document, indent=2) + "\n", encoding="utf-8"
    )
    invalid_config.write_text(
        json.dumps({"runtime": {"shards": -1}}), encoding="utf-8"
    )
    invalid_plan.write_text("{", encoding="utf-8")
    http_plan.write_text(
        json.dumps(
            {
                "workflow_id": "cli-http-validation",
                "schema_version": 1,
                "nodes": [
                    {
                        "id": "request",
                        "executor": "http",
                        "outputs": ["result"],
                        "config": {
                            "method": "GET",
                            "url": "http://127.0.0.1:1/health",
                            "headers": [],
                            "input_headers": [],
                            "accepted_statuses": [],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    for path, workflow_id, url in (
        (http_missing_url_plan, "cli-http-missing-url", None),
        (http_relative_url_plan, "cli-http-relative-url", "/relative"),
        (http_malformed_url_plan, "cli-http-malformed-url", "http://["),
    ):
        node_config: dict[str, object] = {
            "method": "GET",
            "headers": [],
            "input_headers": [],
            "accepted_statuses": [],
        }
        if url is not None:
            node_config["url"] = url
        path.write_text(
            json.dumps(
                {
                    "workflow_id": workflow_id,
                    "schema_version": 1,
                    "nodes": [
                        {
                            "id": "request",
                            "executor": "http",
                            "outputs": ["result"],
                            "config": node_config,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    environment = os.environ.copy()
    for variable in ("DAGFORGE_CONFIG", "DAGFORGE_ENDPOINT", "DAGFORGE_API_TOKEN"):
        environment.pop(variable, None)
    try:
        validated = run(
            binary,
            "validate",
            "dags/hello_world.json",
            expected=0,
            environment=environment,
        )
        if "workflow_id=hello-world" not in validated.stdout:
            raise AssertionError(validated.stdout)
        print("PASS CLI validate command plan")

        run(
            binary,
            "validate",
            str(http_plan),
            expected=0,
            environment=environment,
        )
        run(
            binary,
            "validate",
            str(http_missing_url_plan),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "validate",
            str(http_relative_url_plan),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "validate",
            str(http_malformed_url_plan),
            expected=1,
            environment=environment,
        )
        print("PASS CLI offline HTTP validation")

        run(
            binary,
            "validate",
            str(http_plan),
            "--config",
            str(config),
            expected=1,
            environment=environment,
        )
        print("PASS CLI policy-aware rejection")

        run(
            binary,
            "validate",
            str(invalid_plan),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "validate",
            "dags/hello_world.json",
            "--config",
            str(invalid_config),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "validate",
            "dags/hello_world.json",
            "--config",
            str(disabled_config),
            expected=1,
            environment=environment,
        )
        print("PASS CLI validation failures")

        completed = run(
            binary,
            "run",
            "dags/hello_world.json",
            "--config",
            str(config),
            expected=0,
            environment=environment,
        )
        if "succeeded" not in completed.stdout:
            raise AssertionError(completed.stdout)
        print("PASS CLI run waits for terminal state")

        run(
            binary,
            "run",
            "dags/retry_failure.json",
            "--config",
            str(config),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "run",
            "dags/hello_world.json",
            "--config",
            str(invalid_config),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "run",
            str(invalid_plan),
            "--config",
            str(config),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "run",
            str(http_plan),
            "--config",
            str(config),
            expected=1,
            environment=environment,
        )
        print("PASS CLI run failures")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen(1)
            busy_port = int(occupied.getsockname()[1])
            write_config(busy_config, root, busy_port)
            run(
                binary,
                "serve",
                str(busy_config),
                expected=1,
                environment=environment,
            )
        run(
            binary,
            "serve",
            str(invalid_config),
            expected=1,
            environment=environment,
        )
        run(binary, "run", "dags/hello_world.json", "--wait", expected=109)
        run(binary, "run", "dags/hello_world.json", "--payload", "{}", expected=109)
        run(binary, "api", "plan", "list", "--limit", "0", expected=105)
        run(binary, "api", "run", "get", "bad/id", expected=106)
        run(binary, "api", "request", "trace", "/api/status", expected=106)
        run(binary, "api", "request", "get", "api/status", expected=106)
        run(binary, "api", "request", "get", "/api\nstatus", expected=106)
        run(binary, "api", "run", "get", "", expected=106)
        run(binary, "api", "request", "get", "/", "--header", "bad", expected=105)
        run(binary, "api", "health", "--endpoint", "ftp://example.com", expected=105)
        run(
            binary,
            "api",
            "health",
            "--endpoint",
            "http://example.com/path",
            expected=105,
        )
        run(binary, "api", "health", "--endpoint", "not a url", expected=105)
        run(
            binary,
            "api",
            "health",
            "--endpoint",
            "http://127.0.0.1:0",
            expected=2,
            environment=environment,
        )
        multiple = run(
            binary,
            "api",
            "health",
            "status",
            "--endpoint",
            "http://127.0.0.1:1",
            expected=105,
        )
        if "API request failed" in multiple.stderr:
            raise AssertionError("multiple subcommands executed before rejection")
        run(
            binary,
            "api",
            "health",
            "--endpoint",
            "http://127.0.0.1:1",
            expected=1,
            environment=environment,
        )
        print("PASS CLI11 option and positional validation")

        verify_ipv6_endpoint(binary, environment)
        print("PASS CLI IPv6 endpoint")

        verify_raw_api_transport(binary, root, environment)
        print("PASS CLI raw API transport")

        verify_service_commands(binary, config, port, root, environment)
        print("PASS CLI serve and semantic API commands")
    finally:
        shutil.rmtree(root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

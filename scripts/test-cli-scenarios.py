#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def toml_string(value: Path | str) -> str:
    return json.dumps(str(value))


def write_config(path: Path, root: Path, port: int) -> None:
    source = (REPOSITORY_ROOT / "system_config.toml").read_text(encoding="utf-8")
    source = source.replace(
        'execution_root = "./executions"',
        f"execution_root = {toml_string(root / 'executions')}",
    )
    source = source.replace(
        'directory = "./state"', f"directory = {toml_string(root / 'state')}"
    )
    source = source.replace("port = 8888", f"port = {port}")
    source = source.replace("shards = 0", "shards = 2")
    path.write_text(source, encoding="utf-8")


def run(
    binary: Path,
    *arguments: str,
    expected: int,
    timeout: float = 15,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(binary), *arguments],
        cwd=REPOSITORY_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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


def verify_serve_shutdown(
    binary: Path, config: Path, port: int, environment: dict[str, str]
) -> None:
    process = subprocess.Popen(
        [str(binary), "serve", "--config", str(config)],
        cwd=REPOSITORY_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_listener(process, port)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        if process.returncode != 0:
            raise AssertionError(
                f"serve shutdown returned {process.returncode}\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
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
    config = root / "system.toml"
    invalid_config = root / "invalid.toml"
    invalid_plan = root / "invalid.json"
    http_plan = root / "http.json"
    port = reserve_port()
    write_config(config, root, port)
    invalid_config.write_text("[runtime]\nshards = -1\n", encoding="utf-8")
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

    environment = os.environ.copy()
    try:
        validated = run(
            binary,
            "validate",
            "--file",
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
            "--file",
            str(http_plan),
            expected=0,
            environment=environment,
        )
        print("PASS CLI offline HTTP validation")

        run(
            binary,
            "validate",
            "--config",
            str(config),
            "--file",
            str(http_plan),
            expected=1,
            environment=environment,
        )
        print("PASS CLI policy-aware rejection")

        run(
            binary,
            "validate",
            "--file",
            str(invalid_plan),
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "validate",
            "--config",
            str(invalid_config),
            "--file",
            "dags/hello_world.json",
            expected=1,
            environment=environment,
        )
        print("PASS CLI validation failures")

        completed = run(
            binary,
            "run",
            "--config",
            str(config),
            "--file",
            "dags/hello_world.json",
            "--payload",
            '{"source":"cli"}',
            "--wait",
            expected=0,
            environment=environment,
        )
        if "succeeded" not in completed.stdout:
            raise AssertionError(completed.stdout)
        print("PASS CLI run wait JSON payload")

        run(
            binary,
            "run",
            "--config",
            str(config),
            "--file",
            "dags/hello_world.json",
            "--payload",
            "plain-text",
            expected=0,
            environment=environment,
        )
        print("PASS CLI run no-wait text payload")

        run(
            binary,
            "run",
            "--config",
            str(config),
            "--file",
            "dags/retry_failure.json",
            "--wait",
            expected=1,
            environment=environment,
        )
        run(
            binary,
            "run",
            "--config",
            str(invalid_config),
            "--file",
            "dags/hello_world.json",
            expected=1,
            environment=environment,
        )
        print("PASS CLI run failures")

        run(
            binary,
            "serve",
            "--config",
            str(invalid_config),
            expected=1,
            environment=environment,
        )
        verify_serve_shutdown(binary, config, port, environment)
        print("PASS CLI serve startup and signal shutdown")
    finally:
        shutil.rmtree(root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

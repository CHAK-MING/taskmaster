from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Mapping, Sequence


def run_command(
    cmd: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        cwd=str(cwd),
        env=dict(env) if env is not None else None,
        check=check,
        text=True,
        capture_output=capture_output,
    )


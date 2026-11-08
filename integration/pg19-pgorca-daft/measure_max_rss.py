#!/usr/bin/env python3
"""Run one command and record its Linux child-process maximum RSS in KiB."""

from __future__ import annotations

import resource
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: measure_max_rss.py REPORT COMMAND [ARG ...]", file=sys.stderr)
        return 2
    report = Path(sys.argv[1])
    result = subprocess.run(sys.argv[2:], check=False)
    max_rss_kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    report.write_text(f"{max_rss_kb}\n", encoding="ascii")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

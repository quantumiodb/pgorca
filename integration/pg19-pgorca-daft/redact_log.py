#!/usr/bin/env python3
"""Fail when integration reports contain common secret-bearing patterns."""

from __future__ import annotations

import re
import sys
from pathlib import Path


PATTERNS = {
    "authorization bearer": re.compile(r"authorization\s*[:=]\s*bearer\s+\S+", re.IGNORECASE),
    "aws access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "signed url": re.compile(r"[?&](X-Amz-Signature|Signature|sig)=", re.IGNORECASE),
    "password assignment": re.compile(r"\b(password|passwd|secret|token)\s*[:=]\s*[^\s,]+", re.IGNORECASE),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "credential URI": re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s/:]+:[^\s/@]+@", re.IGNORECASE),
    "home directory": re.compile(r"/home/[^/\s]+/"),
    "spill directory": re.compile(r"/(tmp|var/tmp)/pgdaft-[^\s]+", re.IGNORECASE),
    "IR payload": re.compile(r"\b(ir[_ ]?payload|plan[_ ]?payload|payload_bytes)\s*[:=]", re.IGNORECASE),
    "row or tuple value": re.compile(r"\b(row_value|tuple_value|sample_row|data_value)\s*[:=]", re.IGNORECASE),
    "internal address": re.compile(r"\b0x[0-9a-f]{8,}\b", re.IGNORECASE),
}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: redact_log.py REPORT_PATH", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    if not root.exists():
        print(f"report path does not exist: {root}", file=sys.stderr)
        return 2
    findings = []
    paths = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            findings.append((path, "read error", str(exc)))
            continue
        for label, pattern in PATTERNS.items():
            match = pattern.search(text)
            if match:
                findings.append((path, label, f"offset={match.start()}"))
    if findings:
        for path, label, detail in findings:
            print(f"FAIL {path}: {label} {detail}", file=sys.stderr)
        return 1
    print(f"PASS redaction scan: {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Summarize Borromean memory profile JSON reports."""

from __future__ import annotations

import json
import sys
from pathlib import Path


HEADERS = [
    "scenario",
    "ops/s",
    "reads",
    "writes",
    "checkpoint",
    "apply",
    "undo_records",
    "undo_bytes",
    "checkpoint_fallbacks",
    "update_encode",
    "wal_encode",
    "wal_write",
    "wal_sync",
    "key_cmps",
    "key_decodes",
    "value_decodes",
    "write_bytes",
]


def fmt_nanos(value: object) -> str:
    nanos = int(value or 0)
    if nanos < 1_000:
        return f"{nanos}ns"
    if nanos < 1_000_000:
        return f"{nanos / 1_000:.3f}us"
    if nanos < 1_000_000_000:
        return f"{nanos / 1_000_000:.3f}ms"
    return f"{nanos / 1_000_000_000:.3f}s"


def summary_row(path: Path) -> list[str]:
    if not path.exists():
        return [path.stem, "missing", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]

    data = json.loads(path.read_text())
    report = next(
        (
            item
            for item in data["engine_reports"]
            if item["engine"] == "borromean-memory"
        ),
        None,
    )
    if report is None:
        return [path.stem, "no-report", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]

    counters = report["counters"]
    metrics = report.get("borromean_core_metrics") or {}
    process_io = (report.get("diagnostics") or {}).get("workload_process_io") or {}
    writes = int(counters.get("sets", 0)) + int(counters.get("deletes", 0))
    return [
        path.stem,
        f"{float(report['operations_per_second']):.1f}",
        str(counters.get("reads", 0)),
        str(writes),
        fmt_nanos(metrics.get("frontier_checkpoint_nanos", 0)),
        fmt_nanos(metrics.get("frontier_apply_nanos", 0)),
        str(metrics.get("frontier_undo_records", 0)),
        str(metrics.get("frontier_undo_bytes", 0)),
        str(metrics.get("frontier_full_checkpoint_fallbacks", 0)),
        fmt_nanos(metrics.get("update_encode_nanos", 0)),
        fmt_nanos(metrics.get("wal_encode_nanos", 0)),
        fmt_nanos(metrics.get("wal_write_nanos", 0)),
        fmt_nanos(metrics.get("wal_sync_nanos", 0)),
        str(metrics.get("encoded_key_comparisons", 0)),
        str(metrics.get("key_decodes_during_comparison", 0)),
        str(metrics.get("value_decodes", 0)),
        str(process_io.get("write_bytes", 0)),
    ]


def print_table(rows: list[list[str]]) -> None:
    widths = [len(header) for header in HEADERS]
    for row in rows:
        widths = [max(width, len(value)) for width, value in zip(widths, row)]

    print("memory profile summary:")
    print("  " + "  ".join(header.ljust(width) for header, width in zip(HEADERS, widths)))
    for row in rows:
        print("  " + "  ".join(value.ljust(width) for value, width in zip(row, widths)))


def main() -> int:
    rows = [summary_row(Path(arg)) for arg in sys.argv[1:]]
    print_table(rows)
    if any(row[-1] not in ("0", "") for row in rows):
        print(
            "warning: one or more memory profiles reported non-zero process write_bytes",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

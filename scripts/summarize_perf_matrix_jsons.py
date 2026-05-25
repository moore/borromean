#!/usr/bin/env python3
"""Render perf matrix JSON reports as Markdown tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


DEFAULT_ENGINE_ORDER = ["borromean", "borromean-memory", "redb", "fjall"]


def fmt_count(value: object) -> str:
    if value is None:
        return "-"
    try:
        number = int(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:,}"


def fmt_float(value: object, digits: int = 1) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:,.{digits}f}"


def fmt_ratio(value: object) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:.2f}x"


def fmt_nanos(value: object) -> str:
    if value is None:
        return "-"
    try:
        nanos = int(value)
    except (TypeError, ValueError):
        return "-"
    if nanos < 1_000:
        return f"{nanos}ns"
    if nanos < 1_000_000:
        return f"{nanos / 1_000:.3f}us"
    if nanos < 1_000_000_000:
        return f"{nanos / 1_000_000:.3f}ms"
    return f"{nanos / 1_000_000_000:.3f}s"


def fmt_bytes(value: object) -> str:
    if value is None:
        return "-"
    try:
        size = int(value)
    except (TypeError, ValueError):
        return "-"
    sign = "-" if size < 0 else ""
    size = abs(size)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    number = float(size)
    unit = units[0]
    for unit in units:
        if number < 1024 or unit == units[-1]:
            break
        number /= 1024
    if unit == "B":
        return f"{sign}{int(number)}B"
    return f"{sign}{number:.2f}{unit}"


def bold(value: str) -> str:
    return f"**{value}**" if value != "-" else value


def best_number(values: list[float], *, higher_is_better: bool) -> float | None:
    if not values:
        return None
    return max(values) if higher_is_better else min(values)


def fmt_best_number(
    value: float | int | None,
    best: float | None,
    formatter: Any,
) -> str:
    if value is None:
        return "-"
    rendered = formatter(value)
    return bold(rendered) if best is not None and float(value) == best else rendered


def scenario_label(path: Path) -> str:
    stem = path.stem
    if stem.startswith("file_backing_"):
        return stem[len("file_backing_") :]
    if stem == "file_backing":
        return "insert"
    return stem


def load_reports(paths: list[Path]) -> list[dict[str, Any]]:
    reports = []
    for path in paths:
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            reports.append({"scenario": scenario_label(path), "path": path, "missing": True})
            continue
        reports.append(
            {
                "scenario": scenario_label(path),
                "path": path,
                "missing": False,
                "data": data,
                "engines": {
                    item.get("engine"): item
                    for item in data.get("engine_reports", [])
                    if item.get("engine")
                },
            }
        )
    return reports


def available_engines(reports: list[dict[str, Any]], hide_memory: bool) -> list[str]:
    seen = set()
    for report in reports:
        seen.update(report.get("engines", {}).keys())
    engines = [engine for engine in DEFAULT_ENGINE_ORDER if engine in seen]
    engines.extend(sorted(seen.difference(DEFAULT_ENGINE_ORDER)))
    if hide_memory:
        engines = [engine for engine in engines if engine != "borromean-memory"]
    return engines


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def engine_report(report: dict[str, Any], engine: str) -> dict[str, Any] | None:
    return report.get("engines", {}).get(engine)


def throughput_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        row = [report["scenario"]]
        values = [
            float(item.get("operations_per_second"))
            for engine in engines
            if (item := engine_report(report, engine)) and item.get("operations_per_second") is not None
        ]
        best = best_number(values, higher_is_better=True)
        for engine in engines:
            item = engine_report(report, engine)
            value = float(item.get("operations_per_second")) if item else None
            row.append(fmt_best_number(value, best, fmt_float))
        rows.append(row)
    return markdown_table(["scenario", *engines], rows)


def relative_throughput_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        base = engine_report(report, "borromean")
        base_ops = float(base.get("operations_per_second", 0)) if base else 0.0
        row = [report["scenario"]]
        ratios = []
        for engine in engines:
            item = engine_report(report, engine)
            if item and base_ops > 0:
                ratios.append(float(item.get("operations_per_second", 0)) / base_ops)
        best = best_number(ratios, higher_is_better=True)
        for engine in engines:
            item = engine_report(report, engine)
            if not item or base_ops <= 0:
                row.append("-")
                continue
            ratio = float(item.get("operations_per_second", 0)) / base_ops
            row.append(fmt_best_number(ratio, best, fmt_ratio))
        rows.append(row)
    return markdown_table(["scenario", *engines], rows)


def latency_table(reports: list[dict[str, Any]], engines: list[str], percentile: str) -> str:
    key = f"{percentile}_nanos"
    rows = []
    for report in reports:
        row = [report["scenario"]]
        values = []
        for engine in engines:
            item = engine_report(report, engine)
            latency = item.get("sampled_latency") if item else None
            if latency and latency.get(key) is not None:
                values.append(float(latency.get(key)))
        best = best_number(values, higher_is_better=False)
        for engine in engines:
            item = engine_report(report, engine)
            latency = item.get("sampled_latency") if item else None
            value = int(latency.get(key)) if latency and latency.get(key) is not None else None
            row.append(fmt_best_number(value, best, fmt_nanos))
        rows.append(row)
    return markdown_table(["scenario", *engines], rows)


def operation_counts_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        for engine in engines:
            item = engine_report(report, engine)
            if not item:
                rows.append([report["scenario"], engine, "-", "-", "-", "-", "-", "-", "-"])
                continue
            counters = item.get("counters") or {}
            rows.append(
                [
                    report["scenario"],
                    engine,
                    fmt_count(counters.get("reads")),
                    fmt_count(counters.get("sets")),
                    fmt_count(counters.get("set_inserts_expected")),
                    fmt_count(counters.get("set_updates_expected")),
                    fmt_count(counters.get("deletes")),
                    fmt_count(counters.get("hits")),
                    fmt_count(counters.get("misses")),
                ]
            )
    return markdown_table(
        ["scenario", "engine", "reads", "sets", "inserts", "updates", "deletes", "hits", "misses"],
        rows,
    )


def disk_io_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        for engine in engines:
            item = engine_report(report, engine)
            if not item:
                rows.append([report["scenario"], engine, "-", "-", "-", "-", "-"])
                continue
            diagnostics = item.get("diagnostics") or {}
            process_io = diagnostics.get("workload_process_io") or {}
            rows.append(
                [
                    report["scenario"],
                    engine,
                    fmt_bytes(item.get("logical_len_bytes")),
                    fmt_bytes(item.get("file_len_bytes")),
                    fmt_bytes(diagnostics.get("fjall_path_size_bytes")),
                    fmt_bytes(process_io.get("read_bytes")),
                    fmt_bytes(process_io.get("write_bytes")),
                ]
            )
    return markdown_table(
        ["scenario", "engine", "logical", "file_len", "path_size", "process_read", "process_write"],
        rows,
    )


def borromean_sync_count(item: dict[str, Any]) -> object:
    metrics = item.get("borromean_core_metrics") or {}
    audit = item.get("sync_audit") or {}
    return metrics.get("wal_syncs", audit.get("wal_syncs"))


def durability_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        for engine in engines:
            item = engine_report(report, engine)
            if not item:
                rows.append([report["scenario"], engine, "-", "-", "-", "-", "-"])
                continue
            diagnostics = item.get("diagnostics") or {}
            metrics = item.get("borromean_core_metrics") or {}
            if engine.startswith("borromean"):
                count = borromean_sync_count(item)
                time = metrics.get("wal_sync_nanos")
                mode = "wal-sync"
            elif engine == "redb":
                count = diagnostics.get("commit_count")
                time = diagnostics.get("commit_nanos")
                mode = "commit"
            elif engine == "fjall":
                count = diagnostics.get("fjall_persist_count")
                time = diagnostics.get("fjall_persist_nanos")
                mode = diagnostics.get("fjall_persist_mode") or "persist"
            else:
                count = "-"
                time = None
                mode = "-"
            count_number = int(count or 0) if isinstance(count, (int, float)) else 0
            time_number = int(time or 0)
            rows.append(
                [
                    report["scenario"],
                    engine,
                    str(mode),
                    fmt_count(count),
                    fmt_nanos(time),
                    fmt_nanos(time_number // count_number) if count_number > 0 else "-",
                    fmt_bytes((metrics.get("wal_bytes") if engine.startswith("borromean") else None)),
                ]
            )
    return markdown_table(
        ["scenario", "engine", "mode", "count", "time", "time/op", "wal_bytes"],
        rows,
    )


def borromean_internals_table(reports: list[dict[str, Any]], engines: list[str]) -> str:
    rows = []
    for report in reports:
        for engine in engines:
            if not engine.startswith("borromean"):
                continue
            item = engine_report(report, engine)
            if not item:
                rows.append([report["scenario"], engine, "-", "-", "-", "-", "-", "-", "-", "-", "-"])
                continue
            metrics = item.get("borromean_core_metrics") or {}
            rows.append(
                [
                    report["scenario"],
                    engine,
                    fmt_count(metrics.get("frontier_cache_hits")),
                    fmt_count(metrics.get("frontier_cache_misses")),
                    fmt_count(metrics.get("frontier_reloads")),
                    fmt_bytes(metrics.get("wal_bytes")),
                    fmt_nanos(metrics.get("wal_sync_nanos")),
                    fmt_nanos(metrics.get("mmap_flush_nanos")),
                    fmt_count(metrics.get("compactions_run")),
                    fmt_count(metrics.get("frontier_undo_records")),
                    fmt_count(metrics.get("frontier_full_checkpoint_fallbacks")),
                ]
            )
    return markdown_table(
        [
            "scenario",
            "engine",
            "cache_hits",
            "cache_misses",
            "reloads",
            "wal_bytes",
            "wal_sync",
            "mmap_flush",
            "compactions",
            "undo_records",
            "checkpoint_fallbacks",
        ],
        rows,
    )


def render_markdown(paths: list[Path], hide_memory: bool) -> str:
    reports = load_reports(paths)
    engines = available_engines(reports, hide_memory)
    sections = [
        "# Perf Matrix Summary",
        "",
        "Bold values mark the best result in each comparable performance row.",
        "",
        "## Throughput (ops/s, higher is better)",
        throughput_table(reports, engines),
        "",
        "## Relative Throughput (x, engine / borromean, higher is better)",
        relative_throughput_table(reports, engines),
        "",
        "## Latency P50 (time/op, lower is better)",
        latency_table(reports, engines, "p50"),
        "",
        "## Latency P95 (time/op, lower is better)",
        latency_table(reports, engines, "p95"),
        "",
        "## Latency P99 (time/op, lower is better)",
        latency_table(reports, engines, "p99"),
        "",
        "## Operation Counts (operations)",
        operation_counts_table(reports, engines),
        "",
        "## Disk And IO (bytes)",
        disk_io_table(reports, engines),
        "",
        "## Durability Cost (time and bytes)",
        durability_table(reports, engines),
        "",
        "## Borromean Internals (counts, bytes, and time)",
        borromean_internals_table(reports, engines),
        "",
    ]
    return "\n".join(sections)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("json_paths", nargs="+", type=Path)
    parser.add_argument("--include-memory", action="store_true")
    parser.add_argument("--hide-memory", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    markdown = render_markdown(args.json_paths, hide_memory=args.hide_memory or not args.include_memory)
    output = markdown if markdown.endswith("\n") else f"{markdown}\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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


def base_scenario_label(path: Path) -> str:
    stem = path.stem
    if stem.endswith("_4k"):
        stem = stem[:-3]
    if stem.startswith("file_backing_"):
        return stem[len("file_backing_") :]
    if stem == "file_backing":
        return "insert"
    return stem


def fmt_region_size(value: object) -> str | None:
    if value is None:
        return None
    try:
        size = int(value)
    except (TypeError, ValueError):
        return None
    if size <= 0:
        return None
    units = [
        (1024 * 1024 * 1024, "GiB"),
        (1024 * 1024, "MiB"),
        (1024, "KiB"),
    ]
    for divisor, unit in units:
        if size % divisor == 0:
            return f"{size // divisor}{unit}"
    return f"{size}B"


def report_region_size(data: dict[str, Any]) -> object:
    geometry = data.get("geometry") or {}
    if geometry.get("region_size") is not None:
        return geometry.get("region_size")
    config_geometry = (data.get("config") or {}).get("geometry") or {}
    return config_geometry.get("region_size")


def scenario_label(path: Path, data: dict[str, Any] | None = None) -> str:
    label = base_scenario_label(path)
    region = fmt_region_size(report_region_size(data)) if data is not None else None
    if region is None and path.stem.endswith("_4k"):
        region = "4KiB"
    return f"{label} / {region}" if region is not None else label


def report_region_label(path: Path, data: dict[str, Any] | None = None) -> str | None:
    region = fmt_region_size(report_region_size(data)) if data is not None else None
    if region is None and path.stem.endswith("_4k"):
        return "4KiB"
    return region


def load_reports(paths: list[Path]) -> list[dict[str, Any]]:
    reports = []
    for path in paths:
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            reports.append(
                {
                    "scenario": base_scenario_label(path),
                    "region": report_region_label(path),
                    "path": path,
                    "missing": True,
                }
            )
            continue
        reports.append(
            {
                "scenario": base_scenario_label(path),
                "region": report_region_label(path, data),
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


def grouped_reports(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    by_scenario: dict[str, dict[str, Any]] = {}
    for report in reports:
        scenario = report["scenario"]
        group = by_scenario.get(scenario)
        if group is None:
            group = {"scenario": scenario, "reports": []}
            by_scenario[scenario] = group
            groups.append(group)
        group["reports"].append(report)
    return groups


def region_order(reports: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    seen = set()
    for report in reports:
        label = report.get("region")
        if label is None or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return labels


def comparison_engines(reports: list[dict[str, Any]]) -> list[str]:
    seen = set()
    for report in reports:
        seen.update(report.get("engines", {}).keys())
    excluded = {"borromean", "borromean-memory"}
    engines = [engine for engine in DEFAULT_ENGINE_ORDER if engine in seen and engine not in excluded]
    engines.extend(sorted(seen.difference(DEFAULT_ENGINE_ORDER).difference(excluded)))
    return engines


def report_for_region(group: dict[str, Any], region: str) -> dict[str, Any] | None:
    for report in group.get("reports", []):
        if report.get("region") == region:
            return report
    return None


def first_report_with_engine(group: dict[str, Any], engine: str) -> dict[str, Any] | None:
    for report in group.get("reports", []):
        if engine_report(report, engine):
            return report
    return None


def matrix_headers(regions: list[str], engines: list[str], *, include_memory: bool) -> list[str]:
    headers = [f"borromean {region}" for region in regions]
    if include_memory:
        headers.extend(f"borromean-memory {region}" for region in regions)
    headers.extend(engines)
    return headers


def matrix_items(
    group: dict[str, Any],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> list[tuple[str, dict[str, Any] | None]]:
    items: list[tuple[str, dict[str, Any] | None]] = []
    for region in regions:
        report = report_for_region(group, region)
        items.append((f"borromean {region}", engine_report(report, "borromean") if report else None))
    if include_memory:
        for region in regions:
            report = report_for_region(group, region)
            items.append(
                (
                    f"borromean-memory {region}",
                    engine_report(report, "borromean-memory") if report else None,
                )
            )
    for engine in engines:
        report = first_report_with_engine(group, engine)
        items.append((engine, engine_report(report, engine) if report else None))
    return items


def base_borromean_report(group: dict[str, Any], regions: list[str]) -> dict[str, Any] | None:
    for region in regions:
        report = report_for_region(group, region)
        item = engine_report(report, "borromean") if report else None
        if item:
            return item
    return None


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


def throughput_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> str:
    rows = []
    headers = matrix_headers(regions, engines, include_memory=include_memory)
    for group in groups:
        row = [group["scenario"]]
        items = matrix_items(group, regions, engines, include_memory=include_memory)
        values = [
            float(item.get("operations_per_second"))
            for _, item in items
            if item and item.get("operations_per_second") is not None
        ]
        best = best_number(values, higher_is_better=True)
        for _, item in items:
            value = float(item.get("operations_per_second")) if item else None
            row.append(fmt_best_number(value, best, fmt_float))
        rows.append(row)
    return markdown_table(["scenario", *headers], rows)


def relative_throughput_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> str:
    rows = []
    headers = matrix_headers(regions, engines, include_memory=include_memory)
    for group in groups:
        base = base_borromean_report(group, regions)
        base_ops = float(base.get("operations_per_second", 0)) if base else 0.0
        row = [group["scenario"]]
        items = matrix_items(group, regions, engines, include_memory=include_memory)
        ratios = []
        for _, item in items:
            if item and base_ops > 0:
                ratios.append(float(item.get("operations_per_second", 0)) / base_ops)
        best = best_number(ratios, higher_is_better=True)
        for _, item in items:
            if not item or base_ops <= 0:
                row.append("-")
                continue
            ratio = float(item.get("operations_per_second", 0)) / base_ops
            row.append(fmt_best_number(ratio, best, fmt_ratio))
        rows.append(row)
    return markdown_table(["scenario", *headers], rows)


def latency_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    percentile: str,
    *,
    include_memory: bool,
) -> str:
    key = f"{percentile}_nanos"
    rows = []
    headers = matrix_headers(regions, engines, include_memory=include_memory)
    for group in groups:
        row = [group["scenario"]]
        items = matrix_items(group, regions, engines, include_memory=include_memory)
        values = []
        for _, item in items:
            latency = item.get("sampled_latency") if item else None
            if latency and latency.get(key) is not None:
                values.append(float(latency.get(key)))
        best = best_number(values, higher_is_better=False)
        for _, item in items:
            latency = item.get("sampled_latency") if item else None
            value = int(latency.get(key)) if latency and latency.get(key) is not None else None
            row.append(fmt_best_number(value, best, fmt_nanos))
        rows.append(row)
    return markdown_table(["scenario", *headers], rows)


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


def disk_io_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> str:
    rows = []
    for group in groups:
        for label, item in matrix_items(group, regions, engines, include_memory=include_memory):
            if not item:
                rows.append([group["scenario"], label, "-", "-", "-", "-", "-"])
                continue
            diagnostics = item.get("diagnostics") or {}
            process_io = diagnostics.get("workload_process_io") or {}
            rows.append(
                [
                    group["scenario"],
                    label,
                    fmt_bytes(item.get("logical_len_bytes")),
                    fmt_bytes(item.get("file_len_bytes")),
                    fmt_bytes(diagnostics.get("fjall_path_size_bytes")),
                    fmt_bytes(process_io.get("read_bytes")),
                    fmt_bytes(process_io.get("write_bytes")),
                ]
            )
    return markdown_table(
        ["scenario", "target", "logical", "file_len", "path_size", "process_read", "process_write"],
        rows,
    )


def borromean_sync_count(item: dict[str, Any]) -> object:
    metrics = item.get("borromean_core_metrics") or {}
    audit = item.get("sync_audit") or {}
    return metrics.get("wal_syncs", audit.get("wal_syncs"))


def durability_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> str:
    rows = []
    for group in groups:
        for label, item in matrix_items(group, regions, engines, include_memory=include_memory):
            if not item:
                rows.append([group["scenario"], label, "-", "-", "-", "-", "-"])
                continue
            diagnostics = item.get("diagnostics") or {}
            metrics = item.get("borromean_core_metrics") or {}
            engine = item.get("engine")
            if str(engine).startswith("borromean"):
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
                    group["scenario"],
                    label,
                    str(mode),
                    fmt_count(count),
                    fmt_nanos(time),
                    fmt_nanos(time_number // count_number) if count_number > 0 else "-",
                    fmt_bytes((metrics.get("wal_bytes") if str(engine).startswith("borromean") else None)),
                ]
            )
    return markdown_table(
        ["scenario", "target", "mode", "count", "time", "time/op", "wal_bytes"],
        rows,
    )


def borromean_internals_table(
    groups: list[dict[str, Any]],
    regions: list[str],
    engines: list[str],
    *,
    include_memory: bool,
) -> str:
    rows = []
    for group in groups:
        for label, item in matrix_items(group, regions, engines, include_memory=include_memory):
            if not label.startswith("borromean"):
                continue
            if not item:
                rows.append([group["scenario"], label, "-", "-", "-", "-", "-", "-", "-", "-", "-"])
                continue
            metrics = item.get("borromean_core_metrics") or {}
            rows.append(
                [
                    group["scenario"],
                    label,
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
            "target",
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
    groups = grouped_reports(reports)
    regions = region_order(reports)
    engines = comparison_engines(reports)
    include_memory = not hide_memory
    sections = [
        "# Perf Matrix Summary",
        "",
        "Bold values mark the best result in each comparable performance row.",
        "Borromean geometry is shown as separate Borromean columns; redb and Fjall are shown once",
        "per workload because Borromean region size does not apply to those engines.",
        "",
        "## Throughput (ops/s, higher is better)",
        throughput_table(groups, regions, engines, include_memory=include_memory),
        "",
        "## Relative Throughput (x, engine / borromean 1MiB when present, higher is better)",
        relative_throughput_table(groups, regions, engines, include_memory=include_memory),
        "",
        "## Latency P50 (time/op, lower is better)",
        latency_table(groups, regions, engines, "p50", include_memory=include_memory),
        "",
        "## Latency P95 (time/op, lower is better)",
        latency_table(groups, regions, engines, "p95", include_memory=include_memory),
        "",
        "## Latency P99 (time/op, lower is better)",
        latency_table(groups, regions, engines, "p99", include_memory=include_memory),
        "",
        "## Disk And IO (bytes)",
        disk_io_table(groups, regions, engines, include_memory=include_memory),
        "",
        "## Durability Cost (time and bytes)",
        durability_table(groups, regions, engines, include_memory=include_memory),
        "",
        "## Borromean Internals (counts, bytes, and time)",
        borromean_internals_table(groups, regions, engines, include_memory=include_memory),
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

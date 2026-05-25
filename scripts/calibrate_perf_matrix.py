#!/usr/bin/env python3
"""Run perf configs at several operation counts and summarize throughput stability."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CONFIGS = [
    Path("perf/file_backing.toml"),
    Path("perf/file_backing_4k.toml"),
    Path("perf/file_backing_update_hot.toml"),
    Path("perf/file_backing_update_hot_4k.toml"),
    Path("perf/file_backing_read_hits.toml"),
    Path("perf/file_backing_read_hits_4k.toml"),
    Path("perf/file_backing_read_misses.toml"),
    Path("perf/file_backing_read_misses_4k.toml"),
    Path("perf/file_backing_mixed_update.toml"),
    Path("perf/file_backing_mixed_update_4k.toml"),
]


@dataclass(frozen=True)
class RunSpec:
    source_config: Path
    scenario: str
    region: str
    kind: str
    operation_count: int
    repeat: int
    config_path: Path
    json_path: Path
    log_path: Path
    engines_override: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ThroughputSample:
    scenario: str
    region: str
    kind: str
    operation_count: int
    engine: str
    operations_per_second: float


@dataclass(frozen=True)
class ThroughputStats:
    scenario: str
    region: str
    kind: str
    operation_count: int
    engine: str
    runs: int
    median: float
    minimum: float
    maximum: float
    relative_stdev: float | None
    relative_mad: float | None


def fmt_float(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def fmt_percent(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.2f}%"


def fmt_count(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def parse_counts(text: str) -> list[int]:
    counts = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        count = int(item)
        if count <= 0:
            raise ValueError(f"operation counts must be positive: {count}")
        counts.append(count)
    if not counts:
        raise ValueError("at least one operation count is required")
    return sorted(dict.fromkeys(counts))


def base_scenario_label(path: Path) -> str:
    stem = path.stem
    if stem.endswith("_4k"):
        stem = stem[:-3]
    if stem.startswith("file_backing_"):
        return stem[len("file_backing_") :]
    if stem == "file_backing":
        return "insert"
    return stem


def fmt_region_size(value: object) -> str:
    try:
        size = int(value)
    except (TypeError, ValueError):
        return "unknown"
    for divisor, unit in ((1024 * 1024, "MiB"), (1024, "KiB")):
        if size > 0 and size % divisor == 0:
            return f"{size // divisor}{unit}"
    return f"{size}B"


def region_slug(region: str) -> str:
    return region.lower().replace(" ", "").replace(",", "").replace("/", "-")


def load_config(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def workload_kind(config: dict[str, Any]) -> str:
    workload = config.get("workload") or {}
    read_ratio = int(workload.get("read_ratio") or 0)
    set_ratio = int(workload.get("set_ratio") or 0)
    delete_ratio = int(workload.get("delete_ratio") or 0)
    if read_ratio > 0 and set_ratio == 0 and delete_ratio == 0:
        return "read"
    if read_ratio > 0 and (set_ratio > 0 or delete_ratio > 0):
        return "mixed"
    return "write"


def configured_engines(config: dict[str, Any]) -> list[str]:
    comparison = config.get("comparison") or {}
    engines = comparison.get("engines") or ["borromean"]
    return [str(engine) for engine in engines]


def borromean_engines(config: dict[str, Any]) -> tuple[str, ...]:
    engines = [engine for engine in configured_engines(config) if engine.startswith("borromean")]
    return tuple(engines or ["borromean"])


def geometry_label(config: dict[str, Any]) -> str:
    geometry = config.get("geometry") or {}
    size = fmt_region_size(geometry.get("region_size"))
    try:
        count = int(geometry.get("region_count"))
    except (TypeError, ValueError):
        return size
    return f"{size} x {count:,}"


def scenario_region(config_path: Path, config: dict[str, Any]) -> tuple[str, str]:
    return base_scenario_label(config_path), geometry_label(config)


def counts_for_kind(
    kind: str,
    *,
    counts: list[int] | None,
    read_counts: list[int],
    write_counts: list[int],
    mixed_counts: list[int],
) -> list[int]:
    if counts is not None:
        return counts
    if kind == "read":
        return read_counts
    if kind == "mixed":
        return mixed_counts
    return write_counts


def replace_config_line(
    line: str,
    *,
    section: str | None,
    spec: RunSpec,
    output_dir: Path,
) -> str:
    stripped = line.strip()
    if section == "workload" and stripped.startswith("operation_count"):
        return f"operation_count = {spec.operation_count}\n"
    if section == "comparison" and stripped.startswith("engines") and spec.engines_override is not None:
        engines = ", ".join(f'"{engine}"' for engine in spec.engines_override)
        return f"engines = [{engines}]\n"
    if section == "output" and stripped.startswith("json_path"):
        return f'json_path = "{spec.json_path.as_posix()}"\n'
    if section == "output" and stripped.startswith("latency_sample_interval"):
        return "latency_sample_interval = 0\n"
    if section == "output" and stripped.startswith("progress_interval"):
        return "progress_interval = 0\n"
    if section in {"backing", "redb", "fjall"} and stripped.startswith("path"):
        path = output_dir / "db" / (
            f"{spec.source_config.stem}_{region_slug(spec.region)}_{section}"
            f"_ops{spec.operation_count}_run{spec.repeat}.db"
        )
        return f'path = "{path.as_posix()}"\n'
    if section in {"backing", "redb", "fjall"} and stripped.startswith("remove_after"):
        return "remove_after = true\n"
    return line


def write_calibrated_config(source: Path, spec: RunSpec, output_dir: Path) -> None:
    section: str | None = None
    lines = []
    for line in source.read_text().splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped.strip("[]")
        lines.append(replace_config_line(line, section=section, spec=spec, output_dir=output_dir))
    spec.config_path.parent.mkdir(parents=True, exist_ok=True)
    spec.config_path.write_text("".join(lines))


def build_run_specs(
    configs: list[Path],
    *,
    output_dir: Path,
    repeats: int,
    counts: list[int] | None,
    read_counts: list[int],
    write_counts: list[int],
    mixed_counts: list[int],
    dedupe_comparison_engines: bool = True,
) -> list[RunSpec]:
    specs = []
    seen_scenarios: set[str] = set()
    for config_path in configs:
        config = load_config(config_path)
        scenario, region = scenario_region(config_path, config)
        kind = workload_kind(config)
        engines_override = None
        if dedupe_comparison_engines and scenario in seen_scenarios:
            engines_override = borromean_engines(config)
        seen_scenarios.add(scenario)
        for operation_count in counts_for_kind(
            kind,
            counts=counts,
            read_counts=read_counts,
            write_counts=write_counts,
            mixed_counts=mixed_counts,
        ):
            for repeat in range(1, repeats + 1):
                name = f"{config_path.stem}__ops{operation_count}__run{repeat}"
                specs.append(
                    RunSpec(
                        source_config=config_path,
                        scenario=scenario,
                        region=region,
                        kind=kind,
                        operation_count=operation_count,
                        repeat=repeat,
                        config_path=output_dir / "configs" / f"{name}.toml",
                        json_path=output_dir / "json" / f"{name}.json",
                        log_path=output_dir / "logs" / f"{name}.log",
                        engines_override=engines_override,
                    )
                )
    return specs


def run_specs(specs: list[RunSpec], *, binary: Path, output_dir: Path, dry_run: bool) -> list[Path]:
    json_paths = []
    for spec in specs:
        write_calibrated_config(spec.source_config, spec, output_dir)
        json_paths.append(spec.json_path)
        print(
            "[perf-calibrate]"
            f" {spec.source_config} {spec.region} ops={spec.operation_count}"
            f" repeat={spec.repeat} -> {spec.json_path}"
        )
        if dry_run:
            continue
        spec.log_path.parent.mkdir(parents=True, exist_ok=True)
        with spec.log_path.open("w") as log:
            subprocess.run(
                [str(binary), "--config", str(spec.config_path)],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    return json_paths


def stem_from_calibration_json(path: Path) -> str:
    stem = path.stem
    marker = "__ops"
    if marker not in stem:
        return stem
    return stem.split(marker, 1)[0]


def load_samples(paths: list[Path]) -> list[ThroughputSample]:
    samples = []
    for path in paths:
        if not path.exists():
            continue
        data = json.loads(path.read_text())
        source_stem = stem_from_calibration_json(path)
        config = data.get("config") or {}
        scenario = base_scenario_label(Path(source_stem))
        region = geometry_label(config)
        kind = workload_kind(config)
        operation_count = int((config.get("workload") or {}).get("operation_count") or 0)
        for report in data.get("engine_reports", []):
            ops = report.get("operations_per_second")
            engine = report.get("engine")
            if engine is None or ops is None:
                continue
            samples.append(
                ThroughputSample(
                    scenario=scenario,
                    region=region,
                    kind=kind,
                    operation_count=operation_count,
                    engine=str(engine),
                    operations_per_second=float(ops),
                )
            )
    return samples


def summarize_samples(samples: list[ThroughputSample]) -> list[ThroughputStats]:
    grouped: dict[tuple[str, str, str, int, str], list[float]] = {}
    for sample in samples:
        key = (
            sample.scenario,
            sample.region,
            sample.kind,
            sample.operation_count,
            sample.engine,
        )
        grouped.setdefault(key, []).append(sample.operations_per_second)
    stats = []
    for (scenario, region, kind, operation_count, engine), values in grouped.items():
        values = sorted(values)
        median = statistics.median(values)
        relative_stdev = None
        if len(values) > 1 and median != 0:
            relative_stdev = statistics.stdev(values) / median
        relative_mad = None
        if median != 0:
            relative_mad = statistics.median(abs(value - median) for value in values) / median
        stats.append(
            ThroughputStats(
                scenario=scenario,
                region=region,
                kind=kind,
                operation_count=operation_count,
                engine=engine,
                runs=len(values),
                median=median,
                minimum=min(values),
                maximum=max(values),
                relative_stdev=relative_stdev,
                relative_mad=relative_mad,
            )
        )
    return sorted(stats, key=lambda item: (item.scenario, item.region, item.operation_count, item.engine))


def threshold_for_kind(kind: str, *, read_threshold: float, write_threshold: float, mixed_threshold: float) -> float:
    if kind == "read":
        return read_threshold
    if kind == "mixed":
        return mixed_threshold
    return write_threshold


def recommendation_rows(
    stats: list[ThroughputStats],
    *,
    repeats: int,
    read_threshold: float,
    write_threshold: float,
    mixed_threshold: float,
) -> list[list[str]]:
    scenario_keys = sorted({(item.scenario, item.region, item.kind) for item in stats})
    rows = []
    for scenario, region, kind in scenario_keys:
        threshold = threshold_for_kind(
            kind,
            read_threshold=read_threshold,
            write_threshold=write_threshold,
            mixed_threshold=mixed_threshold,
        )
        scenario_stats = [
            item for item in stats if (item.scenario, item.region, item.kind) == (scenario, region, kind)
        ]
        counts = sorted({item.operation_count for item in scenario_stats})
        chosen_count: int | None = None
        chosen_spread: float | None = None
        status = "needs larger run"
        for count in counts:
            count_stats = [item for item in scenario_stats if item.operation_count == count]
            spreads = [
                item.relative_mad
                for item in count_stats
                if item.runs >= repeats and item.relative_mad is not None
            ]
            if len(spreads) != len(count_stats) or not spreads:
                continue
            max_spread = max(spreads)
            if max_spread <= threshold:
                chosen_count = count
                chosen_spread = max_spread
                status = "stable"
                break
        if chosen_count is None and counts:
            chosen_count = counts[-1]
            count_stats = [item for item in scenario_stats if item.operation_count == chosen_count]
            spreads = [item.relative_mad for item in count_stats if item.relative_mad is not None]
            chosen_spread = max(spreads) if spreads else None
        rows.append(
            [
                f"{scenario} / {region}",
                kind,
                fmt_percent(threshold),
                fmt_count(chosen_count),
                fmt_percent(chosen_spread),
                status,
            ]
        )
    return rows


def detail_rows(stats: list[ThroughputStats]) -> list[list[str]]:
    rows = []
    for item in stats:
        rows.append(
            [
                f"{item.scenario} / {item.region}",
                fmt_count(item.operation_count),
                item.engine,
                fmt_count(item.runs),
                fmt_float(item.median),
                fmt_percent(item.relative_stdev),
                fmt_percent(item.relative_mad),
                fmt_float(item.minimum),
                fmt_float(item.maximum),
            ]
        )
    return rows


def render_summary(
    stats: list[ThroughputStats],
    *,
    repeats: int,
    read_threshold: float,
    write_threshold: float,
    mixed_threshold: float,
) -> str:
    sections = [
        "# Perf Calibration Summary",
        "",
        "Relative MAD is the median absolute deviation divided by median throughput.",
        "Use the smallest operation count marked stable for each scenario, then rerun the matrix",
        "with median-of-repeats reporting if the benchmark document needs publication-quality numbers.",
        "",
        "## Recommended Operation Counts",
        markdown_table(
            ["scenario", "kind", "threshold", "recommended operations", "max rel MAD", "status"],
            recommendation_rows(
                stats,
                repeats=repeats,
                read_threshold=read_threshold,
                write_threshold=write_threshold,
                mixed_threshold=mixed_threshold,
            ),
        ),
        "",
        "## Stability Details",
        markdown_table(
            [
                "scenario",
                "operations",
                "engine",
                "runs",
                "median ops/s",
                "rel stdev",
                "rel MAD",
                "min ops/s",
                "max ops/s",
            ],
            detail_rows(stats),
        ),
        "",
    ]
    return "\n".join(sections)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("configs", nargs="*", type=Path, default=DEFAULT_CONFIGS)
    parser.add_argument("--binary", type=Path, default=Path("target/release/file_backing_perf"))
    parser.add_argument("--output-dir", type=Path, default=Path("target/perf/calibration"))
    parser.add_argument("--summary", type=Path, default=Path("target/perf/perf_calibration_summary.md"))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--counts", help="Comma-separated operation counts for all workload kinds")
    parser.add_argument("--read-counts", default="3000,10000,30000,100000,300000")
    parser.add_argument("--write-counts", default="3000,10000")
    parser.add_argument("--mixed-counts", default="3000,10000")
    parser.add_argument("--read-threshold", type=float, default=0.03)
    parser.add_argument("--write-threshold", type=float, default=0.05)
    parser.add_argument("--mixed-threshold", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--comparison-every-geometry",
        action="store_true",
        help="Run redb/Fjall for every geometry config instead of once per workload",
    )
    parser.add_argument(
        "--summarize-only",
        action="store_true",
        help="Summarize existing calibration JSON files under --output-dir/json",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.repeats <= 0:
        raise SystemExit("--repeats must be positive")
    counts = parse_counts(args.counts) if args.counts else None
    read_counts = parse_counts(args.read_counts)
    write_counts = parse_counts(args.write_counts)
    mixed_counts = parse_counts(args.mixed_counts)

    if args.summarize_only:
        json_paths = sorted((args.output_dir / "json").glob("*.json"))
    else:
        configs = args.configs if args.configs else DEFAULT_CONFIGS
        specs = build_run_specs(
            configs,
            output_dir=args.output_dir,
            repeats=args.repeats,
            counts=counts,
            read_counts=read_counts,
            write_counts=write_counts,
            mixed_counts=mixed_counts,
            dedupe_comparison_engines=not args.comparison_every_geometry,
        )
        json_paths = run_specs(specs, binary=args.binary, output_dir=args.output_dir, dry_run=args.dry_run)

    if args.dry_run:
        return 0

    samples = load_samples(json_paths)
    stats = summarize_samples(samples)
    summary = render_summary(
        stats,
        repeats=args.repeats,
        read_threshold=args.read_threshold,
        write_threshold=args.write_threshold,
        mixed_threshold=args.mixed_threshold,
    )
    output = summary if summary.endswith("\n") else f"{summary}\n"
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(output)
    sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

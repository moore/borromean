#!/usr/bin/env python3
"""Tests for summarize_perf_matrix_jsons.py."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("summarize_perf_matrix_jsons.py")
SPEC = importlib.util.spec_from_file_location("summarize_perf_matrix_jsons", SCRIPT_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def engine(
    name: str,
    ops: float,
    *,
    p50: int | None = 1000,
    write_bytes: int = 0,
) -> dict[str, object]:
    return {
        "engine": name,
        "operations_per_second": ops,
        "counters": {
            "reads": 1,
            "sets": 2,
            "set_inserts_expected": 1,
            "set_updates_expected": 1,
            "deletes": 3,
            "hits": 4,
            "misses": 5,
        },
        "sampled_latency": None
        if p50 is None
        else {
            "samples": 1,
            "min_nanos": p50,
            "p50_nanos": p50,
            "p95_nanos": p50 * 2,
            "p99_nanos": p50 * 3,
            "max_nanos": p50 * 3,
        },
        "diagnostics": {
            "workload_process_io": {
                "read_bytes": 0,
                "write_bytes": write_bytes,
            },
            "commit_count": 2,
            "commit_nanos": 20_000,
            "fjall_persist_count": 2,
            "fjall_persist_nanos": 30_000,
            "fjall_persist_mode": "sync-data",
            "fjall_path_size_bytes": 4096,
        },
        "borromean_core_metrics": {
            "frontier_cache_hits": 10,
            "frontier_cache_misses": 1,
            "frontier_reloads": 1,
            "wal_bytes": 2048,
            "wal_syncs": 2,
            "wal_sync_nanos": 10_000,
            "mmap_flush_nanos": 9000,
            "compactions_run": 0,
            "frontier_undo_records": 2,
            "frontier_full_checkpoint_fallbacks": 0,
        }
        if name.startswith("borromean")
        else None,
        "sync_audit": {"wal_syncs": 2},
        "file_len_bytes": 8192,
        "logical_len_bytes": 8192,
    }


class PerfMatrixSummaryTests(unittest.TestCase):
    def write_report(
        self,
        directory: Path,
        name: str,
        reports: list[dict[str, object]],
        *,
        region_size: int = 1_048_576,
    ) -> Path:
        path = directory / name
        path.write_text(
            json.dumps(
                {
                    "config": {"geometry": {"region_size": region_size}},
                    "engine_reports": reports,
                }
            )
        )
        return path

    def test_formats_throughput_and_ratios(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_report(
                Path(temp),
                "file_backing_update_hot.json",
                [
                    engine("borromean", 100.0),
                    engine("borromean-memory", 400.0),
                    engine("redb", 50.0),
                    engine("fjall", 200.0),
                ],
            )

            markdown = MODULE.render_markdown([path], hide_memory=True)

        self.assertIn("## Throughput (ops/s, higher is better)", markdown)
        self.assertIn("| update_hot | 100.0 | 50.0 | **200.0** |", markdown)
        self.assertIn("| update_hot | 1.00x | 0.50x | **2.00x** |", markdown)

    def test_hides_memory_by_default_and_includes_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_report(
                Path(temp),
                "file_backing.json",
                [engine("borromean", 100.0), engine("borromean-memory", 300.0)],
            )

            default_markdown = MODULE.render_markdown([path], hide_memory=True)
            included_markdown = MODULE.render_markdown([path], hide_memory=False)

        self.assertIn("| scenario | borromean 1MiB |", default_markdown)
        self.assertNotIn("borromean-memory", default_markdown)
        self.assertIn("| scenario | borromean 1MiB | borromean-memory 1MiB |", included_markdown)

    def test_missing_latency_and_diagnostics_render_as_dash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_report(
                Path(temp),
                "file_backing_read_hits.json",
                [
                    {
                        "engine": "borromean",
                        "operations_per_second": 10.0,
                        "counters": {},
                        "sampled_latency": None,
                        "diagnostics": {},
                        "file_len_bytes": 0,
                        "logical_len_bytes": 0,
                    }
                ],
            )

            markdown = MODULE.render_markdown([path], hide_memory=True)

        self.assertIn("| read_hits | - |", markdown)
        self.assertIn("| read_hits | borromean 1MiB | 0B | 0B | - | - | - |", markdown)

    def test_latency_bolds_lowest_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_report(
                Path(temp),
                "file_backing_read_hits.json",
                [
                    engine("borromean", 100.0, p50=3000),
                    engine("redb", 100.0, p50=1000),
                    engine("fjall", 100.0, p50=2000),
                ],
            )

            markdown = MODULE.render_markdown([path], hide_memory=True)

        self.assertIn("| read_hits | 3.000us | **1.000us** | 2.000us |", markdown)

    def test_uses_region_size_in_scenario_label(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_report(
                Path(temp),
                "file_backing_read_hits_4k.json",
                [engine("borromean", 100.0)],
                region_size=4096,
            )

            markdown = MODULE.render_markdown([path], hide_memory=True)

        self.assertIn("| scenario | borromean 4KiB |", markdown)
        self.assertIn("| read_hits | **100.0** |", markdown)

    def test_groups_regions_as_columns_and_uses_one_comparison_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            path_1m = self.write_report(
                directory,
                "file_backing.json",
                [
                    engine("borromean", 100.0),
                    engine("redb", 50.0),
                    engine("fjall", 200.0),
                ],
            )
            path_4k = self.write_report(
                directory,
                "file_backing_4k.json",
                [
                    engine("borromean", 25.0),
                    engine("redb", 999.0),
                    engine("fjall", 999.0),
                ],
                region_size=4096,
            )

            markdown = MODULE.render_markdown([path_1m, path_4k], hide_memory=True)

        self.assertIn("| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |", markdown)
        self.assertIn("| insert | 100.0 | 25.0 | 50.0 | **200.0** |", markdown)
        self.assertNotIn("999.0", markdown)

    def test_output_file_matches_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            path = self.write_report(directory, "file_backing.json", [engine("borromean", 100.0)])
            output_path = directory / "summary.md"
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                result = MODULE.main([str(path), "--output", str(output_path)])

            printed = stdout.getvalue()
            written = output_path.read_text()

        self.assertEqual(0, result)
        self.assertEqual(written, printed)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Tests for calibrate_perf_matrix.py."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("calibrate_perf_matrix.py")
SPEC = importlib.util.spec_from_file_location("calibrate_perf_matrix", SCRIPT_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_config(path: Path) -> None:
    path.write_text(
        """
[geometry]
region_size = 4096
region_count = 4096

[comparison]
engines = ["borromean", "redb"]

[backing]
path = "target/perf/file_backing_read_hits_4k.db"
remove_existing = true
remove_after = false

[redb]
path = "target/perf/redb_read_hits_4k.db"
remove_existing = true
remove_after = false

[fjall]
path = "target/perf/fjall_read_hits_4k.db"
remove_existing = true
remove_after = false

[workload]
operation_count = 3000
read_ratio = 100
set_ratio = 0
delete_ratio = 0

[output]
json_path = "target/perf/file_backing_read_hits_4k.json"
latency_sample_interval = 250
progress_interval = 500
""".lstrip()
    )


def report_json(operation_count: int, borromean_ops: float, redb_ops: float) -> dict[str, object]:
    return {
        "config": {
            "geometry": {"region_size": 4096, "region_count": 4096},
            "workload": {
                "operation_count": operation_count,
                "read_ratio": 100,
                "set_ratio": 0,
                "delete_ratio": 0,
            },
        },
        "engine_reports": [
            {"engine": "borromean", "operations_per_second": borromean_ops},
            {"engine": "redb", "operations_per_second": redb_ops},
        ],
    }


class PerfCalibrationTests(unittest.TestCase):
    def test_calibrated_config_rewrites_run_specific_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            source = directory / "file_backing_read_hits_4k.toml"
            write_config(source)
            spec = MODULE.RunSpec(
                source_config=source,
                scenario="read_hits",
                region="4KiB x 4,096",
                kind="read",
                operation_count=100_000,
                repeat=2,
                config_path=directory / "out" / "config.toml",
                json_path=directory / "out" / "result.json",
                log_path=directory / "out" / "result.log",
            )

            MODULE.write_calibrated_config(source, spec, directory / "calibration")

            text = spec.config_path.read_text()

        self.assertIn("operation_count = 100000", text)
        self.assertIn(f'json_path = "{spec.json_path.as_posix()}"', text)
        self.assertIn("latency_sample_interval = 0", text)
        self.assertIn("progress_interval = 0", text)
        self.assertIn("remove_after = true", text)
        self.assertIn("file_backing_read_hits_4k_4kibx4096_backing_ops100000_run2.db", text)
        self.assertIn("file_backing_read_hits_4k_4kibx4096_redb_ops100000_run2.db", text)
        self.assertIn("file_backing_read_hits_4k_4kibx4096_fjall_ops100000_run2.db", text)

    def test_duplicate_geometry_configs_run_only_borromean_engines(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            one_mib = directory / "file_backing_read_hits.toml"
            four_kib = directory / "file_backing_read_hits_4k.toml"
            write_config(one_mib)
            write_config(four_kib)
            one_mib_text = one_mib.read_text().replace("region_size = 4096", "region_size = 1048576")
            one_mib.write_text(one_mib_text.replace("region_count = 4096", "region_count = 64"))

            specs = MODULE.build_run_specs(
                [one_mib, four_kib],
                output_dir=directory / "calibration",
                repeats=1,
                counts=[3000],
                read_counts=[3000],
                write_counts=[3000],
                mixed_counts=[3000],
            )

            first_spec = specs[0]
            second_spec = specs[1]
            MODULE.write_calibrated_config(first_spec.source_config, first_spec, directory / "calibration")
            MODULE.write_calibrated_config(second_spec.source_config, second_spec, directory / "calibration")

            first_text = first_spec.config_path.read_text()
            second_text = second_spec.config_path.read_text()

        self.assertIn('engines = ["borromean", "redb"]', first_text)
        self.assertIn('engines = ["borromean"]', second_text)

    def test_summarizes_samples_and_recommends_smallest_stable_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            paths = []
            values = {
                3_000: [(100.0, 50.0), (130.0, 80.0), (90.0, 30.0)],
                30_000: [(100.0, 50.0), (102.0, 51.0), (98.0, 49.0)],
            }
            for count, runs in values.items():
                for index, (borromean_ops, redb_ops) in enumerate(runs, start=1):
                    path = directory / f"file_backing_read_hits_4k__ops{count}__run{index}.json"
                    path.write_text(json.dumps(report_json(count, borromean_ops, redb_ops)))
                    paths.append(path)

            samples = MODULE.load_samples(paths)
            stats = MODULE.summarize_samples(samples)
            summary = MODULE.render_summary(
                stats,
                repeats=3,
                read_threshold=0.03,
                write_threshold=0.05,
                mixed_threshold=0.05,
            )

        self.assertIn("| read_hits / 4KiB x 4,096 | read | 3.00% | 30,000 | 2.00% | stable |", summary)
        self.assertIn("| read_hits / 4KiB x 4,096 | 3,000 | borromean | 3 | 100.0 |", summary)
        self.assertIn("| read_hits / 4KiB x 4,096 | 30,000 | redb | 3 | 50.0 |", summary)


if __name__ == "__main__":
    unittest.main()

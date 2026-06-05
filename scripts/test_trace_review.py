#!/usr/bin/env python3
"""Tests for trace_review.py."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("trace_review.py")
SPEC = importlib.util.spec_from_file_location("trace_review", SCRIPT_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_repo(root: Path) -> None:
    (root / "Cargo.toml").write_text("[package]\nname = \"demo\"\n", encoding="utf-8")
    (root / "src" / "foo").mkdir(parents=True)
    (root / "src" / "tests" / "traceability").mkdir(parents=True)
    (root / "spec").mkdir()
    (root / "src" / "foo.rs").write_text("pub fn value() -> u8 { 1 }\n", encoding="utf-8")
    (root / "src" / "lib.rs").write_text("mod tests;\n", encoding="utf-8")
    (root / "src" / "tests" / "mod.rs").write_text("mod traceability;\n", encoding="utf-8")
    (root / "src" / "tests" / "traceability.rs").write_text(
        "mod arch;\n", encoding="utf-8"
    )
    (root / "spec" / "foo.md").write_text(
        """# Demo Spec

## Runtime Behavior

1. `RING-DEMO-001` Runtime behavior MUST return the expected value.

### Ignored Child

This child should stay in the extracted section.

## Other Section

1. `RING-DEMO-002` Other behavior MUST exist.
""",
        encoding="utf-8",
    )
    (root / "src" / "foo" / "tests.rs").write_text(
        """use super::*;

//= spec/foo.md#runtime-behavior
//= type=test
//# `RING-DEMO-001` Runtime behavior MUST return the expected value.
#[test]
fn requirement_returns_expected_value() {
    let message = format!("value={}", 1);
    assert_eq!(message, "value=1");
    assert_eq!(crate::foo::value(), 1);
}

//= spec/foo.md#runtime-behavior
//= type=todo
//# `RING-DEMO-099` Future behavior MUST be reviewed later.
#[test]
fn todo_future_behavior() {
}
""",
        encoding="utf-8",
    )
    (root / "src" / "tests" / "traceability" / "arch.rs").write_text(
        """use super::*;

proptest! {
    //= spec/foo.md#runtime-behavior
    //= type=test
    //# `RING-DEMO-003` Property behavior MUST hold for generated inputs.
    #[test]
    fn requirement_property_behavior(
        value in 0u8..10
    ) {
        assert!(value < 10);
    }
}
""",
        encoding="utf-8",
    )


class TraceReviewTests(unittest.TestCase):
    def test_collects_requirement_and_todo_packets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)

            packets, errors = MODULE.collect_trace_packets(root)

        self.assertEqual(errors, [])
        self.assertEqual([packet.entry.trace_type for packet in packets], ["test", "todo", "test"])
        first = packets[0]
        self.assertEqual(first.entry.function, "requirement_returns_expected_value")
        self.assertIn("Runtime Behavior", first.spec_section)
        self.assertIn("Ignored Child", first.spec_section)
        self.assertNotIn("Other Section", first.spec_section)
        self.assertIn('format!("value={}", 1)', first.test_source)
        self.assertIn("src/foo.rs", first.entry.likely_entry_points)

        prop = packets[2]
        self.assertEqual(prop.entry.function, "requirement_property_behavior")
        self.assertIn("src/lib.rs", prop.entry.likely_entry_points)
        self.assertIn("src/tests/traceability.rs", prop.entry.likely_entry_points)

    def test_writes_inventory_packets_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)
            packets, errors = MODULE.collect_trace_packets(root)
            self.assertEqual(errors, [])
            output = root / "target" / "trace-review"

            MODULE.write_inventory(output, packets, packet_limit=1)
            written = MODULE.write_packets(root, output, packets, packet_limit=1)
            MODULE.write_reviewer_readme(output)

            inventory = json.loads((output / "inventory.json").read_text(encoding="utf-8"))
            self.assertEqual(inventory["requirement_tests"], 2)
            self.assertEqual(inventory["todo_tests"], 1)
            self.assertEqual(written, 1)
            packet_files = sorted((output / "packets").glob("*.md"))
            self.assertEqual(len(packet_files), 1)
            self.assertIn("Do not read `target/trace-review/results/`", packet_files[0].read_text())

            result_id = inventory["entries"][0]["test_id"]
            (output / "results" / f"{result_id}.json").write_text(
                json.dumps(
                    {
                        "test_id": result_id,
                        "verdict": "weak",
                        "rationale": "Only checks one value.",
                        "inspected_paths": ["src/foo.rs", "src/foo/tests.rs"],
                        "key_assertions": ["assert_eq!(crate::foo::value(), 1)"],
                        "missing_clauses": ["generated values"],
                        "suggested_improvement": "Add a boundary case.",
                    }
                ),
                encoding="utf-8",
            )

            summary = MODULE.render_summary(output)

        self.assertIn("Requirement tests inventoried: 2", summary)
        self.assertIn("weak: 1", summary)
        self.assertIn("Suggested improvement: Add a boundary case.", summary)

    def test_result_validation_requires_paths_and_suggestions(self) -> None:
        errors = MODULE.validate_result(
            "abc",
            {
                "test_id": "abc",
                "verdict": "fail",
                "rationale": "Wrong trace.",
                "inspected_paths": [],
                "key_assertions": [],
                "missing_clauses": [],
                "suggested_improvement": None,
            },
        )

        self.assertIn("abc: inspected_paths must not be empty for fail", errors)
        self.assertIn("abc: suggested_improvement is required for fail", errors)


if __name__ == "__main__":
    unittest.main()

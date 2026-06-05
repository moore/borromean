#!/usr/bin/env python3
"""Tests for trace_review.py."""

from __future__ import annotations

import importlib.util
import contextlib
import io
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
            (output / "results" / "stale_result.json").write_text(
                json.dumps(
                    {
                        "test_id": "stale_result",
                        "verdict": "fail",
                        "rationale": "This stale result should not affect the summary.",
                        "inspected_paths": ["src/stale.rs"],
                        "key_assertions": ["assert!(false)"],
                        "missing_clauses": ["all current clauses"],
                        "suggested_improvement": "Regenerate stale packets.",
                    }
                ),
                encoding="utf-8",
            )

            summary = MODULE.render_summary(output)

        self.assertIn("Requirement tests inventoried: 2", summary)
        self.assertIn("Stale result files ignored: 1", summary)
        self.assertIn("Result files reviewed: 1", summary)
        self.assertIn("fail: 0", summary)
        self.assertIn("weak: 1", summary)
        self.assertIn("Suggested improvement: Add a boundary case.", summary)
        self.assertIn("`stale_result`", summary)
        self.assertNotIn("This stale result should not affect the summary.", summary)

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

    def test_parse_json_response_accepts_fenced_json(self) -> None:
        parsed = MODULE.parse_json_response(
            """```json
{"test_id": "abc", "verdict": "pass"}
```"""
        )

        self.assertEqual(parsed["test_id"], "abc")
        self.assertEqual(parsed["verdict"], "pass")

    def test_new_status_lines_allows_review_output_only(self) -> None:
        before: set[str] = set()
        after = {
            "?? target/trace-review/results/abc.json",
            "?? src/new_file.rs",
        }

        added = MODULE.new_status_lines(
            before,
            after,
            Path("/repo"),
            Path("/repo/target/trace-review"),
        )

        self.assertEqual(added, ["?? src/new_file.rs"])

    def test_default_reviewer_effort_is_xhigh(self) -> None:
        parser = MODULE.build_parser()
        args = parser.parse_args(["review", "--dry-run"])

        self.assertEqual(args.effort, "xhigh")

    def test_review_cache_tracks_packet_and_dependency_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)
            packets, errors = MODULE.collect_trace_packets(root)
            self.assertEqual(errors, [])
            packet = packets[0]
            result = {
                "test_id": packet.entry.test_id,
                "verdict": "pass",
                "rationale": "The assertion checks the implementation value.",
                "inspected_paths": ["src/foo.rs", "src/foo/tests.rs"],
                "key_assertions": ["assert_eq!(crate::foo::value(), 1)"],
                "missing_clauses": [],
                "suggested_improvement": None,
            }
            cached = MODULE.attach_review_cache(root, packet, result)

            current, reason = MODULE.cached_result_status(root, packet, cached)
            self.assertTrue(current, reason)

            (root / "src" / "unrelated.rs").write_text("pub fn unrelated() {}\n")
            current, reason = MODULE.cached_result_status(root, packet, cached)
            self.assertTrue(current, reason)

            (root / "src" / "foo.rs").write_text(
                "pub fn value() -> u8 { 2 }\n", encoding="utf-8"
            )
            current, reason = MODULE.cached_result_status(root, packet, cached)
            self.assertFalse(current)
            self.assertEqual(reason, "review dependency changed")

            (root / "src" / "foo.rs").write_text(
                "pub fn value() -> u8 { 1 }\n", encoding="utf-8"
            )
            spec_text = (root / "spec" / "foo.md").read_text(encoding="utf-8")
            (root / "spec" / "foo.md").write_text(
                spec_text.replace("expected value", "expected runtime value"),
                encoding="utf-8",
            )
            new_packets, errors = MODULE.collect_trace_packets(root)
            self.assertEqual(errors, [])
            current, reason = MODULE.cached_result_status(root, new_packets[0], cached)
            self.assertFalse(current)
            self.assertEqual(reason, "packet changed")

    def test_needs_context_result_is_not_a_cache_hit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)
            packets, errors = MODULE.collect_trace_packets(root)
            self.assertEqual(errors, [])
            packet = packets[0]
            result = MODULE.attach_review_cache(
                root,
                packet,
                {
                    "test_id": packet.entry.test_id,
                    "verdict": "needs_context",
                    "rationale": "Could not inspect files.",
                    "inspected_paths": [],
                    "key_assertions": [],
                    "missing_clauses": ["source unavailable"],
                    "suggested_improvement": "Rerun with working file access.",
                },
            )

            current, reason = MODULE.cached_result_status(root, packet, result)

        self.assertFalse(current)
        self.assertEqual(reason, "needs_context result")

    def test_auto_sandbox_falls_back_when_codex_sandbox_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            script = root / "codex"
            script.write_text(
                "#!/usr/bin/env sh\n"
                "if [ \"$1\" = sandbox ]; then\n"
                "  echo 'bwrap: loopback: Failed RTM_NEWADDR' >&2\n"
                "  exit 1\n"
                "fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            script.chmod(0o755)

            sandbox, warning = MODULE.resolve_reviewer_sandbox(
                str(script), root, "auto", root / "codex-home"
            )

        self.assertEqual(sandbox, "danger-full-access")
        self.assertIsNotNone(warning)
        self.assertIn("bubblewrap sandbox is unavailable", warning)

    def test_review_dry_run_skips_current_cached_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)
            packets, errors = MODULE.collect_trace_packets(root)
            self.assertEqual(errors, [])
            output = root / "target" / "trace-review"
            MODULE.write_inventory(output, packets)
            MODULE.write_packets(root, output, packets)

            packet = packets[0]
            result = MODULE.attach_review_cache(
                root,
                packet,
                {
                    "test_id": packet.entry.test_id,
                    "verdict": "pass",
                    "rationale": "The assertion checks the implementation value.",
                    "inspected_paths": ["src/foo.rs", "src/foo/tests.rs"],
                    "key_assertions": ["assert_eq!(crate::foo::value(), 1)"],
                    "missing_clauses": [],
                    "suggested_improvement": None,
                },
            )
            (output / "results" / f"{packet.entry.test_id}.json").write_text(
                json.dumps(result), encoding="utf-8"
            )
            args = type(
                "Args",
                (),
                {
                    "repo_root": str(root),
                    "output_dir": "target/trace-review",
                    "codex_bin": sys.executable,
                    "skip_preflight": True,
                    "only": [packet.entry.test_id],
                    "resume": True,
                    "limit": None,
                    "dry_run": True,
                    "model": None,
                    "effort": "xhigh",
                    "reviewer_sandbox": "workspace-write",
                },
            )()
            output_text = io.StringIO()

            with contextlib.redirect_stdout(output_text):
                status = MODULE.command_review(args)

        self.assertEqual(status, 0)
        self.assertIn("skipped 1 unchanged review(s)", output_text.getvalue())
        self.assertIn("running 0 fresh review(s)", output_text.getvalue())

    def test_review_dry_run_selects_reviews_without_results(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_repo(root)
            args = type(
                "Args",
                (),
                {
                    "repo_root": str(root),
                    "output_dir": "target/trace-review",
                    "codex_bin": sys.executable,
                    "skip_preflight": True,
                    "only": [],
                    "resume": True,
                    "limit": 1,
                    "dry_run": True,
                    "model": None,
                    "effort": "xhigh",
                    "reviewer_sandbox": "workspace-write",
                },
            )()

            status = MODULE.command_review(args)

            output = root / "target" / "trace-review"
            inventory = json.loads((output / "inventory.json").read_text(encoding="utf-8"))
            result_files = list((output / "results").glob("*.json"))

        self.assertEqual(status, 0)
        self.assertEqual(inventory["requirement_tests"], 2)
        self.assertEqual(result_files, [])


if __name__ == "__main__":
    unittest.main()

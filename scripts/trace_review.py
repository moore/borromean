#!/usr/bin/env python3
"""Prepare and summarize fresh per-test traceability reviews."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


VALID_VERDICTS = {"pass", "weak", "fail", "needs_context"}
REVIEW_CACHE_VERSION = 1
DEFAULT_REVIEWER_EFFORT = "xhigh"
VALID_REVIEWER_EFFORTS = {"minimal", "low", "medium", "high", "xhigh"}
DEFAULT_REVIEWER_SANDBOX = "auto"
VALID_REVIEWER_SANDBOXES = {
    "auto",
    "read-only",
    "workspace-write",
    "danger-full-access",
}
REQUIRED_RESULT_FIELDS = {
    "test_id",
    "verdict",
    "rationale",
    "inspected_paths",
    "key_assertions",
    "missing_clauses",
    "suggested_improvement",
}
REVIEW_PROMPT = """You are performing one fresh semantic traceability review.

Read the packet at:
{packet_path}

Rules:
- Treat this as a new review with no prior findings, summaries, or assumptions.
- Inspect the full repository as needed to understand the implementation under test.
- Do not inspect target/trace-review/results/ or aggregate summaries.
- Do not edit files. You may run read-only inspection commands and non-source-mutating checks.
- Return only one JSON object matching the requested result schema.
- The JSON object must use test_id {test_id!r}.
"""


@dataclass(frozen=True)
class SpecRef:
    doc: str
    anchor: str


@dataclass(frozen=True)
class ParsedBlock:
    test_attrs: int
    spec_refs: list[SpecRef]
    type_refs: list[str]
    quote_blocks: list[str]

    @property
    def is_empty(self) -> bool:
        return not self.spec_refs and not self.type_refs and not self.quote_blocks


@dataclass(frozen=True)
class TraceEntry:
    test_id: str
    trace_type: str
    path: str
    line: int
    function: str
    spec_doc: str
    spec_anchor: str
    requirement_quote: str
    requirement_ids: list[str]
    likely_entry_points: list[str]


@dataclass(frozen=True)
class TracePacket:
    entry: TraceEntry
    annotation_block: str
    test_source: str
    spec_section: str


def repo_root_from_args(value: str | None) -> Path:
    root = Path(value or ".").resolve()
    if not (root / "Cargo.toml").exists():
        raise SystemExit(f"{root} does not look like the repository root")
    return root


def rust_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.rs") if path.is_file())


def relative_path(repo_root: Path, path: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def display_path(repo_root: Path, path: Path) -> str:
    try:
        return relative_path(repo_root, path)
    except ValueError:
        return path.as_posix()


def function_name(line: str) -> str | None:
    match = re.match(r"\s*fn\s+([A-Za-z0-9_]+)\s*\(", line)
    if not match:
        return None
    return match.group(1)


def collect_annotation_block(lines: list[str], fn_line_index: int) -> list[tuple[int, str]]:
    block: list[tuple[int, str]] = []
    index = fn_line_index
    while index > 0:
        index -= 1
        line = lines[index]
        stripped = line.strip()
        if not stripped:
            if block:
                block.append((index + 1, line))
            continue
        if stripped.startswith("#[") or is_annotation(stripped):
            block.append((index + 1, line))
            continue
        break
    block.reverse()
    return block


def parse_annotation_block(block: list[tuple[int, str]]) -> ParsedBlock:
    test_attrs = 0
    spec_refs: list[SpecRef] = []
    type_refs: list[str] = []
    quote_blocks: list[str] = []
    current_quote: list[str] = []

    for _, line in block:
        stripped = line.strip()
        if stripped == "#[test]":
            test_attrs += 1
        quote = parse_quote_line(stripped)
        if quote is not None:
            current_quote.append(quote)
            continue
        if current_quote:
            quote_blocks.append("\n".join(current_quote))
            current_quote.clear()
        spec_ref = parse_doc_ref(stripped)
        if spec_ref is not None:
            spec_refs.append(spec_ref)
        type_ref = parse_type_ref(stripped)
        if type_ref is not None:
            type_refs.append(type_ref)

    if current_quote:
        quote_blocks.append("\n".join(current_quote))

    return ParsedBlock(
        test_attrs=test_attrs,
        spec_refs=spec_refs,
        type_refs=type_refs,
        quote_blocks=quote_blocks,
    )


def is_annotation(line: str) -> bool:
    return (
        parse_doc_ref(line) is not None
        or parse_type_ref(line) is not None
        or parse_quote_line(line) is not None
    )


def parse_doc_ref(line: str) -> SpecRef | None:
    rest = line.removeprefix("//=")
    if rest == line:
        return None
    rest = rest.strip()
    if "#" not in rest:
        return None
    doc, anchor = rest.split("#", 1)
    if not doc.endswith(".md"):
        return None
    return SpecRef(doc=doc, anchor=anchor.strip())


def parse_type_ref(line: str) -> str | None:
    rest = line.removeprefix("//=")
    if rest == line:
        return None
    rest = rest.strip()
    if not rest.startswith("type="):
        return None
    return rest.removeprefix("type=").strip()


def parse_quote_line(line: str) -> str | None:
    rest = line.removeprefix("//#")
    if rest == line:
        return None
    return rest.strip()


def extract_requirement_ids(text: str) -> list[str]:
    ids: list[str] = []
    for line in text.splitlines():
        trimmed = line.lstrip().removeprefix("//#").lstrip().lstrip("`")
        for prefix in ("RING-", "MAP-"):
            if not trimmed.startswith(prefix):
                continue
            end = len(prefix)
            for char in trimmed[len(prefix) :]:
                if char.isascii() and (char.isupper() or char.isdigit() or char == "-"):
                    end += 1
                else:
                    break
            if end > len(prefix):
                value = trimmed[:end]
                if value not in ids:
                    ids.append(value)
    return ids


def strip_rust_for_braces(
    line: str, block_comment: bool, raw_hashes: str | None
) -> tuple[str, bool, str | None]:
    output: list[str] = []
    index = 0
    while index < len(line):
        if raw_hashes is not None:
            terminator = '"' + raw_hashes
            end = line.find(terminator, index)
            if end == -1:
                return "".join(output), block_comment, raw_hashes
            index = end + len(terminator)
            raw_hashes = None
            continue

        if block_comment:
            end = line.find("*/", index)
            if end == -1:
                return "".join(output), block_comment, raw_hashes
            block_comment = False
            index = end + 2
            continue

        if line.startswith("//", index):
            break
        if line.startswith("/*", index):
            block_comment = True
            index += 2
            continue

        raw = raw_string_start(line, index)
        if raw is not None:
            prefix_len, hashes = raw
            index += prefix_len
            raw_hashes = hashes
            continue

        char = line[index]
        if char == '"':
            index += 1
            while index < len(line):
                if line[index] == "\\":
                    index += 2
                    continue
                if line[index] == '"':
                    index += 1
                    break
                index += 1
            continue

        output.append(char)
        index += 1

    return "".join(output), block_comment, raw_hashes


def raw_string_start(line: str, index: int) -> tuple[int, str] | None:
    prefixes = ("br", "r")
    for prefix in prefixes:
        if not line.startswith(prefix, index):
            continue
        hash_index = index + len(prefix)
        hashes = ""
        while hash_index < len(line) and line[hash_index] == "#":
            hashes += "#"
            hash_index += 1
        if hash_index < len(line) and line[hash_index] == '"':
            return hash_index - index + 1, hashes
    return None


def extract_function_source(lines: list[str], fn_line_index: int) -> str:
    block_comment = False
    raw_hashes: str | None = None
    depth = 0
    opened = False
    collected: list[str] = []

    for line in lines[fn_line_index:]:
        collected.append(line)
        stripped, block_comment, raw_hashes = strip_rust_for_braces(
            line, block_comment, raw_hashes
        )
        for char in stripped:
            if char == "{":
                depth += 1
                opened = True
            elif char == "}":
                depth -= 1
                if opened and depth == 0:
                    return "\n".join(collected).rstrip()

    return "\n".join(collected).rstrip()


def slug_heading(text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = text.strip().strip("#").strip()
    text = text.lower()
    output: list[str] = []
    previous_dash = False
    for char in text:
        if char.isalnum():
            output.append(char)
            previous_dash = False
        elif char.isspace() or char == "-":
            if not previous_dash and output:
                output.append("-")
                previous_dash = True
    return "".join(output).strip("-")


def extract_spec_section(repo_root: Path, spec_doc: str, anchor: str) -> str:
    spec_path = repo_root / spec_doc
    if not spec_path.exists():
        return f"{spec_doc} was not found."

    lines = spec_path.read_text(encoding="utf-8").splitlines()
    start_index: int | None = None
    start_level: int | None = None
    for index, line in enumerate(lines):
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if not match:
            continue
        if slug_heading(match.group(2)) == anchor:
            start_index = index
            start_level = len(match.group(1))
            break

    if start_index is None or start_level is None:
        return f"Could not extract #{anchor} from {spec_doc}; inspect the full spec."

    end_index = len(lines)
    for index in range(start_index + 1, len(lines)):
        match = re.match(r"^(#{1,6})\s+", lines[index])
        if match and len(match.group(1)) <= start_level:
            end_index = index
            break

    return "\n".join(lines[start_index:end_index]).strip()


def likely_entry_points(repo_root: Path, test_path: Path) -> list[str]:
    candidates: list[Path] = [test_path]
    rel_parts = test_path.resolve().relative_to(repo_root.resolve()).parts

    if test_path.name == "tests.rs":
        module_dir = test_path.parent
        sibling_file = module_dir.with_suffix(".rs")
        nested_mod = module_dir / "mod.rs"
        candidates.extend([sibling_file, nested_mod])

    if "tests" in rel_parts:
        tests_index = rel_parts.index("tests")
        if tests_index > 0:
            module_root = repo_root / Path(*rel_parts[:tests_index])
            candidates.extend([module_root.with_suffix(".rs"), module_root / "mod.rs"])

    if rel_parts[:2] == ("src", "tests"):
        candidates.extend([repo_root / "src" / "lib.rs", repo_root / "src" / "tests" / "mod.rs"])
        if len(rel_parts) > 3:
            nested_root = repo_root / Path(*rel_parts[:-1])
            candidates.extend([nested_root.with_suffix(".rs"), nested_root / "mod.rs"])
    elif len(rel_parts) > 2 and rel_parts[0] == "src" and rel_parts[2] == "tests.rs":
        module_root = repo_root / Path(*rel_parts[:2])
        candidates.extend([module_root.with_suffix(".rs"), module_root / "mod.rs"])

    result: list[str] = []
    for candidate in candidates:
        candidate = candidate if candidate.is_absolute() else repo_root / candidate
        if candidate.exists():
            rel = relative_path(repo_root, candidate)
            if rel not in result:
                result.append(rel)
    return result


def make_test_id(path: str, line: int, function: str) -> str:
    source = f"{path}:{line}:{function}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:10]
    stem = re.sub(r"[^A-Za-z0-9]+", "_", f"{path}__{line:04d}__{function}")
    stem = stem.strip("_")[:180].rstrip("_")
    return f"{stem}__{digest}"


def collect_trace_packets(repo_root: Path) -> tuple[list[TracePacket], list[str]]:
    packets: list[TracePacket] = []
    errors: list[str] = []

    for path in rust_files(repo_root / "src"):
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        for index, line in enumerate(lines):
            name = function_name(line)
            if name is None:
                continue

            block = collect_annotation_block(lines, index)
            parsed = parse_annotation_block(block)
            if parsed.is_empty:
                continue
            rel = relative_path(repo_root, path)

            if (
                parsed.test_attrs != 1
                or len(parsed.spec_refs) != 1
                or len(parsed.type_refs) != 1
                or len(parsed.quote_blocks) != 1
            ):
                errors.append(
                    f"{rel}:{index + 1}: {name} has malformed trace metadata "
                    f"(test={parsed.test_attrs}, spec={len(parsed.spec_refs)}, "
                    f"type={len(parsed.type_refs)}, quote={len(parsed.quote_blocks)})"
                )
                continue

            spec = parsed.spec_refs[0]
            quote = parsed.quote_blocks[0]
            trace_type = parsed.type_refs[0]
            entry = TraceEntry(
                test_id=make_test_id(rel, index + 1, name),
                trace_type=trace_type,
                path=rel,
                line=index + 1,
                function=name,
                spec_doc=spec.doc,
                spec_anchor=spec.anchor,
                requirement_quote=quote,
                requirement_ids=extract_requirement_ids(quote),
                likely_entry_points=likely_entry_points(repo_root, path),
            )
            packets.append(
                TracePacket(
                    entry=entry,
                    annotation_block="\n".join(line for _, line in block).rstrip(),
                    test_source=extract_function_source(lines, index),
                    spec_section=extract_spec_section(repo_root, spec.doc, spec.anchor),
                )
            )

    packets.sort(key=lambda packet: (packet.entry.path, packet.entry.line))
    return packets, errors


def run_preflight(repo_root: Path) -> tuple[int, int]:
    traceability = subprocess.run(
        ["cargo", "run", "--quiet", "--bin", "traceability_audit", "--", "check-requirements"],
        cwd=repo_root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print(traceability.stdout, end="")
    match = re.search(
        r"validated\s+(\d+)\s+requirement tests and\s+(\d+)\s+todo tests",
        traceability.stdout,
    )
    if not match:
        raise SystemExit("could not parse traceability_audit summary")

    subprocess.run(
        ["duvet", "report", "--config-path", ".duvet/config.toml", "--require-tests", "true"],
        cwd=repo_root,
        check=True,
    )
    return int(match.group(1)), int(match.group(2))


def git_tracked_file_hashes(repo_root: Path) -> dict[str, str]:
    files = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.split(b"\0")
    hashes: dict[str, str] = {}
    for raw_path in files:
        if not raw_path:
            continue
        rel = raw_path.decode("utf-8")
        path = repo_root / rel
        if path.exists():
            hashes[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            hashes[rel] = "<missing>"
    return hashes


def changed_tracked_files(before: dict[str, str], after: dict[str, str]) -> list[str]:
    changed: list[str] = []
    for path in sorted(set(before) | set(after)):
        if before.get(path) != after.get(path):
            changed.append(path)
    return changed


def git_status_lines(repo_root: Path) -> set[str]:
    status = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repo_root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout
    return {line for line in status.splitlines() if line.strip()}


def new_status_lines(
    before: set[str], after: set[str], repo_root: Path, output_dir: Path
) -> list[str]:
    output_prefix = display_path(repo_root, output_dir).rstrip("/") + "/"
    allowed_prefixes = {output_prefix}
    added: list[str] = []
    for line in sorted(after - before):
        path = line[3:]
        if any(path.startswith(prefix) for prefix in allowed_prefixes):
            continue
        added.append(line)
    return added


def write_result_schema(output_dir: Path) -> Path:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": sorted(REQUIRED_RESULT_FIELDS),
        "properties": {
            "test_id": {"type": "string"},
            "verdict": {"enum": sorted(VALID_VERDICTS)},
            "rationale": {"type": "string", "minLength": 1},
            "inspected_paths": {"type": "array", "items": {"type": "string"}},
            "key_assertions": {"type": "array", "items": {"type": "string"}},
            "missing_clauses": {"type": "array", "items": {"type": "string"}},
            "suggested_improvement": {"type": ["string", "null"]},
        },
    }
    schema_path = output_dir / "result_schema.json"
    schema_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    return schema_path


def parse_json_response(text: str) -> Any:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(stripped[start : end + 1])


def packet_path(output_dir: Path, test_id: str) -> Path:
    return output_dir / "packets" / f"{test_id}.md"


def result_path(output_dir: Path, test_id: str) -> Path:
    return output_dir / "results" / f"{test_id}.json"


def hash_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def packet_hash(packet: TracePacket) -> str:
    return hash_json(
        {
            "entry": asdict(packet.entry),
            "annotation_block": packet.annotation_block,
            "test_source": packet.test_source,
            "spec_section": packet.spec_section,
        }
    )


def normalize_review_path(repo_root: Path, path: str) -> str:
    cleaned = path.strip().strip("`")
    cleaned = re.sub(r":\d+(?::\d+)?$", "", cleaned)
    if cleaned.startswith("./"):
        cleaned = cleaned[2:]

    candidate = Path(cleaned)
    if candidate.is_absolute():
        try:
            return candidate.resolve().relative_to(repo_root.resolve()).as_posix()
        except ValueError:
            return candidate.as_posix()

    return candidate.as_posix()


def review_dependency_paths(
    repo_root: Path, packet: TracePacket, result: dict[str, Any]
) -> list[str]:
    paths = [
        packet.entry.path,
        packet.entry.spec_doc,
        *packet.entry.likely_entry_points,
        *result.get("inspected_paths", []),
    ]
    normalized: list[str] = []
    for path in paths:
        if not isinstance(path, str) or not path.strip():
            continue
        normalized_path = normalize_review_path(repo_root, path)
        if normalized_path not in normalized:
            normalized.append(normalized_path)
    return normalized


def file_or_directory_hash(repo_root: Path, relative: str) -> str:
    path = Path(relative)
    if path.is_absolute():
        try:
            path = path.resolve().relative_to(repo_root.resolve())
        except ValueError:
            return "outside-repo"

    absolute = repo_root / path
    if not absolute.exists():
        return "missing"
    if absolute.is_file():
        return "file:" + hashlib.sha256(absolute.read_bytes()).hexdigest()
    if absolute.is_dir():
        entries: list[tuple[str, str]] = []
        for child in sorted(absolute.rglob("*")):
            if not child.is_file():
                continue
            rel = child.resolve().relative_to(repo_root.resolve()).as_posix()
            if rel.startswith(".git/") or rel.startswith("target/trace-review/"):
                continue
            entries.append((rel, hashlib.sha256(child.read_bytes()).hexdigest()))
        return "dir:" + hash_json(entries)
    return "unsupported"


def dependency_hashes(repo_root: Path, paths: list[str]) -> dict[str, str]:
    return {path: file_or_directory_hash(repo_root, path) for path in paths}


def attach_review_cache(
    repo_root: Path, packet: TracePacket, result: dict[str, Any]
) -> dict[str, Any]:
    result = dict(result)
    dependencies = dependency_hashes(
        repo_root, review_dependency_paths(repo_root, packet, result)
    )
    result["review_cache"] = {
        "version": REVIEW_CACHE_VERSION,
        "packet_hash": packet_hash(packet),
        "dependencies": dependencies,
    }
    return result


def cached_result_status(
    repo_root: Path, packet: TracePacket, result: dict[str, Any]
) -> tuple[bool, str]:
    errors = validate_result(packet.entry.test_id, result)
    if errors:
        return False, "invalid result"
    if result.get("verdict") == "needs_context":
        return False, "needs_context result"

    cache = result.get("review_cache")
    if not isinstance(cache, dict):
        return False, "missing cache metadata"
    if cache.get("version") != REVIEW_CACHE_VERSION:
        return False, "cache version changed"
    if cache.get("packet_hash") != packet_hash(packet):
        return False, "packet changed"

    expected_dependencies = dependency_hashes(
        repo_root, review_dependency_paths(repo_root, packet, result)
    )
    if cache.get("dependencies") != expected_dependencies:
        return False, "review dependency changed"

    return True, "unchanged"


def load_result_file(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if isinstance(value, dict):
        return value
    return None


def prepare_codex_home(output_dir: Path) -> Path:
    codex_home = output_dir / "codex-home"
    codex_home.mkdir(parents=True, exist_ok=True)
    source_home = Path.home() / ".codex"
    for name in ("auth.json", "config.toml", "installation_id"):
        source = source_home / name
        target = codex_home / name
        if target.exists() or target.is_symlink() or not source.exists():
            continue
        try:
            os.symlink(source, target)
        except OSError:
            shutil.copy2(source, target)
    return codex_home


def codex_environment(codex_home: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["CODEX_HOME"] = str(codex_home)
    return env


def codex_sandbox_available(
    codex_bin: str, repo_root: Path, codex_home: Path
) -> tuple[bool, str]:
    try:
        process = subprocess.run(
            [
                codex_bin,
                "sandbox",
                "--permissions-profile",
                ":workspace",
                "--cd",
                str(repo_root),
                "true",
            ],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=codex_environment(codex_home),
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, str(error)

    if process.returncode == 0:
        return True, ""

    detail = (process.stderr or process.stdout).strip()
    return False, detail or f"codex sandbox exited with {process.returncode}"


def resolve_reviewer_sandbox(
    codex_bin: str, repo_root: Path, requested: str, codex_home: Path
) -> tuple[str, str | None]:
    if requested != "auto":
        return requested, None

    available, detail = codex_sandbox_available(codex_bin, repo_root, codex_home)
    if available:
        return "workspace-write", None

    return (
        "danger-full-access",
        "Codex bubblewrap sandbox is unavailable for nested reviewer runs; "
        f"falling back to danger-full-access. Sandbox check: {detail}",
    )


def run_reviewer(
    repo_root: Path,
    output_dir: Path,
    packet_entry: TracePacket,
    *,
    codex_bin: str,
    schema_path: Path,
    model: str | None,
    effort: str,
    sandbox: str,
    codex_home: Path,
) -> None:
    entry = packet_entry.entry
    test_id = entry.test_id
    review_packet_path = packet_path(output_dir, test_id)
    if not review_packet_path.exists():
        raise SystemExit(f"{review_packet_path} does not exist; run init first")

    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    last_message_path = logs_dir / f"{test_id}.last-message.json"
    stdout_path = logs_dir / f"{test_id}.stdout.log"
    stderr_path = logs_dir / f"{test_id}.stderr.log"

    prompt = REVIEW_PROMPT.format(
        packet_path=display_path(repo_root, review_packet_path),
        test_id=test_id,
    )
    command = [
        codex_bin,
        "exec",
        "-c",
        'approval_policy="never"',
        "-c",
        f'model_reasoning_effort="{effort}"',
        "--cd",
        str(repo_root),
        "--sandbox",
        sandbox,
        "--ephemeral",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(last_message_path),
        "-",
    ]
    if model:
        command[2:2] = ["--model", model]

    before = git_tracked_file_hashes(repo_root)
    status_before = git_status_lines(repo_root)
    process = subprocess.run(
        command,
        cwd=repo_root,
        input=prompt,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=codex_environment(codex_home),
    )
    stdout_path.write_text(process.stdout, encoding="utf-8")
    stderr_path.write_text(process.stderr, encoding="utf-8")

    after = git_tracked_file_hashes(repo_root)
    status_after = git_status_lines(repo_root)
    changed = changed_tracked_files(before, after)
    if changed:
        raise SystemExit(
            "reviewer changed tracked files; inspect and revert intentionally before continuing: "
            + ", ".join(changed)
        )
    new_status = new_status_lines(status_before, status_after, repo_root, output_dir)
    if new_status:
        raise SystemExit(
            "reviewer left new non-review worktree changes; inspect them before continuing: "
            + ", ".join(new_status)
        )

    if process.returncode != 0:
        raise SystemExit(
            f"reviewer failed for {test_id}; see {display_path(repo_root, stderr_path)}"
        )
    if not last_message_path.exists():
        raise SystemExit(f"reviewer did not write {display_path(repo_root, last_message_path)}")

    result = parse_json_response(last_message_path.read_text(encoding="utf-8"))
    errors = validate_result(test_id, result)
    if errors:
        raise SystemExit(
            "reviewer returned invalid result:\n" + "\n".join(f"- {error}" for error in errors)
        )

    result = attach_review_cache(repo_root, packet_entry, result)
    path = result_path(output_dir, test_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def write_inventory(
    output_dir: Path, packets: list[TracePacket], *, packet_limit: int | None = None
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    requirement_entries = [
        asdict(packet.entry) for packet in packets if packet.entry.trace_type == "test"
    ]
    todo_entries = [
        asdict(packet.entry) for packet in packets if packet.entry.trace_type == "todo"
    ]

    inventory = {
        "requirement_tests": len(requirement_entries),
        "todo_tests": len(todo_entries),
        "packet_limit": packet_limit,
        "entries": requirement_entries,
        "todos": todo_entries,
    }
    (output_dir / "inventory.json").write_text(
        json.dumps(inventory, indent=2) + "\n", encoding="utf-8"
    )


def write_packets(
    repo_root: Path,
    output_dir: Path,
    packets: list[TracePacket],
    *,
    packet_limit: int | None = None,
) -> int:
    packet_dir = output_dir / "packets"
    packet_dir.mkdir(parents=True, exist_ok=True)
    result_dir = output_dir / "results"
    result_dir.mkdir(parents=True, exist_ok=True)

    selected = [packet for packet in packets if packet.entry.trace_type == "test"]
    if packet_limit is not None:
        selected = selected[:packet_limit]

    for packet in selected:
        packet_path = packet_dir / f"{packet.entry.test_id}.md"
        packet_path.write_text(render_packet(repo_root, output_dir, packet), encoding="utf-8")

    return len(selected)


def render_packet(repo_root: Path, output_dir: Path, packet: TracePacket) -> str:
    entry = packet.entry
    result_path = output_dir / "results" / f"{entry.test_id}.json"
    result_rel = display_path(repo_root, result_path)
    likely_paths = "\n".join(f"- {path}" for path in entry.likely_entry_points)
    requirement_ids = ", ".join(entry.requirement_ids) if entry.requirement_ids else "(none)"
    result_template = {
        "test_id": entry.test_id,
        "verdict": "pass|weak|fail|needs_context",
        "rationale": "",
        "inspected_paths": [],
        "key_assertions": [],
        "missing_clauses": [],
        "suggested_improvement": None,
    }

    return f"""# Fresh Traceability Review: `{entry.function}`

## Isolation Rules
- Treat this as a new review with no prior findings, summaries, or assumptions.
- You may inspect the full repository read-only to understand the implementation under test.
- Do not read `target/trace-review/results/` or aggregate summaries while reviewing this test.
- Do not edit source files. Write only the JSON result file named below.
- Run non-mutating commands when useful, such as `rg`, `sed`, targeted `cargo test`, or traceability checks.

## Review Rubric
- `pass`: the test's assertions would likely fail for a meaningful violation of every normative clause in the quoted requirement.
- `weak`: the test is related but incomplete, overly smoke-test-like, misses required edge cases, or depends on implementation details that do not prove the requirement.
- `fail`: the test does not materially verify the quoted requirement or appears traced to the wrong requirement.
- `needs_context`: use only if repository access is insufficient to judge.

## Seed
- Test id: `{entry.test_id}`
- Test: `{entry.path}:{entry.line}` `{entry.function}`
- Spec: `{entry.spec_doc}#{entry.spec_anchor}`
- Requirement ids: {requirement_ids}

## Requirement Quote
```text
{entry.requirement_quote}
```

## Relevant Spec Section
```markdown
{packet.spec_section}
```

## Trace Annotation
```rust
{packet.annotation_block}
```

## Test Source
```rust
{packet.test_source}
```

## Likely Starting Points
{likely_paths if likely_paths else "- No path hints found; search the repository."}

## Required Result
Return this JSON object as the final response. In manual mode, write the
same JSON result to `{result_rel}`:

```json
{json.dumps(result_template, indent=2)}
```
"""


def write_reviewer_readme(output_dir: Path) -> None:
    text = """# Trace Review Workspace

This directory is generated by `scripts/trace_review.py`.

Use one fresh reviewer/session per packet in `packets/`. Each reviewer may
inspect the full repository read-only, but must not inspect prior review results
or aggregate summaries. The only expected write is that packet's JSON file under
`results/`.

After reviewers write result files, run:

```sh
python3 scripts/trace_review.py summarize
```
"""
    (output_dir / "README.md").write_text(text, encoding="utf-8")


def validate_result(test_id: str, value: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, dict):
        return [f"{test_id}: result is not a JSON object"]

    missing = sorted(REQUIRED_RESULT_FIELDS - set(value))
    if missing:
        errors.append(f"{test_id}: missing fields {missing}")

    if value.get("test_id") != test_id:
        errors.append(f"{test_id}: test_id field does not match filename")

    verdict = value.get("verdict")
    if verdict not in VALID_VERDICTS:
        errors.append(f"{test_id}: invalid verdict {verdict!r}")

    for field in ("rationale",):
        if not isinstance(value.get(field), str) or not value.get(field, "").strip():
            errors.append(f"{test_id}: {field} must be a non-empty string")

    for field in ("inspected_paths", "key_assertions", "missing_clauses"):
        if not isinstance(value.get(field), list):
            errors.append(f"{test_id}: {field} must be a list")

    if verdict in {"pass", "weak", "fail"} and not value.get("inspected_paths"):
        errors.append(f"{test_id}: inspected_paths must not be empty for {verdict}")

    if verdict in {"weak", "fail"} and not value.get("suggested_improvement"):
        errors.append(f"{test_id}: suggested_improvement is required for {verdict}")

    return errors


def load_inventory(output_dir: Path) -> dict[str, Any]:
    inventory_path = output_dir / "inventory.json"
    if not inventory_path.exists():
        raise SystemExit(f"{inventory_path} does not exist; run init first")
    return json.loads(inventory_path.read_text(encoding="utf-8"))


def load_results(output_dir: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    result_dir = output_dir / "results"
    results: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    if not result_dir.exists():
        return results, errors

    for path in sorted(result_dir.glob("*.json")):
        test_id = path.stem
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            errors.append(f"{path}: invalid JSON: {error}")
            continue
        errors.extend(validate_result(test_id, value))
        if isinstance(value, dict):
            results[test_id] = value

    return results, errors


def render_summary(output_dir: Path) -> str:
    inventory = load_inventory(output_dir)
    entries = inventory["entries"]
    by_id = {entry["test_id"]: entry for entry in entries}
    results, errors = load_results(output_dir)
    stale_result_ids = sorted(set(results) - set(by_id))
    results = {
        test_id: result
        for test_id, result in results.items()
        if test_id in by_id
    }

    verdict_counts = {verdict: 0 for verdict in sorted(VALID_VERDICTS)}
    for result in results.values():
        verdict = result.get("verdict")
        if verdict in verdict_counts:
            verdict_counts[verdict] += 1

    reviewed = len(results)
    total = len(entries)
    lines = [
        "# Fresh Traceability Review Summary",
        "",
        f"- Requirement tests inventoried: {total}",
        f"- Todo traces inventoried: {len(inventory['todos'])}",
        f"- Result files reviewed: {reviewed}",
        f"- Pending reviews: {total - reviewed}",
        f"- Stale result files ignored: {len(stale_result_ids)}",
        "",
        "## Verdict Counts",
        "",
    ]
    for verdict in ("fail", "weak", "needs_context", "pass"):
        lines.append(f"- {verdict}: {verdict_counts[verdict]}")

    if errors:
        lines.extend(["", "## Result Validation Errors", ""])
        lines.extend(f"- {error}" for error in errors)

    if stale_result_ids:
        lines.extend(["", "## Stale Results Ignored", ""])
        lines.extend(f"- `{test_id}`" for test_id in stale_result_ids)

    findings = [
        (test_id, result)
        for test_id, result in sorted(results.items())
        if result.get("verdict") in {"fail", "weak", "needs_context"}
    ]
    if findings:
        lines.extend(["", "## Findings To Resolve", ""])
        for test_id, result in findings:
            entry = by_id.get(test_id, {})
            location = f"{entry.get('path', '?')}:{entry.get('line', '?')}"
            lines.extend(
                [
                    f"### {result.get('verdict')} - `{entry.get('function', test_id)}`",
                    "",
                    f"- Location: `{location}`",
                    f"- Spec: `{entry.get('spec_doc', '?')}#{entry.get('spec_anchor', '?')}`",
                    f"- Rationale: {result.get('rationale', '').strip()}",
                ]
            )
            missing = result.get("missing_clauses") or []
            if missing:
                lines.append(f"- Missing clauses: {'; '.join(str(item) for item in missing)}")
            suggestion = result.get("suggested_improvement")
            if suggestion:
                lines.append(f"- Suggested improvement: {suggestion}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def command_init(args: argparse.Namespace) -> int:
    repo_root = repo_root_from_args(args.repo_root)
    output_dir = (repo_root / args.output_dir).resolve()

    expected_counts: tuple[int, int] | None = None
    if not args.skip_preflight:
        expected_counts = run_preflight(repo_root)

    packets, errors = collect_trace_packets(repo_root)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    requirement_count = sum(1 for packet in packets if packet.entry.trace_type == "test")
    todo_count = sum(1 for packet in packets if packet.entry.trace_type == "todo")
    if expected_counts is not None and expected_counts != (requirement_count, todo_count):
        print(
            "inventory count mismatch: "
            f"traceability_audit={expected_counts}, local={(requirement_count, todo_count)}",
            file=sys.stderr,
        )
        return 1

    write_inventory(output_dir, packets, packet_limit=args.packet_limit)
    packet_count = write_packets(
        repo_root, output_dir, packets, packet_limit=args.packet_limit
    )
    write_reviewer_readme(output_dir)

    print(
        f"wrote {requirement_count} requirement entries, {todo_count} todo entries, "
        f"and {packet_count} review packets to {display_path(repo_root, output_dir)}"
    )
    return 0


def command_inventory(args: argparse.Namespace) -> int:
    repo_root = repo_root_from_args(args.repo_root)
    output_dir = (repo_root / args.output_dir).resolve()
    packets, errors = collect_trace_packets(repo_root)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    write_inventory(output_dir, packets)
    requirement_count = sum(1 for packet in packets if packet.entry.trace_type == "test")
    todo_count = sum(1 for packet in packets if packet.entry.trace_type == "todo")
    print(
        f"wrote inventory for {requirement_count} requirement tests and "
        f"{todo_count} todo traces to {display_path(repo_root, output_dir)}"
    )
    return 0


def command_summarize(args: argparse.Namespace) -> int:
    repo_root = repo_root_from_args(args.repo_root)
    output_dir = (repo_root / args.output_dir).resolve()
    summary = render_summary(output_dir)
    summary_path = output_dir / "summary.md"
    summary_path.write_text(summary, encoding="utf-8")
    print(summary)
    print(f"wrote {display_path(repo_root, summary_path)}")
    return 0


def command_review(args: argparse.Namespace) -> int:
    repo_root = repo_root_from_args(args.repo_root)
    output_dir = (repo_root / args.output_dir).resolve()
    codex_bin = args.codex_bin or shutil.which("codex")
    if codex_bin is None:
        print("codex executable was not found on PATH", file=sys.stderr)
        return 127
    codex_home = prepare_codex_home(output_dir)

    init_args = argparse.Namespace(
        repo_root=args.repo_root,
        output_dir=args.output_dir,
        skip_preflight=args.skip_preflight,
        packet_limit=None,
    )
    init_status = command_init(init_args)
    if init_status != 0:
        return init_status

    inventory = load_inventory(output_dir)
    packets, errors = collect_trace_packets(repo_root)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    packets_by_id = {
        packet.entry.test_id: packet for packet in packets if packet.entry.trace_type == "test"
    }
    entries: list[TracePacket] = [
        packets_by_id[entry["test_id"]]
        for entry in inventory["entries"]
        if entry["test_id"] in packets_by_id
    ]
    if args.only:
        requested = set(args.only)
        entries = [entry for entry in entries if entry.entry.test_id in requested]
        missing = sorted(requested - {entry.entry.test_id for entry in entries})
        if missing:
            print(f"unknown test ids: {missing}", file=sys.stderr)
            return 2

    skipped_current = 0
    stale_reasons: dict[str, int] = {}
    if args.resume:
        selected_entries: list[TracePacket] = []
        for entry in entries:
            existing = load_result_file(result_path(output_dir, entry.entry.test_id))
            if existing is None:
                selected_entries.append(entry)
                continue

            current, reason = cached_result_status(repo_root, entry, existing)
            if current:
                skipped_current += 1
                continue

            stale_reasons[reason] = stale_reasons.get(reason, 0) + 1
            selected_entries.append(entry)
        entries = selected_entries

    if args.limit is not None:
        entries = entries[: args.limit]

    reviewer_sandbox, sandbox_warning = resolve_reviewer_sandbox(
        codex_bin, repo_root, args.reviewer_sandbox, codex_home
    )
    if sandbox_warning:
        print(f"warning: {sandbox_warning}", file=sys.stderr)

    schema_path = write_result_schema(output_dir)
    if args.resume:
        print(f"skipped {skipped_current} unchanged review(s)")
        for reason, count in sorted(stale_reasons.items()):
            print(f"rerunning {count} stale review(s): {reason}")
    print(
        f"running {len(entries)} fresh review(s) with {codex_bin} "
        f"(sandbox={reviewer_sandbox}, effort={args.effort}, "
        f"codex_home={display_path(repo_root, codex_home)})"
    )

    for index, entry in enumerate(entries, start=1):
        test_id = entry.entry.test_id
        print(
            f"[{index}/{len(entries)}] reviewing "
            f"{entry.entry.path}:{entry.entry.line} {entry.entry.function}"
        )
        if args.dry_run:
            continue
        run_reviewer(
            repo_root,
            output_dir,
            entry,
            codex_bin=codex_bin,
            schema_path=schema_path,
            model=args.model,
            effort=args.effort,
            sandbox=reviewer_sandbox,
            codex_home=codex_home,
        )

    if not args.dry_run:
        summary = render_summary(output_dir)
        summary_path = output_dir / "summary.md"
        summary_path.write_text(summary, encoding="utf-8")
        print(f"wrote {display_path(repo_root, summary_path)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare and summarize fresh per-test traceability reviews."
    )
    parser.add_argument("--repo-root", default=".", help="repository root")
    parser.add_argument(
        "--output-dir",
        default="target/trace-review",
        help="generated review workspace",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser(
        "init", help="run preflight, generate inventory, and write review packets"
    )
    init.add_argument(
        "--skip-preflight",
        action="store_true",
        help="skip traceability_audit and Duvet preflight",
    )
    init.add_argument(
        "--packet-limit",
        type=int,
        default=None,
        help="write only the first N packets while still inventorying all tests",
    )
    init.set_defaults(func=command_init)

    inventory = subparsers.add_parser("inventory", help="write inventory.json only")
    inventory.set_defaults(func=command_inventory)

    summarize = subparsers.add_parser(
        "summarize", help="validate result JSON files and write summary.md"
    )
    summarize.set_defaults(func=command_summarize)

    review = subparsers.add_parser(
        "review", help="run the fresh per-test review loop with Codex"
    )
    review.add_argument(
        "--skip-preflight",
        action="store_true",
        help="skip traceability_audit and Duvet preflight before reviewing",
    )
    review.add_argument(
        "--limit",
        type=int,
        default=None,
        help="review only the first N pending tests",
    )
    review.add_argument(
        "--only",
        action="append",
        default=[],
        help="review only this test id; may be repeated",
    )
    review.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="rerun reviews even when the cached result is still current",
    )
    review.add_argument(
        "--dry-run",
        action="store_true",
        help="print selected reviews without invoking Codex",
    )
    review.add_argument("--codex-bin", default=None, help="path to the codex executable")
    review.add_argument("--model", default=None, help="optional Codex model override")
    review.add_argument(
        "--effort",
        default=DEFAULT_REVIEWER_EFFORT,
        choices=sorted(VALID_REVIEWER_EFFORTS),
        help=(
            "Codex model_reasoning_effort for each reviewer "
            f"(default: {DEFAULT_REVIEWER_EFFORT})"
        ),
    )
    review.add_argument(
        "--reviewer-sandbox",
        default=DEFAULT_REVIEWER_SANDBOX,
        choices=sorted(VALID_REVIEWER_SANDBOXES),
        help=(
            "sandbox for each fresh reviewer; auto uses workspace-write when "
            "Codex sandbox works and danger-full-access when nested bubblewrap fails "
            f"(default: {DEFAULT_REVIEWER_SANDBOX})"
        ),
    )
    review.set_defaults(func=command_review, resume=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

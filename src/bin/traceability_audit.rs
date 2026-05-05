use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Summary {
    requirement_tests: usize,
    todo_tests: usize,
}

#[derive(Clone)]
struct SpecRef {
    spec_doc: String,
    spec_anchor: String,
}

#[derive(Default)]
struct ParsedBlock {
    test_attrs: usize,
    spec_refs: Vec<SpecRef>,
    type_refs: Vec<String>,
    quote_blocks: Vec<String>,
}

impl ParsedBlock {
    fn is_empty(&self) -> bool {
        self.spec_refs.is_empty() && self.type_refs.is_empty() && self.quote_blocks.is_empty()
    }
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (None, None) | (Some("check-requirements"), None) => {
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
            match check_requirements(repo_root) {
                Ok(summary) => {
                    println!(
                        "validated {} requirement tests and {} todo tests",
                        summary.requirement_tests, summary.todo_tests
                    );
                    ExitCode::SUCCESS
                }
                Err(errors) => {
                    eprintln!("[traceability-audit] repository traceability policy failed");
                    for error in errors {
                        eprintln!("- {error}");
                    }
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("usage: cargo run --bin traceability_audit -- check-requirements");
            ExitCode::from(2)
        }
    }
}

fn check_requirements(repo_root: &Path) -> Result<Summary, Vec<String>> {
    let mut failures = Vec::new();

    for (spec_path, prefixes) in [
        ("spec/implementation.md", &["RING-IMPL-"][..]),
        ("spec/implementation-policy.md", &["RING-IMPL-"][..]),
        ("spec/ring.md", &["RING-"][..]),
        ("spec/map.md", &["MAP-", "RING-IMPL-REGRESSION-"][..]),
        ("spec/channel.md", &["RING-IMPL-REGRESSION-"][..]),
        ("spec/mock.md", &["RING-IMPL-REGRESSION-"][..]),
    ] {
        match spec_requirement_format_offenders(repo_root, spec_path, prefixes) {
            Ok(Some(message)) => {
                failures.push(format!("{spec_path}#requirements-format: {message}"));
            }
            Ok(None) => {}
            Err(error) => failures.push(error),
        }
    }

    let summary = check_annotation_shape(repo_root, &mut failures);

    if let Some(message) = inline_test_module_offenders(repo_root) {
        failures.push(format!("RING-IMPL-TEST-005: {message}"));
    }

    if let Some(message) = functional_untraced_test_offenders(repo_root) {
        failures.push(format!("RING-IMPL-TEST-006: {message}"));
    }

    if let Some(message) = multi_requirement_harness_offenders(repo_root) {
        failures.push(format!("RING-IMPL-TEST-004: {message}"));
    }

    if failures.is_empty() {
        Ok(summary)
    } else {
        Err(failures)
    }
}

fn check_annotation_shape(repo_root: &Path, errors: &mut Vec<String>) -> Summary {
    let mut seen_requirements: HashMap<(String, String, String), (PathBuf, usize, String)> =
        HashMap::new();
    let mut spec_requirements: HashMap<String, Result<Option<HashSet<String>>, String>> =
        HashMap::new();
    let mut summary = Summary {
        requirement_tests: 0,
        todo_tests: 0,
    };

    for path in rust_files(&repo_root.join("src")) {
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) => {
                errors.push(format!(
                    "{}: failed to read file: {error}",
                    relative_display(repo_root, &path)
                ));
                continue;
            }
        };
        let lines: Vec<&str> = contents.lines().collect();
        let mut consumed_annotations = HashSet::new();

        for index in 0..lines.len() {
            let Some(fn_name) = function_name(lines[index]) else {
                continue;
            };

            let block = collect_annotation_block(&lines, index);
            let parsed = parse_annotation_block(&block);
            if parsed.is_empty() {
                continue;
            }

            for (line_number, line) in &block {
                if is_annotation(line.trim()) {
                    consumed_annotations.insert(*line_number);
                }
            }

            if parsed.test_attrs != 1
                || parsed.spec_refs.len() != 1
                || parsed.type_refs.len() != 1
                || parsed.quote_blocks.len() != 1
            {
                errors.push(format!(
                    "{}:{}: {fn_name} must have exactly one #[test], one //= <spec>.md#..., one //= type=..., and one logical //# quote block (found test={}, spec={}, type={}, quote={})",
                    relative_display(repo_root, &path),
                    index + 1,
                    parsed.test_attrs,
                    parsed.spec_refs.len(),
                    parsed.type_refs.len(),
                    parsed.quote_blocks.len()
                ));
                continue;
            }

            let trace_type = &parsed.type_refs[0];
            match trace_type.as_str() {
                "test" if !fn_name.starts_with("requirement_") => {
                    errors.push(format!(
                        "{}:{}: {fn_name} must use the requirement_ prefix for type=test traces",
                        relative_display(repo_root, &path),
                        index + 1
                    ));
                    continue;
                }
                "todo" if !fn_name.starts_with("todo_") => {
                    errors.push(format!(
                        "{}:{}: {fn_name} must use the todo_ prefix for type=todo traces",
                        relative_display(repo_root, &path),
                        index + 1
                    ));
                    continue;
                }
                "test" => summary.requirement_tests += 1,
                "todo" => summary.todo_tests += 1,
                _ => {
                    errors.push(format!(
                        "{}:{}: {fn_name} uses unsupported trace type {trace_type:?}",
                        relative_display(repo_root, &path),
                        index + 1
                    ));
                    continue;
                }
            }

            let spec = &parsed.spec_refs[0];
            let quote = &parsed.quote_blocks[0];
            let normalized_quote = normalize_requirement_whitespace(quote);
            if contains_test_name_placeholder_requirement(quote) {
                errors.push(format!(
                    "{}:{}: {fn_name} traces a test-name placeholder instead of functional behavior",
                    relative_display(repo_root, &path),
                    index + 1
                ));
                continue;
            }

            let ids = extract_requirement_ids(quote);
            if ids.len() > 1 {
                errors.push(format!(
                    "{}:{}: {fn_name} traces multiple requirement identifiers: {ids:?}",
                    relative_display(repo_root, &path),
                    index + 1
                ));
                continue;
            }
            if !ids.is_empty() {
                if !spec_requirements.contains_key(&spec.spec_doc) {
                    spec_requirements.insert(
                        spec.spec_doc.clone(),
                        load_spec_requirement_ids(repo_root, &spec.spec_doc),
                    );
                }
                match spec_requirements.get(&spec.spec_doc) {
                    Some(Ok(Some(requirements)))
                        if !ids.iter().all(|id| requirements.contains(id)) =>
                    {
                        errors.push(format!(
                            "{}:{}: {fn_name} quotes an identifier that does not exist in {}: {ids:?}",
                            relative_display(repo_root, &path),
                            index + 1,
                            spec.spec_doc
                        ));
                        continue;
                    }
                    Some(Err(error)) => {
                        errors.push(error.clone());
                        continue;
                    }
                    _ => {}
                }
            }

            let key = (
                spec.spec_doc.clone(),
                spec.spec_anchor.clone(),
                normalized_quote,
            );
            if let Some((prev_path, prev_line, prev_fn)) = seen_requirements.get(&key) {
                errors.push(format!(
                    "{}:{}: {fn_name} duplicates requirement {}#{} / {:?}, already used by {}:{} ({prev_fn})",
                    relative_display(repo_root, &path),
                    index + 1,
                    spec.spec_doc,
                    spec.spec_anchor,
                    quote,
                    relative_display(repo_root, prev_path),
                    prev_line
                ));
                continue;
            }
            seen_requirements.insert(key, (path.clone(), index + 1, fn_name));
        }

        for (index, line) in lines.iter().enumerate() {
            let line_number = index + 1;
            if consumed_annotations.contains(&line_number) {
                continue;
            }
            if is_annotation(line.trim()) {
                errors.push(format!(
                    "{}:{line_number}: Duvet annotation must be attached to a test function",
                    relative_display(repo_root, &path)
                ));
            }
        }
    }

    summary
}

fn normalize_requirement_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn load_spec_requirement_ids(
    repo_root: &Path,
    relative_spec_path: &str,
) -> Result<Option<HashSet<String>>, String> {
    let spec_path = repo_root.join(relative_spec_path);
    if !spec_path.exists() {
        return Ok(None);
    }

    Ok(Some(
        extract_all_requirement_ids(&read_text(&spec_path)?)
            .into_iter()
            .collect(),
    ))
}

fn spec_requirement_format_offenders(
    repo_root: &Path,
    relative_spec_path: &str,
    expected_prefixes: &[&str],
) -> Result<Option<String>, String> {
    let spec_path = repo_root.join(relative_spec_path);
    let items = collect_normative_requirement_items(&spec_path)?;
    if items.is_empty() {
        return Ok(Some(format!(
            "no normative requirement items found in {relative_spec_path}"
        )));
    }

    let mut offenders = Vec::new();
    for item in items {
        if !expected_prefixes
            .iter()
            .any(|expected_prefix| item.starts_with(&format!("`{expected_prefix}")))
        {
            offenders.push(format!(
                "requirement item does not start with a stable identifier: {item}"
            ));
            continue;
        }

        if !contains_normative_language(&item) {
            offenders.push(format!(
                "requirement item does not contain explicit normative language: {item}"
            ));
        }
        if contains_test_name_placeholder_requirement(&item) {
            offenders.push(format!(
                "requirement item describes a test-name placeholder instead of functional behavior: {item}"
            ));
        }
    }

    if offenders.is_empty() {
        Ok(None)
    } else {
        Ok(Some(offenders.join("; ")))
    }
}

fn collect_normative_requirement_items(spec_path: &Path) -> Result<Vec<String>, String> {
    let source = read_text(spec_path)?;
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_code_block = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if in_code_block {
            continue;
        }

        if let Some(rest) = strip_numbered_prefix(trimmed) {
            push_normative_item(&mut items, &current);
            current.clear();
            current.push_str(rest);
            continue;
        }

        if current.is_empty() {
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            push_normative_item(&mut items, &current);
            current.clear();
            continue;
        }

        current.push(' ');
        current.push_str(trimmed);
    }

    push_normative_item(&mut items, &current);
    Ok(items)
}

fn push_normative_item(items: &mut Vec<String>, item: &str) {
    if contains_normative_language(item) {
        items.push(item.trim().to_string());
    }
}

fn strip_numbered_prefix(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let index = bytes
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(bytes.len());

    if index == 0 || index + 1 >= bytes.len() || bytes[index] != b'.' || bytes[index + 1] != b' ' {
        return None;
    }

    Some(&line[index + 2..])
}

fn contains_normative_language(text: &str) -> bool {
    text.contains(" MUST ")
        || text.contains(" MUST NOT ")
        || text.contains(" SHOULD ")
        || text.contains(" MAY ")
}

fn contains_test_name_placeholder_requirement(text: &str) -> bool {
    text.contains("functional behavior exercised by")
}

fn inline_test_module_offenders(repo_root: &Path) -> Option<String> {
    let src_root = repo_root.join("src");
    let mut offenders = Vec::new();
    for path in rust_files(&src_root) {
        if is_dedicated_test_file(&path) {
            continue;
        }

        let Ok(source) = read_text(&path) else {
            continue;
        };
        if contains_inline_test_body(&source) {
            offenders.push(relative_display(repo_root, &path));
        }
    }

    if offenders.is_empty() {
        None
    } else {
        Some(format!(
            "non-test source files still contain inline test bodies: {offenders:?}"
        ))
    }
}

fn contains_inline_test_body(source: &str) -> bool {
    source.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == "#[test]" || trimmed.starts_with("mod tests {")
    })
}

fn functional_untraced_test_offenders(repo_root: &Path) -> Option<String> {
    let src_root = repo_root.join("src");
    let mut offenders = Vec::new();

    for path in rust_files(&src_root) {
        if !is_functional_test_file(repo_root, &path) {
            continue;
        }

        let Ok(source) = read_text(&path) else {
            continue;
        };
        let lines: Vec<&str> = source.lines().collect();
        for index in 0..lines.len() {
            let Some(fn_name) = function_name(lines[index]) else {
                continue;
            };
            let parsed = parse_annotation_block(&collect_annotation_block(&lines, index));
            if parsed.test_attrs > 0 && parsed.is_empty() {
                offenders.push(format!(
                    "{}:{} ({fn_name})",
                    relative_display(repo_root, &path),
                    index + 1
                ));
            }
        }
    }

    if offenders.is_empty() {
        None
    } else {
        Some(format!(
            "functional test entry points without Duvet traces: {offenders:?}"
        ))
    }
}

fn is_functional_test_file(repo_root: &Path, path: &Path) -> bool {
    if !is_dedicated_test_file(path) {
        return false;
    }

    let relative = path.strip_prefix(repo_root).unwrap_or(path);
    let mut components = relative.components();
    if components
        .next()
        .is_some_and(|component| component.as_os_str() == "src")
        && components
            .next()
            .is_some_and(|component| component.as_os_str() == "bin")
    {
        return false;
    }

    true
}

fn multi_requirement_harness_offenders(repo_root: &Path) -> Option<String> {
    let mut harness_files = Vec::new();
    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            harness_files.extend(rust_files(&root));
        }
    }

    let mut offenders = Vec::new();
    for path in harness_files {
        let Ok(source) = read_text(&path) else {
            continue;
        };
        let mut ids = Vec::new();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                push_unique(&mut ids, id);
            }
        }
        if ids.len() > 1 {
            offenders.push(format!(
                "{} -> {:?}",
                relative_display(repo_root, &path),
                ids
            ));
        }
    }

    if offenders.is_empty() {
        None
    } else {
        Some(format!(
            "non-runtime harness entries still claim multiple requirements: {offenders:?}"
        ))
    }
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_sources(root, &mut files);
    files.sort();
    files
}

fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn is_dedicated_test_file(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) == Some("tests.rs") {
        return true;
    }

    path.components()
        .any(|component| component.as_os_str() == "tests")
}

fn collect_annotation_block<'a>(lines: &'a [&str], fn_line_index: usize) -> Vec<(usize, &'a str)> {
    let mut block = Vec::new();
    let mut index = fn_line_index;
    while index > 0 {
        index -= 1;
        let line = lines[index];
        let stripped = line.trim();
        if stripped.is_empty() {
            if !block.is_empty() {
                block.push((index + 1, line));
            }
            continue;
        }
        if stripped.starts_with("#[") || is_annotation(stripped) {
            block.push((index + 1, line));
            continue;
        }
        break;
    }
    block.reverse();
    block
}

fn parse_annotation_block(block: &[(usize, &str)]) -> ParsedBlock {
    let mut parsed = ParsedBlock::default();
    let mut current_quote = Vec::new();

    for (_, line) in block {
        let stripped = line.trim();
        if stripped == "#[test]" {
            parsed.test_attrs += 1;
        }
        if let Some(quote) = parse_quote_line(stripped) {
            current_quote.push(quote.to_owned());
            continue;
        }
        if !current_quote.is_empty() {
            parsed.quote_blocks.push(current_quote.join("\n"));
            current_quote.clear();
        }
        if let Some(spec) = parse_doc_ref(stripped) {
            parsed.spec_refs.push(spec);
        }
        if let Some(trace_type) = parse_type_ref(stripped) {
            parsed.type_refs.push(trace_type);
        }
    }

    if !current_quote.is_empty() {
        parsed.quote_blocks.push(current_quote.join("\n"));
    }

    parsed
}

fn function_name(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("fn ")?;
    let name_end = rest.find('(')?;
    let name = &rest[..name_end];
    if name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(name.to_owned())
    } else {
        None
    }
}

fn is_annotation(line: &str) -> bool {
    parse_doc_ref(line).is_some()
        || parse_type_ref(line).is_some()
        || parse_quote_line(line).is_some()
}

fn parse_doc_ref(line: &str) -> Option<SpecRef> {
    let rest = line.strip_prefix("//=")?.trim_start();
    let (doc, anchor) = rest.split_once('#')?;
    if !doc.ends_with(".md") {
        return None;
    }
    Some(SpecRef {
        spec_doc: doc.to_owned(),
        spec_anchor: anchor.trim().to_owned(),
    })
}

fn parse_type_ref(line: &str) -> Option<String> {
    let rest = line.strip_prefix("//=")?.trim();
    let trace_type = rest.strip_prefix("type=")?;
    Some(trace_type.to_owned())
}

fn parse_quote_line(line: &str) -> Option<&str> {
    Some(line.strip_prefix("//#")?.trim())
}

fn extract_requirement_ids(text: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for line in text.lines() {
        let trimmed = line
            .trim_start()
            .strip_prefix("//#")
            .unwrap_or(line.trim_start())
            .trim_start()
            .trim_start_matches('`');
        for prefix in ["RING-", "MAP-"] {
            let Some(rest) = trimmed.strip_prefix(prefix) else {
                continue;
            };
            let mut end = prefix.len();
            for byte in rest.as_bytes() {
                if byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'-' {
                    end += 1;
                } else {
                    break;
                }
            }
            if end > prefix.len() {
                push_unique(&mut ids, trimmed[..end].to_string());
            }
        }
    }
    ids
}

fn extract_all_requirement_ids(text: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for line in text.lines() {
        for prefix in ["RING-", "MAP-"] {
            let mut search_start = 0;
            while let Some(relative_start) = line[search_start..].find(prefix) {
                let start = search_start + relative_start;
                let rest = &line[start + prefix.len()..];
                let mut end = start + prefix.len();
                for byte in rest.as_bytes() {
                    if byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'-' {
                        end += 1;
                    } else {
                        break;
                    }
                }
                if end > start + prefix.len() {
                    push_unique(&mut ids, line[start..end].to_string());
                }
                search_start = end.max(start + prefix.len());
            }
        }
    }
    ids
}

fn push_unique(ids: &mut Vec<String>, value: String) {
    if !ids.contains(&value) {
        ids.push(value);
    }
}

fn read_text(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn relative_display(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .display()
        .to_string()
}

#[cfg(test)]
#[path = "traceability_audit/tests.rs"]
mod tests;

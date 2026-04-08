use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug)]
struct TestEntry {
    location: String,
    requirement_ids: Vec<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("[traceability-audit] {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut failures = Vec::new();

    if let Some(message) =
        spec_requirement_format_offenders(repo_root, "spec/implementation.md", "RING-IMPL-")?
    {
        failures.push(format!(
            "spec/implementation-policy.md#requirements-format: {message}"
        ));
    }

    if let Some(message) =
        spec_requirement_format_offenders(repo_root, "spec/implementation-policy.md", "RING-IMPL-")?
    {
        failures.push(format!(
            "spec/implementation-policy.md#requirements-format: {message}"
        ));
    }

    if let Some(message) = spec_requirement_format_offenders(repo_root, "spec/ring.md", "RING-")? {
        failures.push(format!("spec/ring.md#requirements-format: {message}"));
    }

    if let Some(message) = inline_test_module_offenders(repo_root)? {
        failures.push(format!("RING-IMPL-TEST-005: {message}"));
    }

    if let Some(message) = multi_requirement_test_offenders(repo_root)? {
        failures.push(format!("RING-IMPL-TEST-002: {message}"));
    }

    if let Some(message) = traced_helper_offenders(repo_root)? {
        failures.push(format!("RING-IMPL-TEST-003: {message}"));
    }

    if let Some(message) = multi_requirement_harness_offenders(repo_root)? {
        failures.push(format!("RING-IMPL-TEST-004: {message}"));
    }

    if failures.is_empty() {
        return Ok(());
    }

    let mut message = String::from("repository traceability policy failed");
    for failure in failures {
        message.push_str("\n- ");
        message.push_str(&failure);
    }
    Err(message)
}

fn spec_requirement_format_offenders(
    repo_root: &Path,
    relative_spec_path: &str,
    expected_prefix: &str,
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
        if !item.starts_with(&format!("`{expected_prefix}")) {
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
    }

    if offenders.is_empty() {
        Ok(None)
    } else {
        Ok(Some(offenders.join("; ")))
    }
}

fn inline_test_module_offenders(repo_root: &Path) -> Result<Option<String>, String> {
    let src_root = repo_root.join("src");
    let mut files = Vec::new();
    collect_rust_sources(&src_root, &mut files)?;

    let mut offenders = Vec::new();
    for path in files {
        if is_dedicated_test_file(&path) {
            continue;
        }

        let source = read_text(&path)?;
        if contains_inline_test_body(&source) {
            offenders.push(relative_display(repo_root, &path));
        }
    }

    if offenders.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!(
            "non-test source files still contain inline test bodies: {offenders:?}"
        )))
    }
}

fn multi_requirement_test_offenders(repo_root: &Path) -> Result<Option<String>, String> {
    let src_root = repo_root.join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root)? {
        for entry in parse_test_entries(&path)? {
            if entry.requirement_ids.len() > 1 {
                offenders.push(format!("{} -> {:?}", entry.location, entry.requirement_ids));
            }
        }
    }

    if offenders.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!(
            "top-level test functions still claim multiple requirements: {offenders:?}"
        )))
    }
}

fn traced_helper_offenders(repo_root: &Path) -> Result<Option<String>, String> {
    let src_root = repo_root.join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root)? {
        for helper in parse_traced_helpers(&path)? {
            offenders.push(format!(
                "{} -> {:?}",
                helper.location, helper.requirement_ids
            ));
        }
    }

    if offenders.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!(
            "shared helpers or fixtures still carry traced requirement ids: {offenders:?}"
        )))
    }
}

fn multi_requirement_harness_offenders(repo_root: &Path) -> Result<Option<String>, String> {
    let mut harness_files = Vec::new();
    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            collect_rust_sources(&root, &mut harness_files)?;
        }
    }

    let mut offenders = Vec::new();
    for path in harness_files {
        let source = read_text(&path)?;
        let mut ids = BTreeSet::new();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                ids.insert(id);
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
        Ok(None)
    } else {
        Ok(Some(format!(
            "non-runtime harness entries still claim multiple requirements: {offenders:?}"
        )))
    }
}

fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(dir).map_err(|error| format!("{}: {error}", dir.display()))? {
        let entry = entry.map_err(|error| format!("{}: {error}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, files)?;
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }

    Ok(())
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
            if current.contains("`RING-") && contains_normative_language(&current) {
                items.push(current.trim().to_string());
            }
            current.clear();
            current.push_str(rest);
            continue;
        }

        if current.is_empty() {
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            if current.contains("`RING-") && contains_normative_language(&current) {
                items.push(current.trim().to_string());
            }
            current.clear();
            continue;
        }

        current.push(' ');
        current.push_str(trimmed);
    }

    if current.contains("`RING-") && contains_normative_language(&current) {
        items.push(current.trim().to_string());
    }

    Ok(items)
}

fn strip_numbered_prefix(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }

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

fn contains_inline_test_body(source: &str) -> bool {
    source.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == "#[test]" || trimmed.starts_with("mod tests {")
    })
}

fn is_dedicated_test_file(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) == Some("tests.rs") {
        return true;
    }

    path.components()
        .any(|component| component.as_os_str() == "tests")
}

fn collect_dedicated_test_files(src_root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    collect_rust_sources(src_root, &mut files)?;
    Ok(files
        .into_iter()
        .filter(|path| is_dedicated_test_file(path))
        .collect())
}

fn extract_requirement_ids(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut ids = Vec::new();
    let mut index = 0usize;
    while index + 5 <= bytes.len() {
        if &bytes[index..index + 5] != b"RING-" {
            index += 1;
            continue;
        }

        let mut end = index + 5;
        while end < bytes.len() {
            let byte = bytes[end];
            if byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-' {
                end += 1;
            } else {
                break;
            }
        }

        if end > index + 5 {
            push_unique(&mut ids, text[index..end].to_string());
            index = end;
        } else {
            index += 1;
        }
    }

    ids
}

fn push_unique(ids: &mut Vec<String>, value: String) {
    if !ids.contains(&value) {
        ids.push(value);
    }
}

fn extract_fn_name(line: &str) -> Option<String> {
    let fn_offset = line.find("fn ")?;
    let mut end = fn_offset + 3;
    let bytes = line.as_bytes();
    while end < bytes.len() {
        let byte = bytes[end];
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            end += 1;
        } else {
            break;
        }
    }

    (end > fn_offset + 3).then(|| line[fn_offset + 3..end].to_string())
}

fn collect_requirement_ids_in_lines(lines: &[&str]) -> Vec<String> {
    let mut ids = Vec::new();
    for line in lines {
        if !line.trim().starts_with("//#") {
            continue;
        }

        for id in extract_requirement_ids(line) {
            push_unique(&mut ids, id);
        }
    }
    ids
}

fn parse_test_entries(path: &Path) -> Result<Vec<TestEntry>, String> {
    let source = read_text(path)?;
    let lines: Vec<&str> = source.lines().collect();
    let mut entries = Vec::new();

    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[test]" {
            continue;
        }

        let mut start = index;
        while start > 0 {
            let trimmed = lines[start - 1].trim();
            if trimmed.is_empty() || trimmed.starts_with("//#") || trimmed.starts_with("//=") {
                start -= 1;
            } else {
                break;
            }
        }

        let mut fn_index = index + 1;
        while fn_index < lines.len() {
            let trimmed = lines[fn_index].trim();
            if trimmed.is_empty()
                || trimmed.starts_with("//#")
                || trimmed.starts_with("//=")
                || trimmed.starts_with("#[")
            {
                fn_index += 1;
                continue;
            }
            break;
        }

        let name = if fn_index < lines.len() {
            extract_fn_name(lines[fn_index]).unwrap_or_else(|| "<unknown>".to_string())
        } else {
            "<missing fn>".to_string()
        };
        entries.push(TestEntry {
            location: format!("{}::{name}", path.display()),
            requirement_ids: collect_requirement_ids_in_lines(&lines[start..fn_index]),
        });
    }

    Ok(entries)
}

fn parse_traced_helpers(path: &Path) -> Result<Vec<TestEntry>, String> {
    let source = read_text(path)?;
    let lines: Vec<&str> = source.lines().collect();
    let mut helpers = Vec::new();

    for (index, line) in lines.iter().enumerate() {
        let Some(name) = extract_fn_name(line) else {
            continue;
        };

        let mut start = index;
        while start > 0 {
            let trimmed = lines[start - 1].trim();
            if trimmed.is_empty()
                || trimmed.starts_with("//#")
                || trimmed.starts_with("//=")
                || trimmed.starts_with("#[")
            {
                start -= 1;
            } else {
                break;
            }
        }

        let context = &lines[start..index];
        let is_test = context.iter().any(|line| line.trim() == "#[test]");
        let requirement_ids = collect_requirement_ids_in_lines(context);
        if !is_test && !requirement_ids.is_empty() {
            helpers.push(TestEntry {
                location: format!("{}::{name}", path.display()),
                requirement_ids,
            });
        }
    }

    Ok(helpers)
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

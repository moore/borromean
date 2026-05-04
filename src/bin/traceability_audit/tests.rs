use super::{
    check_annotation_shape, collect_annotation_block, collect_normative_requirement_items,
    contains_inline_test_body, contains_normative_language, extract_requirement_ids,
    inline_test_module_offenders, is_dedicated_test_file, multi_requirement_harness_offenders,
    relative_display, spec_requirement_format_offenders, strip_numbered_prefix, ParsedBlock,
    Summary,
};
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempWorkspace {
    root: PathBuf,
}

impl TempWorkspace {
    fn new(source: &str) -> Self {
        let mut root = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        root.push(format!("borromean-traceability-{}-{unique}", process::id()));
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), source).unwrap();
        Self { root }
    }

    fn write_spec(&self, path: &str, source: &str) {
        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(full_path, source).unwrap();
    }

    fn write_harness(&self, path: &str, source: &str) {
        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(full_path, source).unwrap();
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn parsed_block_empty_requires_all_trace_vectors_to_be_empty() {
    let empty = ParsedBlock::default();
    assert!(empty.is_empty());

    let mut block = ParsedBlock::default();
    block.type_refs.push("test".to_owned());
    assert!(!block.is_empty());

    let mut block = ParsedBlock::default();
    block
        .quote_blocks
        .push("`RING-X-001` MUST hold.".to_owned());
    assert!(!block.is_empty());
}

//= spec/implementation-policy.md#requirements-format
//= type=test
//# `RING-IMPL-FORMAT-001` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST start with a stable identifier such as `RING-IMPL-CORE-001`.
#[test]
fn requirement_ring_impl_format_001_rejects_missing_stable_ids() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. The implementation MUST do the thing.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", "RING-")
        .unwrap()
        .unwrap();

    assert!(error.contains("stable identifier"));
}

//= spec/implementation-policy.md#requirements-format
//= type=test
//# `RING-IMPL-FORMAT-002` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST use explicit RFC-2119 normative language.
#[test]
fn requirement_ring_impl_format_002_rejects_missing_normative_language() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` The implementation keeps this behavior stable.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", "RING-")
        .unwrap()
        .unwrap();

    assert!(error.contains("no normative requirement items"));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-001` Every normative requirement in [spec/ring.md](ring.md) or [spec/implementation.md](implementation.md) MUST have at least one dedicated automated test function or dedicated compile-time test case whose primary purpose is to verify that single requirement.
#[test]
fn requirement_ring_impl_test_001_accepts_single_requirement_and_todo_tests() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_parses_things() {{}}\n\n{eq} spec/ring.md#later\n{eq} type=todo\n{quote} `RING-EXAMPLE-002` The compiler MUST do later work.\n#[test]\nfn todo_later_work() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    let summary = check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(errors, Vec::<String>::new());
    assert_eq!(
        summary,
        Summary {
            requirement_tests: 1,
            todo_tests: 1
        }
    );
}

#[test]
fn rejects_blocks_missing_exactly_one_spec_type_or_quote() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_missing_type() {{}}\n\n{eq} type=test\n{quote} `RING-EXAMPLE-002` The parser MUST parse things.\n#[test]\nfn requirement_missing_spec() {{}}\n\n{eq} spec/ring.md#anchor\n{eq} type=test\n#[test]\nfn requirement_missing_quote() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(errors.len(), 3, "{errors:#?}");
    assert!(errors
        .iter()
        .all(|error| { error.contains("must have exactly one #[test], one //= <spec>.md#") }));
}

#[test]
fn rejects_todo_trace_without_todo_prefix() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=todo\n{quote} `RING-EXAMPLE-001` Later work MUST happen.\n#[test]\nfn requirement_later_work() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors.iter().any(|error| error.contains("todo_ prefix")));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT claim to verify multiple normative requirement identifiers.
#[test]
fn requirement_ring_impl_test_002_rejects_multi_requirement_tests() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n{quote} `RING-EXAMPLE-002` The parser MUST parse other things.\n#[test]\nfn requirement_parses_things() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("multiple requirement identifiers")));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions, macros, and data generators MAY be reused across requirement-specific tests, but the final traced test entry point MUST remain specific to one requirement identifier.
#[test]
fn requirement_ring_impl_test_003_rejects_traced_helpers() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\nfn helper() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("must have exactly one #[test]")));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-004` When a requirement is verified by a compile-fail, compile-pass, or other non-runtime harness, that harness entry MUST still be dedicated to a single requirement identifier.
#[test]
fn requirement_ring_impl_test_004_rejects_multi_requirement_harness_entries() {
    let workspace = TempWorkspace::new("");
    workspace.write_harness(
        "tests/ui/example.rs",
        "//! helper\n//# `RING-EXAMPLE-001` One thing MUST hold.\n//# `MAP-EXAMPLE-001` Another thing MUST hold.\n",
    );

    let error = multi_requirement_harness_offenders(&workspace.root).unwrap();

    assert!(error.contains("multiple requirements") || error.contains("claim multiple"));
}

#[test]
fn single_requirement_harness_entries_are_not_offenders() {
    let workspace = TempWorkspace::new("");
    workspace.write_harness(
        "tests/ui/example.rs",
        "//! helper\n//# `RING-EXAMPLE-001` One thing MUST hold.\n",
    );

    assert_eq!(multi_requirement_harness_offenders(&workspace.root), None);
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test harness entries MUST be defined only in dedicated test modules or files rather than inside the functional implementation module they exercise.
#[test]
fn requirement_ring_impl_test_005_rejects_inline_test_modules() {
    let workspace = TempWorkspace::new("fn helper() {}\n\n#[test]\nfn inline_test() {}\n");

    let error = inline_test_module_offenders(&workspace.root).unwrap();

    assert!(error.contains("non-test source files"));
}

#[test]
fn rejects_detached_annotations() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n\n{eq} type=test\n\n{quote} `RING-EXAMPLE-001` Detached quote.\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(errors.len(), 3, "{errors:#?}");
    assert!(errors
        .iter()
        .all(|error| error.contains("Duvet annotation must be attached")));
}

#[test]
fn rejects_duplicate_requirement_traces() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_one() {{}}\n\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_two() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("duplicates requirement")));
    assert!(errors.iter().any(|error| error.contains("src/lib.rs:6")));
}

#[test]
fn rejects_missing_requirement_prefix() {
    let source = format!(
        "\n{eq} spec/ring.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn parses_things() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("requirement_ prefix")));
}

#[test]
fn preserves_line_numbers_for_attached_annotation_blocks() {
    let lines = [
        "",
        "//= spec/ring.md#anchor",
        "",
        "//= type=test",
        "//# `RING-EXAMPLE-001` Requirement text.",
        "#[test]",
        "fn requirement_example() {}",
    ];

    let block = collect_annotation_block(&lines, 6);

    assert_eq!(block[0].0, 1);
    assert_eq!(block[1].0, 2);
    assert_eq!(block[2].0, 3);
    assert_eq!(block[3].0, 4);
    assert_eq!(block[4].0, 5);
    assert_eq!(block[5].0, 6);
}

#[test]
fn requirement_item_collection_ignores_code_blocks_and_joins_continuations() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "```text\n1. `RING-EXAMPLE-000` This code block MUST be ignored.\n```\n\n1. `RING-EXAMPLE-001` The parser\n   MUST join continuation text.\n\n2. `RING-EXAMPLE-002` Optional notes MAY continue.\n",
    );

    let items =
        collect_normative_requirement_items(&workspace.root.join("spec/example.md")).unwrap();

    assert_eq!(items.len(), 2);
    assert!(items[0].contains("MUST join continuation text"));
    assert!(items[1].starts_with("`RING-EXAMPLE-002`"));
}

#[test]
fn requirement_item_collection_stops_at_markdown_headings() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` The parser MUST stop here.\n# Later Section\nThis prose SHOULD NOT join the prior item.\n",
    );

    let items =
        collect_normative_requirement_items(&workspace.root.join("spec/example.md")).unwrap();

    assert_eq!(
        items,
        vec!["`RING-EXAMPLE-001` The parser MUST stop here.".to_owned()]
    );
}

#[test]
fn numbered_prefix_and_normative_language_helpers_cover_boundaries() {
    assert_eq!(
        strip_numbered_prefix("12. `RING-X-001` The system MUST work."),
        Some("`RING-X-001` The system MUST work.")
    );
    assert_eq!(strip_numbered_prefix("1. short"), Some("short"));
    assert_eq!(strip_numbered_prefix("1."), None);
    assert_eq!(strip_numbered_prefix("1.No space"), None);
    assert_eq!(strip_numbered_prefix("No number. MUST work."), None);

    assert!(contains_normative_language("The system MUST work."));
    assert!(contains_normative_language("The system MUST NOT panic."));
    assert!(contains_normative_language(
        "The system SHOULD report errors."
    ));
    assert!(contains_normative_language("The system MAY defer work."));
    assert!(!contains_normative_language("The system must work."));
}

#[test]
fn inline_and_dedicated_test_file_helpers_classify_paths_precisely() {
    assert!(contains_inline_test_body("#[test]\nfn direct() {}\n"));
    assert!(contains_inline_test_body("mod tests {\n}\n"));
    assert!(!contains_inline_test_body(
        "#[test_case]\nfn generated() {}\n"
    ));

    assert!(is_dedicated_test_file(&PathBuf::from(
        "src/example/tests.rs"
    )));
    assert!(is_dedicated_test_file(&PathBuf::from(
        "src/example/tests/case.rs"
    )));
    assert!(!is_dedicated_test_file(&PathBuf::from(
        "src/example/mod.rs"
    )));
}

#[test]
fn extract_requirement_ids_and_relative_display_are_stable() {
    let ids = extract_requirement_ids(
        "//# `RING-EXAMPLE-001` One thing MUST hold.\n//# `MAP-EXAMPLE-002` Another thing MUST hold.\n//# `RING-EXAMPLE-001` Duplicate mention.",
    );
    assert_eq!(ids, vec!["RING-EXAMPLE-001", "MAP-EXAMPLE-002"]);
    assert_eq!(
        extract_requirement_ids("//# `RING-` is not an id."),
        Vec::<String>::new()
    );

    let root = PathBuf::from("/tmp/trace-root");
    assert_eq!(
        relative_display(&root, &root.join("src/lib.rs")),
        "src/lib.rs"
    );
    assert_eq!(
        relative_display(&root, &PathBuf::from("/elsewhere/file.rs")),
        "/elsewhere/file.rs"
    );
}

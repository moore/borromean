use super::{
    check_annotation_shape, collect_annotation_block, collect_requirement_format_items,
    configured_duvet_specifications, contains_inline_test_body, contains_normative_language,
    extract_all_requirement_ids, extract_requirement_ids, functional_untraced_test_offenders,
    has_stable_identifier_suffix, inline_test_module_offenders, is_dedicated_test_file,
    is_functional_test_file, multi_requirement_harness_offenders, normalize_requirement_whitespace,
    relative_display, requirement_format_failures, spec_format_policy,
    spec_requirement_format_offenders, strip_numbered_prefix, ParsedBlock, Summary,
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

    fn write_file(&self, path: &str, source: &str) {
        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(full_path, source).unwrap();
    }

    fn write_spec(&self, path: &str, source: &str) {
        self.write_file(path, source);
    }

    fn write_harness(&self, path: &str, source: &str) {
        self.write_file(path, source);
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
//# `RING-IMPL-FORMAT-001` Each normative requirement in a repository specification configured for
//# Duvet verification MUST start with a stable identifier such as `RING-IMPL-CORE-001`.
#[test]
fn requirement_ring_impl_format_001_rejects_missing_stable_ids() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` The first behavior MUST keep its identifier.\n\
         2. `RING-EXAMPLE-002` The second behavior MUST also be checked.\n\
         3. The later behavior MUST still start with an identifier.\n\
         4. `RING-` The prefix-only behavior MUST be rejected.\n\
         5. `RING-FOO` The non-numeric identifier behavior MUST be rejected.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", &["RING-"])
        .unwrap()
        .unwrap();

    assert!(error.contains("stable identifier"));
    assert!(error.contains("The later behavior MUST"));
    assert!(error.contains("`RING-`"));
    assert!(error.contains("`RING-FOO`"));
    assert!(!error.contains("RING-EXAMPLE-001"));
    assert!(!error.contains("RING-EXAMPLE-002"));

    workspace.write_file(
        ".duvet/config.toml",
        "[[specification]]\nsource = \"spec/file.md\"\nformat = \"markdown\"\n",
    );
    workspace.write_spec(
        "spec/file.md",
        "1. `RING-FILE-001` File behavior MUST be checked.\n\
         2. File behavior configured through Duvet MUST still have a stable id.\n",
    );

    let failures = requirement_format_failures(&workspace.root).unwrap();

    assert_eq!(
        configured_duvet_specifications(&workspace.root),
        Ok(vec!["spec/file.md".to_owned()])
    );
    assert!(spec_format_policy("spec/file.md").is_some());
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(failures[0].contains("spec/file.md#requirements-format"));
    assert!(failures[0].contains("configured through Duvet"));
}

#[test]
fn configured_duvet_specifications_reports_precise_entry_errors() {
    let workspace = TempWorkspace::new("");
    workspace.write_file(
        ".duvet/config.toml",
        "[[specification]]\nformat = \"markdown\"\n[[other]]\n",
    );

    let error = configured_duvet_specifications(&workspace.root).unwrap_err();

    assert!(error.contains(".duvet/config.toml:3"), "{error}");
    assert!(error.contains("missing source"));

    workspace.write_file(
        ".duvet/config.toml",
        "[[specification]]\nsource = \"spec/a.md\"\nsource = \"spec/b.md\"\n",
    );
    let error = configured_duvet_specifications(&workspace.root).unwrap_err();
    assert!(error.contains(".duvet/config.toml:3"), "{error}");
    assert!(error.contains("duplicate specification source"));

    workspace.write_file(
        ".duvet/config.toml",
        "[[specification]]\nsource = \"spec/a.txt\"\n",
    );
    let error = configured_duvet_specifications(&workspace.root).unwrap_err();
    assert!(error.contains(".duvet/config.toml:2"), "{error}");
    assert!(error.contains("must be markdown"));
}

#[test]
fn spec_format_policy_matches_only_supported_spec_paths() {
    assert_eq!(
        spec_format_policy("spec/ring/00-introduction.md").unwrap(),
        super::SpecFormatPolicy {
            prefixes: &["RING-"],
            allow_empty: true,
        }
    );
    assert_eq!(
        spec_format_policy("spec/ring/09-implementation-coverage.md").unwrap(),
        super::SpecFormatPolicy {
            prefixes: &["RING-"],
            allow_empty: false,
        }
    );
    assert!(spec_format_policy("spec/ring/not-markdown.txt").is_none());
    assert!(spec_format_policy("spec/not-ring/example.md").is_none());
}

//= spec/implementation-policy.md#requirements-format
//= type=test
//# `RING-IMPL-FORMAT-002` Each normative requirement in a repository specification configured for
//# Duvet verification MUST use explicit RFC-2119 normative language.
#[test]
fn requirement_ring_impl_format_002_rejects_missing_normative_language() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` The implementation MUST keep the valid behavior stable.\n\
         2. `RING-EXAMPLE-002` Repository behavior remains stable.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", &["RING-"])
        .unwrap()
        .unwrap();

    assert!(error.contains("explicit normative language"));
    assert!(error.contains("RING-EXAMPLE-002"));
    assert!(!error.contains("no normative requirement items"));
    assert!(!error.contains("RING-EXAMPLE-001"));

    workspace.write_file(
        ".duvet/config.toml",
        "[[specification]]\nsource = \"spec/file.md\"\nformat = \"markdown\"\n",
    );
    workspace.write_spec(
        "spec/file.md",
        "1. `RING-FILE-001` File behavior MUST be checked.\n\
         2. `RING-FILE-002` Repository file behavior remains stable.\n",
    );

    let failures = requirement_format_failures(&workspace.root).unwrap();

    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(failures[0].contains("spec/file.md#requirements-format"));
    assert!(failures[0].contains("explicit normative language"));
    assert!(failures[0].contains("RING-FILE-002"));
}

#[test]
fn requirement_format_rejects_test_name_placeholder_requirements() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` The implementation MUST preserve the functional behavior exercised by the requirement_example regression test.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", &["RING-"])
        .unwrap()
        .unwrap();

    assert!(error.contains("test-name placeholder"));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-001` Every normative requirement in a repository specification configured for
//# Duvet verification MUST have at least one dedicated automated test function or dedicated
//# compile-time test case whose primary purpose is to verify that single requirement.
#[test]
fn requirement_ring_impl_test_001_accepts_single_requirement_and_todo_tests() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_parses_things() {{}}\n\n{eq} spec/ring/00-introduction.md#later\n{eq} type=todo\n{quote} `RING-EXAMPLE-002` The compiler MUST do later work.\n#[test]\nfn todo_later_work() {{}}\n",
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

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let verify = fs::read_to_string(repo_root.join("scripts/verify.sh")).unwrap();
    let tasks = fs::read_to_string(repo_root.join("tasks.sh")).unwrap();
    for source in [&verify, &tasks] {
        assert!(source.contains("cargo run --quiet --bin traceability_audit -- check-requirements"));
        assert!(
            source.contains("duvet report --config-path .duvet/config.toml --require-tests true")
        );
    }

    let configured_specs = configured_duvet_specifications(&repo_root).unwrap();
    assert!(configured_specs.contains(&"spec/implementation-policy.md".to_owned()));
    assert!(configured_specs.contains(&"spec/file.md".to_owned()));
    assert!(configured_specs.contains(&"spec/embedded-storage.md".to_owned()));
    assert!(configured_specs
        .iter()
        .all(|spec| spec_format_policy(spec).is_some()));
}

#[test]
fn rejects_blocks_missing_exactly_one_spec_type_or_quote() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_missing_type() {{}}\n\n{eq} type=test\n{quote} `RING-EXAMPLE-002` The parser MUST parse things.\n#[test]\nfn requirement_missing_spec() {{}}\n\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n#[test]\nfn requirement_missing_quote() {{}}\n",
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
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=todo\n{quote} `RING-EXAMPLE-001` Later work MUST happen.\n#[test]\nfn requirement_later_work() {{}}\n",
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
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT claim to verify multiple
//# normative requirement identifiers.
#[test]
fn requirement_ring_impl_test_002_rejects_multi_requirement_tests() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n{quote} `RING-EXAMPLE-002` The parser MUST parse other things.\n#[test]\nfn requirement_parses_things() {{}}\n",
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
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions, macros, and data generators MAY
//# be reused across requirement-specific tests, but the final traced test entry point MUST remain
//# specific to one requirement identifier.
#[test]
fn requirement_ring_impl_test_003_rejects_traced_helpers() {
    let source = format!(
        "\nfn fixture_value() -> u8 {{ 1 }}\n\n\
         macro_rules! assert_nonzero {{\n\
             ($value:expr) => {{\n\
                 assert!($value > 0);\n\
             }};\n\
         }}\n\n\
         fn generated_values() -> [u8; 2] {{ [fixture_value(), 2] }}\n\n\
         {eq} spec/ring/00-introduction.md#anchor\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-001` The parser MUST parse things.\n\
         #[test]\n\
         fn requirement_shared_helper_one() {{\n\
             assert_nonzero!(generated_values()[0]);\n\
         }}\n\n\
         {eq} spec/ring/00-introduction.md#later\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-002` The parser MUST parse other things.\n\
         #[test]\n\
         fn requirement_shared_helper_two() {{\n\
             assert_nonzero!(generated_values()[1]);\n\
         }}\n\n\
         {eq} spec/ring/00-introduction.md#helper\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-003` A helper MUST not be the final traced entry point.\n\
         fn helper() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    let summary = check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(
        summary,
        Summary {
            requirement_tests: 2,
            todo_tests: 0
        }
    );
    assert!(errors
        .iter()
        .any(|error| error.contains("must have exactly one #[test]")));
    assert_eq!(errors.len(), 1, "{errors:#?}");
    assert!(errors.iter().any(|error| error.contains("helper")));
    assert!(!errors
        .iter()
        .any(|error| error.contains("requirement_shared_helper_one")));
    assert!(!errors
        .iter()
        .any(|error| error.contains("requirement_shared_helper_two")));
    assert!(!errors.iter().any(|error| error.contains("fixture_value")));
    assert!(!errors
        .iter()
        .any(|error| error.contains("generated_values")));
    assert!(!errors.iter().any(|error| error.contains("assert_nonzero")));
    assert!(!errors
        .iter()
        .any(|error| error.contains("multiple requirement identifiers")));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-004` When a requirement is verified by a compile-fail, compile-pass, or other
//# non-runtime harness, that harness entry MUST still be dedicated to a single requirement
//# identifier.
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
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test harness entries MUST be
//# defined only in dedicated test modules or files rather than inside the functional implementation
//# module they exercise.
#[test]
fn requirement_ring_impl_test_005_rejects_inline_test_modules() {
    let workspace = TempWorkspace::new("fn helper() {}\n\n#[test]\nfn inline_test() {}\n");
    workspace.write_harness(
        "src/functional.rs",
        "#[cfg(test)]\nmod tests {\n    #[test]\n    fn nested_inline_test() {}\n}\n",
    );
    workspace.write_harness(
        "src/functional_harness.rs",
        "//# `RING-EXAMPLE-001` Compile harness behavior MUST stay dedicated.\n",
    );
    workspace.write_harness(
        "src/storage/tests.rs",
        "#[test]\nfn dedicated_runtime_test() {}\n",
    );
    workspace.write_harness(
        "tests/ui/compile_pass.rs",
        "//# `RING-EXAMPLE-001` Compile harness behavior MUST stay dedicated.\n",
    );

    let error = inline_test_module_offenders(&workspace.root).unwrap();

    assert!(error.contains("non-test source files"));
    assert!(error.contains("src/lib.rs"));
    assert!(error.contains("src/functional.rs"));
    assert!(error.contains("src/functional_harness.rs"));
    assert!(!error.contains("src/storage/tests.rs"));
    assert!(!error.contains("tests/ui/compile_pass.rs"));
}

//= spec/implementation-policy.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-006` Functional library test entry points MUST be
//# requirement-derived and traced with exactly one Duvet requirement or
//# todo block; untraced `#[test]` functions MAY exist only in
//# tooling-specific test suites that verify repository tooling rather
//# than functional library behavior.
#[test]
fn requirement_ring_impl_test_006_rejects_untraced_functional_test_entries() {
    let workspace = TempWorkspace::new("");
    let traced_source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-001` The storage layer MUST preserve data.\n\
         #[test]\n\
         fn requirement_storage_regression() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#todo\n\
         {eq} type=todo\n\
         {quote} `RING-EXAMPLE-002` Later storage work MUST be tracked.\n\
         #[test]\n\
         fn todo_storage_followup() {{}}\n\n\
         #[test]\n\
         fn storage_regression() {{}}\n\n\
         fn helper() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    workspace.write_harness("src/storage/tests.rs", &traced_source);
    workspace.write_harness(
        "src/bin/traceability_audit/tests.rs",
        "\n#[test]\nfn checker_unit_test() {}\n",
    );
    workspace.write_harness(
        "src/tests/functional.rs",
        "\n#[test]\nfn cross_module_regression() {}\n",
    );
    workspace.write_harness(
        "tests/functional.rs",
        "\n#[test]\nfn top_level_functional_regression() {}\n",
    );
    workspace.write_harness(
        "src/bin/non_tool/tests.rs",
        "\n#[test]\nfn non_tooling_bin_regression() {}\n",
    );

    let error = functional_untraced_test_offenders(&workspace.root).unwrap();

    assert!(error.contains("src/storage/tests.rs"));
    assert!(error.contains("storage_regression"));
    assert!(error.contains("src/tests/functional.rs"));
    assert!(error.contains("cross_module_regression"));
    assert!(error.contains("tests/functional.rs"));
    assert!(error.contains("top_level_functional_regression"));
    assert!(error.contains("src/bin/non_tool/tests.rs"));
    assert!(error.contains("non_tooling_bin_regression"));
    assert!(!error.contains("requirement_storage_regression"));
    assert!(!error.contains("todo_storage_followup"));
    assert!(!error.contains("checker_unit_test"));
    assert!(!error.contains("helper"));

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

    let malformed = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n\
         {eq} type=test\n\
         {eq} type=todo\n\
         {quote} `RING-EXAMPLE-003` Duplicate trace metadata MUST be rejected.\n\
         #[test]\n\
         fn requirement_duplicate_type() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#later\n\
         {quote} `RING-EXAMPLE-004` Missing trace type MUST be rejected.\n\
         #[test]\n\
         fn requirement_missing_type() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#todo-prefix\n\
         {eq} type=todo\n\
         {quote} `RING-EXAMPLE-005` Todo traces MUST use the todo prefix.\n\
         #[test]\n\
         fn requirement_wrong_todo_prefix() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#wrong-prefix\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-006` Requirement traces MUST use the requirement prefix.\n\
         #[test]\n\
         fn parses_without_requirement_prefix() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#duplicate-spec\n\
         {eq} spec/ring/00-introduction.md#duplicate-spec-2\n\
         {eq} type=test\n\
         {quote} `RING-EXAMPLE-007` Duplicate spec refs MUST be rejected.\n\
         #[test]\n\
         fn requirement_duplicate_spec() {{}}\n\n\
         {eq} spec/ring/00-introduction.md#missing-quote\n\
         {eq} type=test\n\
         #[test]\n\
         fn requirement_missing_quote() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let malformed_workspace = TempWorkspace::new("");
    malformed_workspace.write_harness("src/storage/tests.rs", &malformed);
    let mut malformed_errors = Vec::new();

    check_annotation_shape(&malformed_workspace.root, &mut malformed_errors);

    assert!(malformed_errors
        .iter()
        .any(|error| error.contains("must have exactly one #[test]")));
    assert!(malformed_errors
        .iter()
        .any(|error| error.contains("todo_ prefix")));
    assert!(malformed_errors
        .iter()
        .any(|error| error.contains("requirement_ prefix")));
    assert!(malformed_errors
        .iter()
        .any(|error| error.contains("spec=2")));
    assert!(malformed_errors
        .iter()
        .any(|error| error.contains("quote=0")));
}

#[test]
fn functional_untraced_test_checker_accepts_traced_entries_and_tooling_tests() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The storage layer MUST preserve data.\n#[test]\nfn requirement_storage_regression() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new("");
    workspace.write_harness("src/storage/tests.rs", &source);
    workspace.write_harness(
        "src/bin/traceability_audit/tests.rs",
        "\n#[test]\nfn checker_unit_test() {}\n",
    );

    assert_eq!(functional_untraced_test_offenders(&workspace.root), None);
}

#[test]
fn rejects_detached_annotations() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n\n{eq} type=test\n\n{quote} `RING-EXAMPLE-001` Detached quote.\n",
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
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_one() {{}}\n\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn requirement_two() {{}}\n",
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
fn rejects_duplicate_requirement_traces_with_only_wrapping_differences() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse wrapped citations.\n#[test]\nfn requirement_one() {{}}\n\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse\n{quote} wrapped citations.\n#[test]\nfn requirement_two() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("duplicates requirement")));
}

#[test]
fn accepts_spec_quotes_with_only_wrapping_differences() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse\n{quote} wrapped citations.\n#[test]\nfn requirement_wrapped_quote() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    workspace.write_spec(
        "spec/ring/00-introduction.md",
        "1. `RING-EXAMPLE-001` The parser MUST parse wrapped citations.\n",
    );
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(errors, Vec::<String>::new());
}

#[test]
fn rejects_unknown_requirement_ids_and_unreadable_spec_files() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-999` Missing ids MUST be rejected.\n#[test]\nfn requirement_missing_spec_id() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    workspace.write_spec(
        "spec/ring/00-introduction.md",
        "1. `RING-EXAMPLE-001` Present ids MUST pass.\n",
    );
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert_eq!(errors.len(), 1, "{errors:#?}");
    assert!(errors[0].contains("quotes an identifier that does not exist"));
    assert!(errors[0].contains("RING-EXAMPLE-999"));

    let unreadable_source = format!(
        "\n{eq} spec/unreadable.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` Unreadable specs MUST be reported.\n#[test]\nfn requirement_unreadable_spec() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let unreadable_workspace = TempWorkspace::new(&unreadable_source);
    fs::create_dir_all(unreadable_workspace.root.join("spec/unreadable.md")).unwrap();
    let mut errors = Vec::new();

    check_annotation_shape(&unreadable_workspace.root, &mut errors);

    assert_eq!(errors.len(), 1, "{errors:#?}");
    assert!(errors[0].contains("spec/unreadable.md"));
}

#[test]
fn rejects_missing_requirement_prefix() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The parser MUST parse things.\n#[test]\nfn parses_things() {{}}\n",
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
fn rejects_trace_quotes_that_are_test_name_placeholders() {
    let source = format!(
        "\n{eq} spec/ring/00-introduction.md#anchor\n{eq} type=test\n{quote} `RING-EXAMPLE-001` The implementation MUST preserve the functional behavior exercised by the requirement_example regression test.\n#[test]\nfn requirement_example() {{}}\n",
        eq = "//=",
        quote = "//#"
    );
    let workspace = TempWorkspace::new(&source);
    let mut errors = Vec::new();

    check_annotation_shape(&workspace.root, &mut errors);

    assert!(errors
        .iter()
        .any(|error| error.contains("test-name placeholder")));
}

#[test]
fn preserves_line_numbers_for_attached_annotation_blocks() {
    let lines = [
        "",
        "//= spec/ring/00-introduction.md#anchor",
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
        collect_requirement_format_items(&workspace.root.join("spec/example.md"), &["RING-"])
            .unwrap();

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
        collect_requirement_format_items(&workspace.root.join("spec/example.md"), &["RING-"])
            .unwrap();

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
    assert!(contains_normative_language("The system MUST."));
    assert!(!contains_normative_language("The system must work."));
    assert_eq!(
        normalize_requirement_whitespace("`RING-X-001` The system\nMUST work."),
        "`RING-X-001` The system MUST work."
    );
}

#[test]
fn requirement_format_identifier_boundaries_are_enforced() {
    let workspace = TempWorkspace::new("");
    workspace.write_spec(
        "spec/example.md",
        "1. `RING-EXAMPLE-001` Numeric identifiers MUST pass.\n\
         2. `RING-EXAMPLE-001A` Letter-suffixed split identifiers MUST pass.\n\
         3. `RING-` Prefix-only identifiers MUST fail.\n\
         4. `RING-EXAMPLE` Non-numeric final segments MUST fail.\n\
         5. `MAP-EXAMPLE-001` Wrong-prefix identifiers MUST fail.\n",
    );

    let error = spec_requirement_format_offenders(&workspace.root, "spec/example.md", &["RING-"])
        .unwrap()
        .unwrap();

    assert!(error.contains("`RING-`"));
    assert!(error.contains("`RING-EXAMPLE`"));
    assert!(error.contains("`MAP-EXAMPLE-001`"));
    assert!(!error.contains("RING-EXAMPLE-001` Numeric"));
    assert!(!error.contains("RING-EXAMPLE-001A"));
}

#[test]
fn stable_identifier_suffix_boundaries_are_enforced() {
    assert!(has_stable_identifier_suffix("001"));
    assert!(has_stable_identifier_suffix("EXAMPLE-001"));
    assert!(has_stable_identifier_suffix("EXAMPLE-001A"));
    assert!(!has_stable_identifier_suffix(""));
    assert!(!has_stable_identifier_suffix("-001"));
    assert!(!has_stable_identifier_suffix("001-"));
    assert!(!has_stable_identifier_suffix("EXAMPLE"));
    assert!(!has_stable_identifier_suffix("EXAMPLE-001AB"));
    assert!(!has_stable_identifier_suffix("EXAMPLE-001a"));
}

#[test]
fn inline_and_dedicated_test_file_helpers_classify_paths_precisely() {
    assert!(contains_inline_test_body("#[test]\nfn direct() {}\n"));
    assert!(contains_inline_test_body("mod tests {\n}\n"));
    assert!(contains_inline_test_body(
        "//# `RING-EXAMPLE-001` Harness behavior MUST be dedicated.\n"
    ));
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

    let root = PathBuf::from("/repo");
    assert!(is_functional_test_file(
        &root,
        &root.join("src/storage/tests.rs")
    ));
    assert!(is_functional_test_file(
        &root,
        &root.join("src/tests/traceability/io.rs")
    ));
    assert!(!is_functional_test_file(
        &root,
        &root.join("src/bin/traceability_audit/tests.rs")
    ));
    assert!(!is_functional_test_file(
        &root,
        &root.join("src/bin/file_backing_perf/tests.rs")
    ));
    assert!(is_functional_test_file(
        &root,
        &root.join("src/bin/non_tool/tests.rs")
    ));
    assert!(!is_functional_test_file(
        &root,
        &root.join("tests/traceability_audit_cli.rs")
    ));
    assert!(is_functional_test_file(
        &root,
        &root.join("tests/functional.rs")
    ));
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
    assert_eq!(
        extract_all_requirement_ids("1. `RING-EXAMPLE-003` Numbered specs work."),
        vec!["RING-EXAMPLE-003"]
    );
    assert_eq!(
        extract_all_requirement_ids("1. `RING-` Prefix-only specs are ignored."),
        Vec::<String>::new()
    );
    assert_eq!(
        extract_all_requirement_ids("1. `MAP-` Prefix-only map specs are ignored."),
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

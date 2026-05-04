use std::process::Command;

fn traceability_audit() -> Command {
    Command::new(env!("CARGO_BIN_EXE_traceability_audit"))
}

#[test]
fn traceability_audit_cli_accepts_no_args_and_explicit_check() {
    for args in [Vec::<&str>::new(), vec!["check-requirements"]] {
        let output = traceability_audit().args(args).output().unwrap();

        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("validated "));
        assert!(stdout.contains(" requirement tests"));
        assert!(stdout.contains(" todo tests"));
    }
}

#[test]
fn traceability_audit_cli_rejects_unknown_or_extra_args() {
    for args in [
        ["bogus"].as_slice(),
        ["check-requirements", "extra"].as_slice(),
    ] {
        let output = traceability_audit().args(args).output().unwrap();

        assert_eq!(output.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("usage: cargo run --bin traceability_audit -- check-requirements"));
    }
}

use std::process::Command;

#[test]
fn bench_subcommand_runs_with_a_small_local_dataset() {
    let output = Command::new(env!("CARGO_BIN_EXE_lsmkv-cli"))
        .args(["bench", "--keys", "120", "--seconds", "1", "--workload", "read-hit"])
        .output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Preparing 120 keys"));
    assert!(stdout.contains("read-hit:"));
}

#[test]
fn bench_subcommand_rejects_invalid_inputs() {
    let status = Command::new(env!("CARGO_BIN_EXE_lsmkv-cli"))
        .args(["bench", "--keys", "5"])
        .status().unwrap();
    assert!(!status.success());
}

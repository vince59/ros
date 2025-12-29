use std::process::Command;

fn run(bin: &str) {
    let status = Command::new("cargo")
        .args(["run", "-p", bin])
        .status()
        .expect("run fail");

    if !status.success() {
        panic!("{} failed", bin);
    }
}

fn main() {
    run("master");
    run("tester");
    run("logger");
}

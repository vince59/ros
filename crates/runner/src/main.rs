use std::process::{Child, Command};

fn spawn(package: &str, bin_args: &[&str]) -> Child {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("-p")
        .arg(package)
        .arg("--"); // tout ce qui suit va au binaire

    cmd.args(bin_args)
        .spawn()
        .expect("failed to spawn")
}

fn main() {
    println!("[runner] Starting all processes...");

    let _master = spawn("master", &["--port", "8080"]);
    let _logger = spawn(
        "logger",
        &["--master", "http://127.0.0.1:8080", "--port", "9001"],
    );
    let _tester = spawn(
        "tester",
        &["--master", "http://127.0.0.1:8080", "--port", "9002"],
    );
    println!("[runner] All processes started. Ctrl+C to stop.");
    std::thread::park();
}

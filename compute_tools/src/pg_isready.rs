use anyhow::{Context, anyhow};
use std::net::TcpStream;

pub fn pg_isready(bin: &str, port: u16) -> anyhow::Result<()> {
    if bin.contains("gaussdb") || bin.contains("openGauss") || bin.ends_with("gsql") {
        og_isready(port)
    } else {
        let child_result = std::process::Command::new(bin)
            .arg("-p")
            .arg(port.to_string())
            .spawn();

        child_result
            .context("spawn() failed")
            .and_then(|mut child| child.wait().context("wait() failed"))
            .and_then(|status| match status.success() {
                true => Ok(()),
                false => Err(anyhow!("process exited with {status}")),
            })
            .with_context(|| format!("could not run `{bin} --port {port}`"))
    }
}

fn og_isready(port: u16) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{port}");
    TcpStream::connect(&addr)
        .map(|_| ())
        .with_context(|| format!("could not connect to openGauss at {addr}"))
}

pub fn get_pg_isready_bin(pgbin: &str) -> String {
    let split = pgbin.split("/").collect::<Vec<&str>>();
    let parent = split[0..split.len() - 1].join("/");
    if pgbin.contains("gaussdb") || pgbin.contains("openGauss") {
        format!("{}/gaussdb", parent)
    } else {
        format!("{}/pg_isready", parent)
    }
}

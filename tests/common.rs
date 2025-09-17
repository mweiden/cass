use std::ffi::OsStr;
use std::process::{Child, Command, Stdio};

pub struct CassProcess {
    child: Child,
}

impl CassProcess {
    pub fn spawn<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cass"));
        command.args(args);
        command.arg("--commitlog-sync-period-ms");
        command.arg("0");
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn server");
        Self { child }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for CassProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Allocate a free loopback HTTP address like "http://127.0.0.1:<ephemeral>".
#[allow(dead_code)]
pub fn free_http_addr() -> String {
    use std::net::TcpListener;
    // Bind to port 0 to let the OS choose an available port, then release it.
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind 127.0.0.1:0");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    format!("http://127.0.0.1:{}", port)
}

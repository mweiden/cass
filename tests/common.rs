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
        let child = Command::new(env!("CARGO_BIN_EXE_cass"))
            .args(args)
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

//! Process-crash recovery test. The child is this test binary in helper mode.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use lsmkv::wal::SyncPolicy;
use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("crash-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() { let _ = self.0.kill(); }
        let _ = self.0.wait();
    }
}

fn options() -> Options {
    Options { sync_policy: SyncPolicy::EveryWrite, memtable_size: 1024,
        l0_compaction_trigger: 2, l0_stop_writes_trigger: 4, target_file_size: 2048,
        ..Options::default() }
}

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("LSMKV_CRASH_CHILD") else { return };
    let db = Db::open(path, options()).unwrap();
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "LSMKV_READY").unwrap();
    stdout.flush().unwrap();
    for sequence in 0..u64::MAX {
        let bytes = sequence.to_be_bytes();
        db.put(bytes, bytes).unwrap();
        writeln!(stdout, "LSMKV_ACK {sequence}").unwrap();
        stdout.flush().unwrap();
    }
}

#[test]
#[cfg(unix)]
fn sigkill_recovery_preserves_acknowledged_prefix_for_200_crashes() {
    let mut random = 0x6b8b_4567_327b_23c6u64;
    for iteration in 0..200 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let target = 1 + random % 20;
        let delay = Duration::from_millis((random >> 8) % 12);
        let dir = TestDir::new();
        let mut child = ChildGuard(Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("crash_child").arg("--nocapture")
            .env("LSMKV_CRASH_CHILD", &dir.0)
            .stdout(Stdio::piped()).spawn().unwrap());
        let pipe = child.0.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut input = BufReader::new(pipe);
            loop {
                let mut bytes = Vec::new();
                match input.read_until(b'\n', &mut bytes) {
                    Ok(0) => break,
                    Ok(_) if bytes.last() != Some(&b'\n') => break,
                    Ok(_) => {
                        bytes.pop();
                        match String::from_utf8(bytes) {
                            Ok(line) => { if send.send(line).is_err() { break; } }
                            Err(error) => { let _ = send.send(format!("LSMKV_PIPE_ERROR {error}")); break; }
                        }
                    }
                    Err(error) => { let _ = send.send(format!("LSMKV_PIPE_ERROR {error}")); break; }
                }
            }
        });
        let mut acknowledged = 0u64;
        let mut ready = false;
        while !ready || acknowledged < target {
            let line = receive.recv_timeout(Duration::from_secs(30)).expect("child stopped before crash point");
            if line.contains("LSMKV_READY") { ready = true; }
            if let Some(number) = line.strip_prefix("LSMKV_ACK ") {
                assert_eq!(number.parse::<u64>().unwrap(), acknowledged, "iteration {iteration}");
                acknowledged += 1;
            }
            assert!(!line.starts_with("LSMKV_PIPE_ERROR"), "{line}");
        }
        std::thread::sleep(delay);
        // On Unix Child::kill sends SIGKILL, not a graceful termination signal.
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        reader.join().unwrap();
        for line in receive.try_iter() {
            if let Some(number) = line.strip_prefix("LSMKV_ACK ") {
                assert_eq!(number.parse::<u64>().unwrap(), acknowledged, "iteration {iteration}");
                acknowledged += 1;
            }
            assert!(!line.starts_with("LSMKV_PIPE_ERROR"), "{line}");
        }
        let db = Db::open(&dir.0, options()).unwrap();
        let entries = db.scan(..).unwrap();
        assert!(entries.len() as u64 >= acknowledged, "lost acknowledged write in iteration {iteration}");
        assert!(entries.len() as u64 <= acknowledged + 1, "non-prefix unacknowledged writes in iteration {iteration}");
        for (sequence, (key, value)) in entries.iter().enumerate() {
            let bytes = (sequence as u64).to_be_bytes().to_vec();
            assert_eq!(key, &bytes, "key gap in iteration {iteration}");
            assert_eq!(value, &bytes, "value mismatch in iteration {iteration}");
        }
    }
}

use std::error::Error;
use std::io::{self, Write};
use std::ops::Bound;
use std::process::ExitCode;
use std::time::Duration;

use lsmkv::benchmark::{run_for, Dataset, Workload, DEFAULT_KEYS};
use lsmkv::{Db, Options};

const USAGE: &str = "Usage:\n  lsmkv-cli put DB KEY VALUE\n  lsmkv-cli get DB KEY\n  lsmkv-cli delete DB KEY\n  lsmkv-cli scan DB [START [END]]\n  lsmkv-cli stats DB\n  lsmkv-cli compact DB\n  lsmkv-cli verify DB\n  lsmkv-cli bench [--seconds N] [--keys N] [--workload sequential-write|random-write|read-hit|read-miss|scan-100]";

struct BenchArgs {
    seconds: u64,
    keys: u64,
    workload: Option<Workload>,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<BenchArgs>, io::Error> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        None | Some("--help") | Some("-h") => return Ok(None),
        Some("bench") => {}
        Some(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE)),
    }
    let mut result = BenchArgs { seconds: 60, keys: DEFAULT_KEYS, workload: None };
    while let Some(flag) = args.next() {
        if flag == "--help" || flag == "-h" { return Ok(None); }
        let value = args.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing option value"))?;
        match flag.as_str() {
            "--seconds" => result.seconds = value.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid duration"))?,
            "--keys" => result.keys = value.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key count"))?,
            "--workload" => result.workload = Some(value.parse()?),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown option")),
        }
    }
    if result.seconds == 0 || !(100..=u64::MAX / 2).contains(&result.keys) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "duration must be positive and key count must be at least 100"));
    }
    Ok(Some(result))
}

fn run_bench(args: BenchArgs, output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    writeln!(output, "Preparing {} keys with 100-byte values", args.keys)?;
    output.flush()?;
    let dataset = Dataset::new(args.keys)?;
    for workload in Workload::ALL {
        if args.workload.is_some_and(|selected| selected != workload) { continue; }
        let (operations, elapsed) = run_for(&dataset, workload, Duration::from_secs(args.seconds))?;
        writeln!(output, "{}: {} operations in {:.3}s ({:.2} operations/s)", workload.name(), operations, elapsed.as_secs_f64(), operations as f64 / elapsed.as_secs_f64())?;
    }
    output.flush()?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for &byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args_os().skip(1).map(|value| {
        value.into_string().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "arguments must be UTF-8"))
    }).collect::<Result<_, _>>()?;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    let Some(command) = args.first().map(String::as_str) else {
        writeln!(output, "{USAGE}")?;
        output.flush()?;
        return Ok(());
    };
    if command == "--help" || command == "-h" {
        writeln!(output, "{USAGE}")?;
        output.flush()?;
        return Ok(());
    }
    if command == "bench" {
        if let Some(args) = parse_args(args.into_iter())? { return run_bench(args, &mut output); }
        writeln!(output, "{USAGE}")?;
        output.flush()?;
        return Ok(());
    }
    let valid = match command {
        "put" => args.len() == 4,
        "get" | "delete" => args.len() == 3,
        "scan" => (2..=4).contains(&args.len()),
        "stats" | "compact" | "verify" => args.len() == 2,
        _ => false,
    };
    if !valid { return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE).into()); }
    let path = &args[1];
    if command == "verify" {
        let report = Db::verify_path(path, Options::default())?;
        writeln!(output, "verified: {} manifest, {} WAL, {} SSTable files", report.manifest_files, report.wal_files, report.sstable_files)?;
        output.flush()?;
        return Ok(());
    }
    let options = Options::default();
    if command != "put" && !options.fs.exists(&std::path::Path::new(path).join("CURRENT"))? {
        return Err(io::Error::new(io::ErrorKind::NotFound, "database does not exist").into());
    }
    let db = Db::open(path, options)?;
    match command {
        "put" => db.put(args[2].as_bytes(), args[3].as_bytes())?,
        "get" => {
            let value = db.get(args[2].as_bytes())?.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "key not found"))?;
            output.write_all(&value)?;
            output.write_all(b"\n")?;
        }
        "delete" => db.delete(args[2].as_bytes())?,
        "scan" => {
            let start = args.get(2).map_or(Bound::Unbounded, |value| Bound::Included(value.as_bytes().to_vec()));
            let end = args.get(3).map_or(Bound::Unbounded, |value| Bound::Excluded(value.as_bytes().to_vec()));
            if let (Bound::Included(start), Bound::Excluded(end)) = (&start, &end) {
                if start > end { return Err(io::Error::new(io::ErrorKind::InvalidInput, "scan start must not exceed end").into()); }
            }
            for (key, value) in db.scan((start, end))? { writeln!(output, "{}\t{}", hex(&key), hex(&value))?; }
        }
        "stats" => {
            let stats = db.stats();
            if !stats.files_per_level.contains_key(&0) {
                writeln!(output, "level 0: 0 files, 0 bytes")?;
            }
            for (level, files) in &stats.files_per_level {
                let bytes = stats.live_bytes_per_level.get(level).map_or(0, |value| *value);
                writeln!(output, "level {level}: {files} files, {bytes} bytes")?;
            }
            writeln!(output, "write amplification: {:.6}", stats.write_amplification)?;
            writeln!(output, "cache hit rate: {:.6} ({} hits, {} misses)", stats.cache_hit_rate, stats.cache_hits, stats.cache_misses)?;
            writeln!(output, "bloom block reads avoided: {}", stats.bloom_filter_block_reads_avoided)?;
            writeln!(output, "logical bytes written: {}", stats.logical_bytes_written)?;
        }
        "compact" => { db.compact()?; writeln!(output, "compaction complete")?; }
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE).into()),
    }
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("lsmkv-cli: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_args;
    use lsmkv::benchmark::{Workload, DEFAULT_KEYS};

    #[test]
    fn parses_bench_defaults_and_options() {
        let defaults = parse_args(["bench".into()]).unwrap().unwrap();
        assert_eq!(defaults.keys, DEFAULT_KEYS);
        assert_eq!(defaults.seconds, 60);
        let args = parse_args(["bench", "--keys", "120", "--seconds", "1", "--workload", "read-hit"].map(str::to_owned)).unwrap().unwrap();
        assert_eq!(args.keys, 120);
        assert_eq!(args.seconds, 1);
        assert_eq!(args.workload, Some(Workload::ReadHit));
    }

    #[test]
    fn rejects_bad_arguments() {
        assert!(parse_args(["bench", "--keys", "2"].map(str::to_owned)).is_err());
        assert!(parse_args(["bench", "--seconds", "0"].map(str::to_owned)).is_err());
        assert!(parse_args(["bench", "--workload", "invalid"].map(str::to_owned)).is_err());
        assert!(parse_args(["unknown".into()]).is_err());
    }
}

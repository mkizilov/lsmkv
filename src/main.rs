use std::error::Error;
use std::io;
use std::process::ExitCode;
use std::time::Duration;

use lsmkv::benchmark::{run_for, Dataset, Workload, DEFAULT_KEYS};

const USAGE: &str = "Usage: lsmkv-cli bench [--seconds N] [--keys N] [--workload sequential-write|random-write|read-hit|read-miss|scan-100]";

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

fn run() -> Result<(), Box<dyn Error>> {
    let Some(args) = parse_args(std::env::args().skip(1))? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("Preparing {} keys with 100-byte values", args.keys);
    let dataset = Dataset::new(args.keys)?;
    for workload in Workload::ALL {
        if args.workload.is_some_and(|selected| selected != workload) { continue; }
        let (operations, elapsed) = run_for(&dataset, workload, Duration::from_secs(args.seconds))?;
        println!("{}: {} operations in {:.3}s ({:.2} operations/s)", workload.name(), operations, elapsed.as_secs_f64(), operations as f64 / elapsed.as_secs_f64());
    }
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

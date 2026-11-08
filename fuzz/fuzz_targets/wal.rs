#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = lsmkv::fuzzing::wal(data);
    let _ = lsmkv::fuzzing::wal_payload(data);
});

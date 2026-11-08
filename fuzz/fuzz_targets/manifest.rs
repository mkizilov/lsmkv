#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = lsmkv::fuzzing::manifest_edit(data);
    let _ = lsmkv::fuzzing::manifest_log(data);
});

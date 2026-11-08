#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = lsmkv::fuzzing::sstable_footer(data);
    let _ = lsmkv::fuzzing::sstable_block(data);
    let _ = lsmkv::fuzzing::sstable_filter(data);
});

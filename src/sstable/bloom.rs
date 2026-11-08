//! Persistent per-table Bloom filters over user keys.

use std::io;

use crate::encoding::{get_u64, put_u64};
use crate::Error;

const VERSION: u8 = 1;

pub(super) fn hash(key: &[u8]) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for &byte in key {
        value ^= u64::from(byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(value)
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn positions(hash: u64, bit_count: u64, probes: u8) -> impl Iterator<Item = u64> {
    let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
    (0..probes).map(move |i| hash.wrapping_add(u64::from(i).wrapping_mul(step)) % bit_count)
}

pub(super) fn build(hashes: &[u64], bits_per_key: usize) -> io::Result<Vec<u8>> {
    if bits_per_key == 0 {
        return Ok(Vec::new());
    }
    let bits = hashes
        .len()
        .checked_mul(bits_per_key)
        .ok_or_else(|| io::Error::other("Bloom filter too large"))?
        .max(64);
    let byte_count = bits
        .checked_add(7)
        .ok_or_else(|| io::Error::other("Bloom filter too large"))?
        / 8;
    let bit_count = u64::try_from(byte_count)
        .ok()
        .and_then(|n| n.checked_mul(8))
        .ok_or_else(|| io::Error::other("Bloom filter too large"))?;
    let probes = ((bits_per_key * 69 + 50) / 100).clamp(1, 30) as u8;
    let mut output = Vec::new();
    let capacity = byte_count
        .checked_add(10)
        .ok_or_else(|| io::Error::other("Bloom filter too large"))?;
    output
        .try_reserve_exact(capacity)
        .map_err(|_| io::Error::other("Bloom filter allocation failed"))?;
    output.push(VERSION);
    output.push(probes);
    put_u64(&mut output, bit_count);
    output.resize(capacity, 0);
    for &value in hashes {
        for position in positions(value, bit_count, probes) {
            output[10 + (position / 8) as usize] |= 1 << (position % 8);
        }
    }
    Ok(output)
}

/// `None` is the legacy empty filter and conservatively means "may contain".
pub(super) struct Bloom {
    probes: u8,
    bits: Vec<u8>,
    bit_count: u64,
}

impl Bloom {
    pub(super) fn parse(mut payload: &[u8]) -> Result<Option<Self>, Error> {
        if payload.is_empty() {
            return Ok(None);
        }
        let (&version, rest) = payload
            .split_first()
            .ok_or(Error::Corruption("filter header"))?;
        let (&probes, rest) = rest
            .split_first()
            .ok_or(Error::Corruption("filter header"))?;
        payload = rest;
        let bit_count = get_u64(&mut payload).ok_or(Error::Corruption("filter size"))?;
        if version != VERSION
            || !(1..=30).contains(&probes)
            || bit_count < 64
            || bit_count % 8 != 0
            || u64::try_from(payload.len()).ok() != Some(bit_count / 8)
        {
            return Err(Error::Corruption("filter header"));
        }
        Ok(Some(Self {
            probes,
            bits: payload.to_vec(),
            bit_count,
        }))
    }

    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
        positions(hash(key), self.bit_count, self.probes)
            .all(|position| self.bits[(position / 8) as usize] & (1 << (position % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::{build, hash, Bloom};

    #[test]
    fn false_positive_rate_for_one_hundred_thousand_absent_keys() {
        let count = 100_000u64;
        let hashes: Vec<_> = (0..count).map(|n| hash(&(n * 2).to_be_bytes())).collect();
        let encoded = build(&hashes, 10).unwrap();
        let filter = Bloom::parse(&encoded).unwrap().unwrap();
        for n in 0..count {
            assert!(filter.may_contain(&(n * 2).to_be_bytes()));
        }
        let positives = (0..count)
            .filter(|n| filter.may_contain(&(n * 2 + 1).to_be_bytes()))
            .count();
        let measured = positives as f64 / count as f64;
        let theoretical = (1.0 - (-7.0f64 / 10.0).exp()).powi(7);
        assert!(
            measured <= 1.5 * theoretical,
            "rate {measured}, theoretical {theoretical}"
        );
    }

    #[test]
    fn handles_empty_disabled_and_invalid_filters() {
        assert!(Bloom::parse(&[]).unwrap().is_none());
        let encoded = build(&[], 10).unwrap();
        let filter = Bloom::parse(&encoded).unwrap().unwrap();
        assert!(!filter.may_contain(b"anything"));
        assert!(Bloom::parse(&[1, 0, 64, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}

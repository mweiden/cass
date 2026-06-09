/// Simple bloom filter implementation used to provide probabilistic
/// membership tests for on-disk SSTables.
use prost::Message;
pub struct BloomFilter {
    /// Bit vector storing hashed membership information.
    bits: Vec<bool>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BloomProto {
    #[prost(bool, repeated, tag = "1")]
    pub bits: Vec<bool>,
}

/// Bits allocated per expected key. With the two hash functions used below
/// this yields a false-positive rate of roughly 3%.
const BITS_PER_KEY: usize = 10;

/// Lower bound on filter size so tiny tables still get a useful filter.
const MIN_BITS: usize = 1024;

impl BloomFilter {
    /// Create a new filter with `size` bits.
    pub fn new(size: usize) -> Self {
        Self {
            bits: vec![false; size.max(1)],
        }
    }

    /// Create a filter sized for the expected number of distinct keys.
    pub fn with_capacity(expected_keys: usize) -> Self {
        Self::new(expected_keys.saturating_mul(BITS_PER_KEY).max(MIN_BITS))
    }

    /// Compute two simple hash values for `item` using a pair of
    /// different algorithms. These are inexpensive and good enough for
    /// the toy implementation.
    fn hashes(&self, item: &str) -> (usize, usize) {
        // djb2 style hash
        let mut h1: u64 = 5381;
        // second hash accumulates bytes with a different multiplier
        let mut h2: u64 = 0;
        for b in item.bytes() {
            h1 = ((h1 << 5).wrapping_add(h1)).wrapping_add(b as u64);
            h2 = h2.wrapping_mul(31).wrapping_add(b as u64);
        }
        let len = self.bits.len() as u64;
        ((h1 % len) as usize, (h2 % len) as usize)
    }

    /// Record `item` in the filter.
    pub fn insert(&mut self, item: &str) {
        if self.bits.is_empty() {
            return;
        }
        let (a, b) = self.hashes(item);
        self.bits[a] = true;
        self.bits[b] = true;
    }

    /// Return `true` if the filter indicates that `item` may be present.
    /// False positives are possible but false negatives are not.
    pub fn may_contain(&self, item: &str) -> bool {
        // An empty filter (e.g. decoded from a missing/corrupt proto) carries
        // no information, so it must not rule anything out.
        if self.bits.is_empty() {
            return true;
        }
        let (a, b) = self.hashes(item);
        self.bits.get(a).copied().unwrap_or(false) && self.bits.get(b).copied().unwrap_or(false)
    }

    /// Convert the bloom filter into a protobuf message.
    pub fn to_proto(&self) -> BloomProto {
        BloomProto {
            bits: self.bits.clone(),
        }
    }

    /// Construct a bloom filter from a protobuf message.
    pub fn from_proto(proto: BloomProto) -> Self {
        Self { bits: proto.bits }
    }

    /// Serialize the bloom filter using protobuf encoding.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.to_proto().encode(&mut buf).unwrap();
        buf
    }

    /// Reconstruct a bloom filter from protobuf bytes produced by
    /// [`to_bytes`].
    pub fn from_bytes(data: &[u8]) -> Self {
        let proto = BloomProto::decode(data).unwrap();
        Self::from_proto(proto)
    }
}

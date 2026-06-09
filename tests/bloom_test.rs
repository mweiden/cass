use cass::bloom::{BloomFilter, BloomProto};

#[test]
fn bloom_insert_check() {
    let mut b = BloomFilter::new(128);
    b.insert("hello");
    assert!(b.may_contain("hello"));
}

#[test]
fn sized_filter_keeps_false_positive_rate_low() {
    let n = 10_000;
    let mut b = BloomFilter::with_capacity(n);
    for i in 0..n {
        b.insert(&format!("present-{i}"));
    }
    // no false negatives
    for i in 0..n {
        assert!(b.may_contain(&format!("present-{i}")));
    }
    // A fixed-size 1024-bit filter would saturate and report ~100% of
    // absent keys as present; a properly sized filter stays far below that.
    let false_positives = (0..1_000)
        .filter(|i| b.may_contain(&format!("absent-{i}")))
        .count();
    assert!(
        false_positives < 200,
        "false positive rate too high: {false_positives}/1000"
    );
}

#[test]
fn empty_filter_from_proto_does_not_panic() {
    // A corrupt or empty meta file can decode to a filter with no bits. It
    // must not panic and must not rule out any keys (no false negatives).
    let mut b = BloomFilter::from_proto(BloomProto { bits: Vec::new() });
    assert!(b.may_contain("anything"));
    b.insert("anything");
    assert!(b.may_contain("anything"));
}

#[test]
fn zero_size_filter_does_not_panic() {
    let mut b = BloomFilter::new(0);
    b.insert("a");
    assert!(b.may_contain("a"));
}

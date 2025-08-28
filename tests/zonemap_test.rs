use cass::zonemap::ZoneMap;

#[test]
fn zone_map_update_and_roundtrip() {
    let mut zm = ZoneMap::default();

    // Without bounds, contains should allow any key.
    assert!(zm.contains("foo"));

    // Update with out-of-order keys.
    zm.update("m");
    zm.update("a");
    zm.update("z");

    // Verify min and max bounds.
    assert_eq!(zm.min.as_deref(), Some("a"));
    assert_eq!(zm.max.as_deref(), Some("z"));

    // Contains should respect bounds.
    assert!(zm.contains("m"));
    assert!(!zm.contains("0"));
    assert!(!zm.contains("zz"));

    // Protobuf round-trip using to_proto and from_proto.
    let proto = zm.to_proto();
    assert_eq!(proto.min.as_deref(), Some("a"));
    assert_eq!(proto.max.as_deref(), Some("z"));
    let zm_from_proto = ZoneMap::from_proto(proto);
    assert_eq!(zm_from_proto.min.as_deref(), Some("a"));
    assert_eq!(zm_from_proto.max.as_deref(), Some("z"));

    // Byte serialization round-trip.
    let bytes = zm.to_bytes();
    let zm_from_bytes = ZoneMap::from_bytes(&bytes);
    assert_eq!(zm_from_bytes.min.as_deref(), Some("a"));
    assert_eq!(zm_from_bytes.max.as_deref(), Some("z"));
}

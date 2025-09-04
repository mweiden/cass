use cass::query::{cast_simple, object_name_to_ns, split_ts, table_factor_to_ns};
use sqlparser::ast::{DataType, Expr, Ident, ObjectName, ObjectNamePart, TableFactor, Value};

#[test]
fn split_ts_handles_short_buffers() {
    let buf = [1u8, 2u8, 3u8];
    let (ts, rest) = split_ts(&buf);
    assert_eq!(ts, 0);
    assert_eq!(rest, &buf);
}

#[test]
fn split_ts_parses_timestamp_and_rest() {
    let ts_val: u64 = 42;
    let mut buf = ts_val.to_be_bytes().to_vec();
    buf.extend_from_slice(b"hello");
    let (ts, rest) = split_ts(&buf);
    assert_eq!(ts, ts_val);
    assert_eq!(rest, b"hello");
}

#[test]
fn object_name_to_ns_extracts_last_segment_lowercase() {
    let name = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("Foo")),
        ObjectNamePart::Identifier(Ident::new("Bar")),
    ]);
    assert_eq!(object_name_to_ns(&name), Some("bar".to_string()));
}

#[test]
fn object_name_to_ns_returns_none_for_empty() {
    let name = ObjectName(vec![]);
    assert!(object_name_to_ns(&name).is_none());
}

#[test]
fn table_factor_to_ns_handles_table_and_non_table() {
    let table = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("users"))]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    };
    assert_eq!(table_factor_to_ns(&table), Some("users".to_string()));

    let func = TableFactor::TableFunction {
        expr: Expr::Value(Value::Number("1".into(), false).into()),
        alias: None,
    };
    assert!(table_factor_to_ns(&func).is_none());
}

#[test]
fn cast_simple_covers_types_and_errors() {
    assert_eq!(cast_simple("123", &DataType::Int(None)), Some("123".to_string()));
    assert!(cast_simple("abc", &DataType::Int(None)).is_none());
    assert_eq!(cast_simple("hi", &DataType::Text), Some("hi".to_string()));
    assert!(cast_simple("t", &DataType::Boolean).is_none());
}


use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Schema information for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub columns: Vec<String>,
}

impl TableSchema {
    /// Create a new [`TableSchema`].
    pub fn new(
        partition_keys: Vec<String>,
        clustering_keys: Vec<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            partition_keys,
            clustering_keys,
            columns,
        }
    }

    /// Return the ordered list of key columns (partition + clustering).
    pub fn key_columns(&self) -> Vec<String> {
        self.partition_keys
            .iter()
            .chain(self.clustering_keys.iter())
            .cloned()
            .collect()
    }
}

/// Error produced when serializing or deserializing a row.
#[derive(thiserror::Error, Debug)]
#[error("row codec: {0}")]
pub struct RowCodecError(#[from] serde_json::Error);

/// Serialize a row map into bytes.
pub fn encode_row(map: &BTreeMap<String, String>) -> Result<Vec<u8>, RowCodecError> {
    Ok(serde_json::to_vec(map)?)
}

/// Deserialize row bytes into a map.
///
/// Empty input is a valid representation of an absent/deleted row and yields
/// an empty map. Non-empty data that fails to parse is corruption and is
/// surfaced as an error rather than silently treated as an empty row.
pub fn decode_row(data: &[u8]) -> Result<BTreeMap<String, String>, RowCodecError> {
    if data.is_empty() {
        return Ok(BTreeMap::new());
    }
    Ok(serde_json::from_slice(data)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_roundtrip() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), "1".to_string());
        let bytes = encode_row(&map).unwrap();
        assert_eq!(decode_row(&bytes).unwrap(), map);
    }

    #[test]
    fn empty_input_is_empty_row() {
        assert!(decode_row(b"").unwrap().is_empty());
    }

    #[test]
    fn corrupt_input_is_an_error_not_an_empty_row() {
        assert!(decode_row(b"{not json").is_err());
        assert!(decode_row(&[0xff, 0xfe]).is_err());
    }
}

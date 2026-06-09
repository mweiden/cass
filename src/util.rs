use std::{borrow::Cow, fs, io::Write, path::Path};

use crate::rpc::Row as RpcRow;

/// Escape a key for the line-oriented WAL/SSTable format (`key\tvalue\n`).
///
/// Tabs and newlines inside a key would otherwise be indistinguishable from
/// the field and record delimiters, corrupting every record that follows.
/// Backslash is escaped as well so decoding is unambiguous.
pub fn escape_key(key: &str) -> Cow<'_, str> {
    if !key.bytes().any(|b| matches!(b, b'\\' | b'\t' | b'\n')) {
        return Cow::Borrowed(key);
    }
    let mut out = String::with_capacity(key.len() + 8);
    for c in key.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// Reverse [`escape_key`]. Unknown escape sequences are preserved verbatim
/// so keys written before escaping existed still parse unchanged.
pub fn unescape_key(key: &str) -> Cow<'_, str> {
    if !key.contains('\\') {
        return Cow::Borrowed(key);
    }
    let mut out = String::with_capacity(key.len());
    let mut chars = key.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    Cow::Owned(out)
}

/// Print rows in a tabular format to the provided writer.
pub fn print_rows<W: Write>(rows: &[RpcRow], w: &mut W) {
    if rows.is_empty() {
        writeln!(w, "(0 rows)").unwrap();
        return;
    }
    let mut cols: Vec<String> = rows
        .iter()
        .flat_map(|r| r.columns.keys().cloned())
        .collect();
    cols.sort();
    cols.dedup();

    let index_width = rows.len().to_string().len();
    let col_widths: Vec<usize> = cols
        .iter()
        .map(|c| {
            let max_val = rows
                .iter()
                .map(|r| r.columns.get(c).map(|v| v.len()).unwrap_or(0))
                .max()
                .unwrap_or(0);
            std::cmp::max(c.len(), max_val)
        })
        .collect();

    let mut header = format!("{:>width$}", "", width = index_width);
    for (c, w_width) in cols.iter().zip(col_widths.iter()) {
        header.push_str(&format!(" {:<width$}", c, width = w_width));
    }
    writeln!(w, "{}", header).unwrap();

    for (i, row) in rows.iter().enumerate() {
        let mut line = format!("{:>width$}", i, width = index_width);
        for (c, w_width) in cols.iter().zip(col_widths.iter()) {
            let val = row.columns.get(c).cloned().unwrap_or_default();
            line.push_str(&format!(" {:<width$}", val, width = w_width));
        }
        writeln!(w, "{}", line).unwrap();
    }
    writeln!(w, "({} rows)", rows.len()).unwrap();
}

/// Recursively calculate the total size of `.tbl` files under the given directory.
pub fn sstable_disk_usage(dir: &str) -> u64 {
    fn visit(path: &Path) -> u64 {
        let mut size = 0;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    size += visit(&p);
                } else if p.extension().and_then(|e| e.to_str()) == Some("tbl")
                    && let Ok(meta) = entry.metadata() {
                        size += meta.len();
                    }
            }
        }
        size
    }
    visit(Path::new(dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn print_rows_formats_output() {
        let rows = vec![
            RpcRow {
                columns: HashMap::from([("a".into(), "1".into())]),
            },
            RpcRow {
                columns: HashMap::from([("a".into(), "2".into())]),
            },
        ];
        let mut buf: Vec<u8> = Vec::new();
        print_rows(&rows, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let expected = "  a\n0 1\n1 2\n(2 rows)\n";
        assert_eq!(output, expected);
    }

    #[test]
    fn escape_key_roundtrip() {
        for key in [
            "plain",
            "with\ttab",
            "with\nnewline",
            "with\\backslash",
            "all\t\n\\three\\t\\n",
            "",
        ] {
            let escaped = escape_key(key);
            assert!(!escaped.contains('\t'));
            assert!(!escaped.contains('\n'));
            assert_eq!(unescape_key(&escaped), key);
        }
    }

    #[test]
    fn escape_key_borrows_for_plain_keys() {
        assert!(matches!(escape_key("plain"), Cow::Borrowed(_)));
        assert!(matches!(unescape_key("plain"), Cow::Borrowed(_)));
    }

    #[test]
    fn unescape_preserves_unknown_sequences() {
        // keys written before escaping existed may contain lone backslashes
        assert_eq!(unescape_key("a\\xb"), "a\\xb");
        assert_eq!(unescape_key("trailing\\"), "trailing\\");
    }

    #[test]
    fn sstable_disk_usage_counts_tbl_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.tbl"), [0u8; 10]).unwrap();
        fs::write(dir.path().join("b.txt"), [0u8; 5]).unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("c.tbl"), [0u8; 7]).unwrap();
        let usage = sstable_disk_usage(dir.path().to_str().unwrap());
        assert_eq!(usage, 17);
    }
}

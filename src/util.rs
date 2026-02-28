use std::{fs, io::Write, path::Path};

use crate::rpc::Row as RpcRow;

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
                } else if p.extension().and_then(|e| e.to_str()) == Some("tbl") {
                    if let Ok(meta) = entry.metadata() {
                        size += meta.len();
                    }
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

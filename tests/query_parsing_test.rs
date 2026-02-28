use cass::SqlEngine;

#[test]
fn base_sql_ignores_if_inside_quotes() {
    let eng = SqlEngine::new();
    let sql = "INSERT INTO msgs (id, val) VALUES ('1', 'as if by magic')";
    let base = eng.base_sql(sql);
    assert_eq!(base, sql);
    assert!(eng.find_trailing_if_index(sql).is_none());
}

#[test]
fn base_sql_ignores_if_inside_double_quotes_identifier() {
    let eng = SqlEngine::new();
    // Double quotes typically denote identifiers; ensure we ignore IF there too
    let sql = "UPDATE \"as if needed\" SET v=1 WHERE id='a'";
    let base = eng.base_sql(sql);
    assert_eq!(base, sql);
    assert!(eng.find_trailing_if_index(sql).is_none());
}

#[test]
fn base_sql_ignores_if_inside_line_comment() {
    let eng = SqlEngine::new();
    let sql = "UPDATE kv SET val='2' WHERE id='a' -- IF val='1'\n";
    let base = eng.base_sql(sql);
    assert_eq!(base, sql.trim());
    assert!(eng.find_trailing_if_index(sql).is_none());
}

#[test]
fn base_sql_ignores_if_in_words() {
    let eng = SqlEngine::new();
    let sql = "UPDATE kv SET note='gift' WHERE id='a'";
    let base = eng.base_sql(sql);
    assert_eq!(base, sql);
    assert!(eng.find_trailing_if_index(sql).is_none());
}

#[test]
fn base_sql_strips_trailing_if_clause() {
    let eng = SqlEngine::new();
    let sql = "UPDATE kv SET val='2' WHERE id='a' IF val='1'";
    let base = eng.base_sql(sql);
    assert_eq!(base, "UPDATE kv SET val='2' WHERE id='a'");
    assert!(eng.find_trailing_if_index(sql).is_some());
}

#[test]
fn base_sql_strips_trailing_if_clause_uppercase() {
    let eng = SqlEngine::new();
    let sql = "UPDATE kv SET val='2' WHERE id='a' IF VAL='1'";
    let base = eng.base_sql(sql);
    assert_eq!(base, "UPDATE kv SET val='2' WHERE id='a'");
    assert!(eng.find_trailing_if_index(sql).is_some());
}

#[test]
fn base_sql_handles_escaped_quotes() {
    let eng = SqlEngine::new();
    let sql = "INSERT INTO msgs (id, val) VALUES ('1', 'don''t if anything')";
    let base = eng.base_sql(sql);
    assert_eq!(base, sql);
    assert!(eng.find_trailing_if_index(sql).is_none());
}

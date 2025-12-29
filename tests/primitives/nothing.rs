use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn nothing_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (id UInt8, value Nothing) ENGINE=Memory"
    )) {
        return;
    }
    if !server.try_exec(&format!("INSERT INTO {table} (id) VALUES (1)")) {
        return;
    }
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("value", "Nothing")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT id, value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::UInt8(1), Value::Nothing]]);
    }
}

#[test]
fn nothing_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (id UInt8, value Nothing) ENGINE=Memory"
    )) {
        return;
    }
    if !server.try_exec(&format!("INSERT INTO {table} (id) VALUES (1),(2)")) {
        return;
    }
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("value", "Nothing")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT id, value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::UInt8(1), Value::Nothing],
                vec![Value::UInt8(2), Value::Nothing],
            ]
        );
    }
}

#[test]
fn nothing_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (id UInt8, value Nothing) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("value", "Nothing")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::UInt8(1), Value::Nothing]],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT id, value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::UInt8(1), Value::Nothing]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn nothing_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (id UInt8, value Nothing) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("value", "Nothing")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::UInt8(1), Value::Nothing],
                vec![Value::UInt8(2), Value::Nothing],
            ],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT id, value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::UInt8(1), Value::Nothing],
                vec![Value::UInt8(2), Value::Nothing],
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

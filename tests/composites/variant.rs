use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn variant_schema() -> Schema {
    // ClickHouse sorts Variant types by name; String comes before UInt8.
    Schema::from_type_strings(&[("value", "Variant(String, UInt8)")]).unwrap()
}

fn variant_rows() -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let first = vec![Value::Variant {
        index: 0,
        value: Box::new(Value::String(b"alpha".to_vec())),
    }];
    let second = vec![Value::Variant {
        index: 1,
        value: Box::new(Value::UInt8(7)),
    }];
    let third = vec![Value::VariantNull];
    (first, second, third)
}

fn create_variant_table(server: &ClickhouseServer, table: &str) -> bool {
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.try_exec(&format!(
        "CREATE TABLE {table} (value Variant(String, UInt8)) ENGINE=Memory"
    ))
}

#[test]
fn variant_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    if !create_variant_table(&server, &table) {
        return;
    }
    let schema = variant_schema();
    let (first, _, _) = variant_rows();
    let insert_sql = format!(
        "INSERT INTO {table} FORMAT {}",
        RowBinaryFormat::RowBinaryWithNamesAndTypes
    );
    server.insert_rowbinary(
        &insert_sql,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
        std::slice::from_ref(&first),
    );

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone()]);
    }
}

#[test]
fn variant_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    if !create_variant_table(&server, &table) {
        return;
    }
    let schema = variant_schema();
    let (first, second, third) = variant_rows();
    let insert_sql = format!(
        "INSERT INTO {table} FORMAT {}",
        RowBinaryFormat::RowBinaryWithNamesAndTypes
    );
    server.insert_rowbinary(
        &insert_sql,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
        &[first.clone(), second.clone(), third.clone()],
    );

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone(), second.clone(), third.clone()]);
    }
}

#[test]
fn variant_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    if !create_variant_table(&server, &table) {
        return;
    }
    let schema = variant_schema();
    let (first, _, _) = variant_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&first));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone()]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn variant_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    if !create_variant_table(&server, &table) {
        return;
    }
    let schema = variant_schema();
    let (first, second, third) = variant_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[first.clone(), second.clone(), third.clone()],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone(), second.clone(), third.clone()]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

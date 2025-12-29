use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn bfloat16_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value BFloat16) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (1.5)"));
    let schema = Schema::from_type_strings(&[("value", "BFloat16")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::BFloat16(1.5_f32)]]);
    }
}

#[test]
fn bfloat16_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value BFloat16) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (1.5),(-2.25)"));
    let schema = Schema::from_type_strings(&[("value", "BFloat16")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::BFloat16(1.5_f32)],
                vec![Value::BFloat16(-2.25_f32)],
            ]
        );
    }
}

#[test]
fn bfloat16_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value BFloat16) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "BFloat16")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::BFloat16(1.5_f32)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 1.5})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value BFloat16) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "BFloat16")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::BFloat16(1.5_f32)],
                vec![Value::BFloat16(-2.25_f32)],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": 1.5}), json!({"value": -2.25})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Nullable(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn bfloat16_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Nullable(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),(1.5)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::BFloat16(1.5_f32))))],
            ]
        );
    }
}

#[test]
fn bfloat16_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Nullable(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Nullable(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Nullable(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Nullable(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::BFloat16(1.5_f32))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": 1.5})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(BFloat16)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (1.5)"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::BFloat16(1.5_f32)]]);
    }
}

#[test]
fn bfloat16_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(BFloat16)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (1.5),(-2.25),(1.5)"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::BFloat16(1.5_f32)],
                vec![Value::BFloat16(-2.25_f32)],
                vec![Value::BFloat16(1.5_f32)],
            ]
        );
    }
}

#[test]
fn bfloat16_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(BFloat16)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::BFloat16(1.5_f32)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 1.5})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(BFloat16)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::BFloat16(1.5_f32)],
                vec![Value::BFloat16(-2.25_f32)],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": 1.5}), json!({"value": -2.25})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([1.5, -2.25])"));
    let schema = Schema::from_type_strings(&[("value", "Array(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::BFloat16(1.5_f32),
                Value::BFloat16(-2.25_f32),
            ])]]
        );
    }
}

#[test]
fn bfloat16_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([1.5]),([])"));
    let schema = Schema::from_type_strings(&[("value", "Array(BFloat16)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::BFloat16(1.5_f32)])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn bfloat16_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::BFloat16(1.5_f32),
                Value::BFloat16(-2.25_f32),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [1.5, -2.25]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(BFloat16)) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(BFloat16)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::BFloat16(1.5_f32)])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [1.5]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(BFloat16))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL, 1.5])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(BFloat16))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::BFloat16(1.5_f32)))),
            ])]]
        );
    }
}

#[test]
fn bfloat16_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(BFloat16))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL]),([])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(BFloat16))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn bfloat16_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(BFloat16))) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(BFloat16))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::BFloat16(1.5_f32)))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, 1.5]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(BFloat16))) ENGINE=Memory"
    )) {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(BFloat16))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(BFloat16))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([1.5, -2.25])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(BFloat16))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::BFloat16(1.5_f32),
                Value::BFloat16(-2.25_f32),
            ])]]
        );
    }
}

#[test]
fn bfloat16_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(BFloat16))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([1.5]),([])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(BFloat16))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::BFloat16(1.5_f32)])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn bfloat16_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(BFloat16))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(BFloat16))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::BFloat16(1.5_f32),
                Value::BFloat16(-2.25_f32),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [1.5, -2.25]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn bfloat16_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(BFloat16))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(BFloat16))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::BFloat16(1.5_f32)])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [1.5]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

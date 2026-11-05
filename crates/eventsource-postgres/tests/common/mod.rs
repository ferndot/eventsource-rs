//! Shared test support: every store gets its own randomly named schema.
#![allow(dead_code)] // Each test binary uses a different subset.

/// The database the tests run against.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@127.0.0.1:5432/eventsource_test".to_owned()
    })
}

/// A fresh, unique schema name. The schema itself is created by the adapter on open.
pub fn fresh_schema() -> String {
    format!("t_{}", uuid::Uuid::new_v4().simple())
}

/// Opens a raw connection (search path left alone) for tampering and cleanup.
pub async fn raw_client() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls)
        .await
        .expect("raw connect");
    tokio::spawn(connection);
    client
}

/// Runs `sql` inside `schema`, bypassing the stores.
pub async fn tamper(schema: &str, sql: &str) {
    let client = raw_client().await;
    client
        .batch_execute(&format!("SET search_path TO \"{schema}\"; {sql}"))
        .await
        .expect("tamper");
}

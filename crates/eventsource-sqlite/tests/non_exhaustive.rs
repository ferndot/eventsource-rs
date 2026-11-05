//! External-crate check that the public error enums are `#[non_exhaustive]`.
//!
//! Each function lists every current variant and then a wildcard. From outside
//! the defining crate that wildcard is mandatory when the enum is
//! `#[non_exhaustive]`; without the attribute it is unreachable, which
//! `deny(unreachable_patterns)` turns into a compile error. Adding a variant to
//! one of these enums is therefore not a breaking change for downstream matches.
#![allow(missing_docs, dead_code)]
#![deny(unreachable_patterns)]

use eventsource_sqlite::SqliteError;

fn sqlite_error(error: &SqliteError) -> &'static str {
    match error {
        SqliteError::Sqlite { .. } => "Sqlite",
        SqliteError::Corrupt { .. } => "Corrupt",
        _ => "future variant",
    }
}

#[test]
fn wildcard_arms_compile() {
    // The functions above are the test; this keeps them referenced.
    let _ = (sqlite_error,);
}

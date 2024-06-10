//! <!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
//!      in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
//!      `cargo readme > README.md` -->
//!
//! # valve.rs
//! A lightweight validation engine written in rust.
//!
//! ## API
//! See [valve]
//!
//! ## Command line usage
//! Run:
//! ```
//! valve --help
//! ```
//! to see command line options.
//!
//! ## Logging
//! By default Valve only logs error messages. To also enable warning and information messages,
//! set the environment variable `RUST_LOG` to the minimum logging level desired for ontodev_valve:
//! `debug`, `info`, `warn`, or `error`.
//! For instance:
//! ```
//! export RUST_LOG="ontodev_valve=info"
//! ```
//! For further information see the [Rust Cookbook](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html).
//!
//! ## Python bindings
//! See [valve.py](https://github.com/ontodev/valve.py)

#[macro_use]
extern crate lalrpop_util;

pub mod ast;
pub mod internal;
pub mod toolkit;
pub mod validate;
pub mod valve;

lalrpop_mod!(
    /// Valve expression grammar
    pub valve_grammar
);

use lazy_static::lazy_static;

/// The number of rows that are validated at a time by a thread.
pub static CHUNK_SIZE: usize = 500;

/// Run valve in multi-threaded mode.
pub static MULTI_THREADED: bool = true;

/// Maximum number of database connections.
pub static MAX_DB_CONNECTIONS: u32 = 5;

/// The maximum interval size to use for the move_row() operation. When MOVE_INTERVAL is set to N,
/// then it will be possible to move N-1 rows in between two given rows.
pub static MOVE_INTERVAL: u32 = 1000;

/// Used to match a printf-style format specifier (see <https://docs.rs/sprintf/latest/sprintf/#>)
pub static PRINTF_RE: &str = r#"^%.*([\w%])$"#;


// Note that SQL_PARAM must be a 'word' (from the point of view of regular expressions) since in the
// local_sql_syntax() function below we are matchng against it using '\b' which represents a word
// boundary. If you want to use a non-word placeholder then you must also change '\b' in the regex
// to '\B'.
/// The word (in the regex sense) placeholder to use for query parameters when binding using sqlx.
pub static SQL_PARAM: &str = "VALVEPARAM";

lazy_static! {
    pub static ref SQL_TYPES: Vec<&'static str> = vec!["text", "varchar", "numeric", "integer", "real"];
}

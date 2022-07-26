<!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
     in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
     `cargo readme > README.md` -->

## valve.rs
A lightweight validation engine written in rust.

This implementation is a port of the
[next implementation of the valve parser](https://github.com/jamesaoverton/cmi-pb-terminology/tree/next) to rust.

### Crates.io page

https://crates.io/crates/ontodev_valve

### Command line usage
```rust
valve table db
```
where `table` is the path to the table table (normally table.tsv) and `db` is the path to the
sqlite database file (which is created if it doesn't exist).

### Python bindings
See [valve.py](https://github.com/ontodev/valve.py/tree/valve_rs_python_bindings)

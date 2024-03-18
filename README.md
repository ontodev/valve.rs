# ontodev_valve

<!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
     in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
     `cargo readme > README.md` -->

## valve.rs
A lightweight validation engine written in rust.

### API
See [valve]

### Command line usage
Run:
```rust
valve --help
```
to see command line options.

### Logging
By default Valve only logs error messages. To also enable warning and information messages,
set the environment variable `RUST_LOG` to the minimum logging level desired for ontodev_valve:
`debug`, `info`, `warn`, or `error`.
For instance:
```rust
export RUST_LOG="ontodev_valve=info"
```
For further information see the [Rust Cookbook](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html).

### Python bindings
See [valve.py](https://github.com/ontodev/valve.py)

License: BSD-3-Clause

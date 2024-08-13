# ontodev_valve/valve.rs

A lightweight validation engine written in rust.

## Table of contents

- [Design and concepts](#design-and-concepts)
- [Installation and configuration](#installation-and-configuration)
- [Command line usage](#command-line-usage)
- [Logging](#logging)
- [Application Programmer Interface (API)](#api)
- [Python bindings](#python-bindings)

## Design and concepts

To be written.

## Installation and configuration

### Installation

To be written

### Configuration tables

#### The 'table' table

Valve's configuration is specified in a number of user-editable tables represented as `.tsv` files. The most important of these is the table called 'table' (normally stored in a file called `table.tsv`, although any filename could be used). The 'table' table contains the following columns: `table`, `path`, `description`, `type`, `options`. Below is an example 'table' table:

| Tables        | Are           | Cool  |
| ------------- |:-------------:| -----:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |

table                | path                                 | description | type     | options
-----------------------------------------------------------------------------------------------
table                | schema/table.tsv                     |             | table    |
column               | schema/column.tsv                    |             | column   |
datatype             | schema/datatype.tsv                  |             | datatype |
rule                 | schema/rule.tsv                      |             | rule     |
user_table1          | schema/user/user_table1.tsv          |             |          |
user_readonly_table2 | schema/user/user_readonly_table1.tsv |             |          | no-edit no-save no-conflict
user_view1           | schema/user/user_view1.sql           |             |          | db_view
user_view2           | schema/user/user_view2.sh            |             |          | db_view
user_view2           | schema/user/user_view2.sh            |             |          | db_view
user_view3           |                                      |             |          | db_view

The column, `table`, indicates the name of the table, and the column, `path`, indicates where in the filesystem that the
table is located. The column, `description`, is optional and is used to describe the table's contents.

Valve recognizes four special table types that can be specified using the `type` column. These are the `table`, `column`, `datatype`, and `rule` table types. Valve requires that tables corresponding to each of the first three types be configured. However specifying a table of type `rule` is optional.

TO BE CONTINUED ...


## Command line usage

The basic syntax when calling Valve on the command line is:

```rust
valve [OPTIONS] <SUBCOMMAND <SUBCOMMAND POSITIONAL PARAMETERS>>
```

To view the list of possible subcommands and global options, run:
```rust
valve --help
```
To get help on a particular subcommand, and on the positional parameters and options that are specific to it, run:
```rust
valve SUBCOMMAND --help
```

## Logging

By default Valve only logs error messages. To also enable warning and information messages,
set the environment variable `RUST_LOG` to the minimum logging level desired for ontodev_valve:
`debug`, `info`, `warn`, or `error`.
For instance:
```rust
export RUST_LOG="ontodev_valve=info"
```
For further information see the [Rust Cookbook](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html).

## API

The API (application-programmer interface) reference documentation can normally be found on [crates.io](https://crates.io/crates/ontodev_valve), however this documentation is currently out of date. To generate the latest API documentation, run

```rust
cargo doc
```

in the the root folder of your local copy of the valve.rs source code repository (see [Installation and configuration](#installation-and-configuration)), and then open the file

    <valve.rs root folder>/target/doc/ontodev_valve/index.html

in your favourite browser. For a more general discussion of how

For a higher level See also the [Design and concepts](#design-and-concepts) section below.

## Python bindings
See [valve.py](https://github.com/ontodev/valve.py)

License: BSD-3-Clause

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

#### The table table

Valve's configuration is specified in a number of user-editable tables represented as `.tsv` files. The most important of these is the table called 'table' (normally stored in a file called `table.tsv`, although any filename could be used). The table table (see below for an example) contains the following columns: **table**, **path**, **description**, **type**, **options**.

table                | path                                    | description | type     | options
-------------------- | ----------------------------------------| ----------- | -------- | ------------
table                | schema/table.tsv                        |             | table    |
column               | schema/column.tsv                       |             | column   |
datatype             | schema/datatype.tsv                     |             | datatype |
rule                 | schema/rule.tsv                         |             | rule     |
user_table1          | schema/user/user_table1.tsv             |             |          |
user_readonly_table2 | schema/user/user_readonly_table1.tsv    |             |          | no-edit no-save no-conflict
user_view1           | schema/user/user_view1.sql              |             |          | db_view
user_view2           | schema/user/user_view2.sh               |             |          | db_view
user_view2           | schema/user/user_view2.sh               |             |          | db_view
user_view3           |                                         |             |          | db_view

These columns have the following significance:
- **table**: the name of the table.
- **path**: where to find information about the contents of the table. This can be a .tsv file, a .sql file, some other executable file, or it can be empty. The kind of path that needs to be specified is determined by the contents of the **options** column (see below) for the table.
- **description**: An optional description of the contents and/or purpose of the table.
- **type**: Valve recognizes four special table types that can be specified using the **type** column. These are the `table`, `column`, `datatype`, and `rule` table types. Valve requires that tables corresponding to each of the first three types be configured. However specifying a table of type `rule` is optional. For more on the first three types, see the sections entitled [The column table](#the-column-table), [The datatype table](#the-column-table), and [The rule table](#the-column-table) below. Note that user-defined tables should not specify a type. Note also that if an unrecognised type is specified Valve will ignore it.
- **options**: allows the user to specify a number of further options for the table (see below).

If no options are specified, the options *db_table*, *truncate*, *load*, *save*, *edit*, *validate_on_load*, and *conflict* will all be set by default. The complete list of allowable options, and their meanings, are:
  - *db_table*: The table is represented in the database by a regular table. Note that this option is not compatible with *db_view*.
  - *db_view*: The table is represented in the database by a view. Note that this option is not compatible with any of *db_table*, *truncate*, *load*, *conflict*, *save*, *edit*, or *validate_on_load*.
  - *load*: The table may be loaded with data. Note that this option is not compatible with *db_view*.
  - *truncate*: The table should be truncated before it is loaded. Note that this option is not compatible with *db_view*.
  - *conflict*: Valve should create a conflict table (see [Design and concepts](#design-and-concepts)). Note that this option is not compatible with *db_view*.
  - *internal*: This option is reserved for internal use and may not be specified by the user.
  - *validate_on_load*: Validate a table's rows before loading them to the database. Note that this option is not compatible with *db_view*.
  - *edit*: It is allowed to edit the table after it has been initially loaded. Note that this option is not compatible with *db_view*.
  - *save*: The table is allowed to be saved to a .tsv file. Note that this option is not compatible with *db_view*.
  - *no-conflict*: Sets the *conflict* option (which is set to true by default when *db_table* is also specified) to false.
  - *no-validate_on_load*: Sets the *validate_on_load* option (which is set to true by default when *db_table* is also specified) to false.
  - *no-edit*: Sets the *edit* option (which is set to true by default when *db_table* is also specified) to false.
  - *no-save*: Sets the *save* option (which is set to true by default when *db_table* is also specified) to false.

TO BE CONTINUED ...

### The column table

TODO

### The datatype table

TODO

### The rule table

TODO

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

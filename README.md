# ontodev_valve/valve.rs

Valve - A lightweight validation engine written in rust.

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

### Configuration

#### The table table

Valve is configured using a number of special, required, configuration tables that can be represented as '.tsv' files. The most important of these is the table called 'table', also known as the table table. One normally configures the table table using a '.tsv' file called 'table.tsv', although any filename could be used in principle, and in fact it is also possible to read the table table directly from the database, as long as the database already contains a table called 'table' with the right setup. Below is an example of a table table:

table                | path                                 | description | type     | options
-------------------- | ------------------------------------ | ----------- | -------- | ------------
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

Note that in the first row above the table being described is the table table itself. The table called 'column', or the column table, is described in the second row, and so on. In general the columns of the table table have the following significance:
- **table**: the name of the table.
- **path**: where to find information about the contents of the table. This can be a '.tsv' file, a '.sql' file, some other executable file, or it can be empty. The kind of path (if any) that needs to be specified is determined by the contents of the **options** column (see below) for the given table.
- **description**: An optional description of the contents and/or the purpose of the table.
- **type**: Valve recognizes four special configuration table types that can be specified using the **type** column of the table table. These are the `table`, `column`, `datatype`, and `rule` table types. Valve requires that a single table corresponding to each of the first three types be configured. However specifying a table of type `rule` (i.e., specifying a rule table) is optional. This section is about the table table. For the other special table types, see the sections on [the column table](#the-column-table), [the datatype table](#the-datatype-table), and the [the rule table](#the-rule-table) below. Data tables (e.g., the 'user_*' tables in the above figure) should not explicitly specify a type, and if one is specified it will be ignored unless it is one of the recognised types just mentioned.
- **options**: allows the user to specify a number of further options for the table (see below).

##### Further information on options

If no options are specified, the options *db_table*, *truncate*, *load*, *save*, *edit*, *validate_on_load*, and *conflict* will all be set by default. The complete list of allowable options, and their meanings, are given below:
  - *db_table*: The table is represented in the database by a regular table. It is allowed to have any empty value for **path** unless the *edit* option has been enabled. If **path** is empty, then Valve expects that the table has already been created and loaded by the user. If **path** is non-empty then it can be (a) a file ending (case insensitively) with '.tsv' explicitly specifying the contents of the table, (b) a file ending (case insensitively) with '.sql', or (c) an executable file. In the case of (a), Valve will take care of validating and loading the data from the '.tsv' file. In cases (b) and (c), Valve relies on the '.sql' file or the executable to load the data, and performs no validation other than to verify that it has been loaded successfully (this verification is also performed in the case of an empty **path**). Note that the *db_table* option is not compatible with *db_view*.
  - *db_view*: The table is represented in the database by a view. A view is allowed to have an empty **path**, in which case valve simply verifies that the view exists but does not create it. When **path** is non-empty then it must either end (case-insensitively) in '.sql' or be an executable file. It must not end (case-insensitively) in '.tsv'. When **path** is not-empty, valve relies on the '.sql' file or the executable to create the view, otherwise it expects the view to have already been created by the user. Regardless of the value of **path**, Valve performs no validation of the data other than to verify that the view exists. Note that the *db_view* option is not compatible with any of *db_table*, *truncate*, *load*, *conflict*, *save*, *edit*, or *validate_on_load*.
  - *load*: The table may be loaded with data. Note that this option is not compatible with *db_view*.
  - *truncate*: The table should be truncated before it is loaded. Note that this option is not compatible with *db_view*.
  - *conflict*: When reading the configuration for a table called 'T', then if 'T' has the *conflict* option set, Valve should create, in addition to 'T', a database table called 'T_conflict'. The purpose of this table is to store invalid data that, due to the violation of a database constraint, cannot be stored in 'T' (see [Design and concepts](#design-and-concepts)). Note that the *conflict* option is not compatible with *db_view*.
  - *internal*: This option is reserved for internal use and may not be specified by the user.
  - *validate_on_load*: Validate a table's rows before loading them to the database. Note that this option is not compatible with *db_view*.
  - *edit*: When set, this indicates that it is allowed to edit the table after it has been initially loaded. Note that this option is not compatible with *db_view*.
  - *save*: When set, this indicates that it is allowed to save the table to a '.tsv' file. Note that this option is not compatible with *db_view*.
  - *no-conflict*: Sets the *conflict* option (which is set to true by default unless *db_view* is true) to false.
  - *no-validate_on_load*: Sets the *validate_on_load* option (which is set to true by default unless *db_view* is true) to false.
  - *no-edit*: Sets the *edit* option (which is set to true by default unless *db_view* is true) to false.
  - *no-save*: Sets the *save* option (which is set to true by default unless *db_view* is true) to false.

#### The column table

In addition to a table table, Valve also requires a column table. The column table configuration is normally stored in a file called 'column.tsv' though in principle any filename may be used. The column table contains one row for every column of every configured table. This includes the special configuration tables (such as the column table itself) as well as user-defined tables. Below is an example column table, with the special configuration tables omitted:

table  | column  | label    | nulltype | default | datatype     | structure            | description
---    | ---     | ---      | ---      | ---     | ---          | ---                  | ---
table1 | column1 | Column 1 |          | value1  |              |                      |
table1 | column2 | Column 2 | empty    |         | integer      | from(table2.column2) |
table2 | column1 | Column 1 |          |         | trimmed_line | primary              |
table2 | column2 | Column 2 |          |         | integer      | unique               |
table3 | column1 | Column 1 |          |         | word         | primary              |
table3 | column2 | Column 2 | empty    |         | word         | tree(column1)        |


The columns of the column table have the following significance:

**TODO.**

#### The datatype table

In addition to the table table and the column table, Valve also requires a datatype table. The datatype table configuration is normally stored in a file called 'datatype.tsv' although in principle any filename could be used. The datatype table stores the definitions of the datatypes referred to in the **datatype** column of the column table. Below is a subset of the rows of an example datatype table:

datatype  | parent   | condition             | description                                       | sql_type | HTML type | format
---       | ---      | ---                   | ---                                               | ---      | ---       | ---
text      |          |                       | any text                                          | TEXT     | textarea  |
integer   | text     | match(/-?\d+/)        | a positive or negative decimal digit, or 0        | INTEGER  |           | %i
word      | text     | exclude(/\W/)         | a single word: letters, numbers, underscore       |          |           | %s
empty     | text     | equals('')            | the empty string                                  | NULL     |           |
custom1   | text     | match(/\S+:\S+/)      | two nonspace character sequences separated by ':' |          |           |
custom2   | text     | in(alice, bob, cindy) | either 'alice', 'bob', or 'cindy'                 |          |           |
custom3   | text     | list(word, ' ')       | a list of words separated by spaces               |          |           |
custom4   | text     | search(/\d+/)         | a string containing a sequence of digits          |          |           |

The columns of the datatype table have the following significance:

**TODO.**

#### The rule table

In addition to the table table, the column table, and the datatype table, it is also possible (but optional) to configure a table of type 'rule', or a rule table. When it is configured, the rule table configuration is normally stored in a file called 'rule.tsv' though in principle any filename may be used. The rule table defines a number of rules of the following form:

&nbsp; &nbsp; *when `CONDITION_ON_COLUMN_1` is satisfied then `CONDITION_ON_COLUMN_2` must also be satisfied*

where `CONDITION_ON_COLUMN_1` and `CONDITION_ON_COLUMN_2` are defined with respect to the same table.

Below is an example rule table:

table  | when column | when condition | then column | then condition | level | description
---    | ---         | ---            | ---         | ---            | ---   | ---
table1 | foo         | null           | bar         | not null       | error |
table2 | foo         | negative_int   | bar         | positive_int   | error |

The columns of the rule table have the following significance:

**TODO.**

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

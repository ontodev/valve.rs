# ontodev_valve/valve.rs

Valve - A lightweight validation engine written in rust.

## Table of contents

- [Design and concepts](#design-and-concepts)
- [Installation and configuration](#installation-and-configuration)
- [Command line usage](#command-line-usage)
- [Logging](#logging)
- [Application Programmer Interface (API)](#api)
- [Troubleshooting](#troubleshooting)
- [Python bindings](#python-bindings)

## Design and concepts

TODO.

## Installation and configuration

### Installation

#### Binary installation

_Option 1_: Build and install the binary for the latest release using [crates.io](https://crates.io/crates/ontodev_valve) by running:

    cargo install ontodev_valve

_Option 2_: Download the appropriate binary for your system from the [release page](https://github.com/ontodev/valve.rs/releases) and copy it to your system's executable path.

#### Source installation

It is also possible to build the binary yourself from the source code. This is required if you would like to work with a version of Valve that is newer than the latest release. Begin by using `git clone` to clone the source code repository into a local folder:

    git clone git@github.com:ontodev/valve.rs.git

Next, use `cargo` to build the `ontodev_valve` binary (this may take awhile to complete and will produce a lot of output):

    cargo build --release

After the build has completed successfully, the `ontodev_valve` binary may be found (relative to the repository's root directory) in the `target/release/` subdirectory. You should copy or move it from here to your user or system-wide `bin/` directory, or to some other convenient location. For example,

    cp target/release/ontodev_valve ~/bin/

Finally, run

    ontodev_valve --help

to verify that Valve was installed correctly.

### Configuration

Valve is configured primarily using a number of special configuration tables that can be represented as '.tsv' files. The most important of these is the table called 'table', also known as the table table. A [table table](#the-table-table) configuration is required to use Valve. A [column table](#the-column-table) and [datatype table](#the-datatype-table) configuration are required as well. Optionally, the user may also specify a rule table configuration.

The table table is alone among the configuration tables in that it cannot be given an arbitrary name but must always be given the name 'table'. This is not the case for the column, datatype and rules tables. Although it is recommended to use the names 'column, 'datatype', and 'rule', respectively, alternate names may be chosen for these tables as explained below.

#### The table table

One normally configures the table table using a '.tsv' file called 'table.tsv', although any filename could be used in principle, and in fact it is also possible to read the table table directly from the database, as long as the database already contains a table called 'table' with the right setup. 

Below is an example of a table table:

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

Note that in the first row above the table being described is the table table itself. In general the columns of the table table have the following significance:
- **table**: the name of the table.
- **path**: where to find information about the contents of the table. This can be a '.tsv' file, a '.sql' file, some other executable file, or it can be empty. The path for one of the special configuration tables must be a '.tsv' file. Otherwise the kind of path (if any) that needs to be specified is determined by the contents of the **options** column (see below) for the given table.
- **description**: An optional description of the contents and/or the purpose of the table.
- **type**: Valve recognizes four special configuration table types that can be specified using the **type** column of the table table. These are the `table`, `column`, `datatype`, and `rule` table types. Data tables (e.g., the 'user_*' tables in the above example) should not explicitly specify a type, and in general if a type other than the ones just mentioned is specified it will be ignored.
- **options** (optional column): Allows the user to specify a number of further options for the table (see below).

##### Further information on options

If no options are specified, the options *db_table*, *truncate*, *load*, *save*, *edit*, *validate_on_load*, and *conflict* will all be set to true by default. The complete list of allowable options, and their meanings, are given below:
  - *db_table*: The table will be represented in the database by a regular table. Note that if the *edit* option has also been set, then **path** must be non-empty and it can be one of (a) a file ending (case insensitively) with '.tsv' explicitly specifying the contents of the table, (b) a file ending (case insensitively) with '.sql', or (c) an executable file. In the case of (a), Valve will take care of validating and loading the data from the '.tsv' file. In cases (b) and (c), Valve uses the '.sql' file or the executable to load the data, and performs no validation other than to verify that this has been done without error. If **path** is empty, then Valve expects that the table has already been created and loaded by the user and will fail otherwise. Note that the *db_table* option is not compatible with the *db_view* option.
  - *db_view*: The table will be represented in the database by a view. A view is allowed to have an empty **path**, in which case valve simply verifies that the view exists but does not create it. When **path** is non-empty then it must either end (case-insensitively) in '.sql' or be an executable file. Note that the **path** for a view must never end (case-insensitively) in '.tsv'. When **path** is not-empty, valve relies on the '.sql' file or the executable to create the view, otherwise it expects the view to have already been created by the user. Regardless of the value of **path**, Valve performs no validation of the data other than to verify that the view exists in the database. Note that the *db_view* option is not compatible with any of *db_table*, *truncate*, *load*, *conflict*, *save*, *edit*, or *validate_on_load*.
  - *load*: The table may be loaded with data. Note that this option is not compatible with *db_view*.
  - *truncate*: The table should be truncated before it is loaded. Note that this option is not compatible with *db_view*.
  - *conflict*: When reading the configuration for a table called 'T', then if 'T' has the *conflict* option set, Valve should create, in addition to 'T', a database table called 'T_conflict'. The purpose of this table is to store invalid data that, due to the violation of a database constraint, cannot be stored in 'T' (see [Design and concepts](#design-and-concepts)). Note that the *conflict* option is not compatible with *db_view*.
  - *internal*: This option is reserved for internal use and is not allowed to be specified by the user.
  - *validate_on_load*: When set, Valve will validate a table's rows before loading them to the database. Note that this option is not compatible with *db_view*.
  - *edit*: When set, this option indicates that it is allowed to edit the table after it has been initially loaded. Note that this option is not compatible with *db_view*.
  - *save*: When set, this indicates that it is allowed to save the table to a '.tsv' file. Note that this option is not compatible with *db_view*.
  - *no-conflict*: Sets the *conflict* option (which is set to true by default unless *db_view* is true) to false.
  - *no-validate_on_load*: Sets the *validate_on_load* option (which is set to true by default unless *db_view* is true) to false.
  - *no-edit*: Sets the *edit* option (which is set to true by default unless *db_view* is true) to false.
  - *no-save*: Sets the *save* option (which is set to true by default unless *db_view* is true) to false.

#### The column table

In addition to a table table, Valve also requires a column table. The column table configuration is normally stored in a file called 'column.tsv', though in principle any filename may be used as long as the **type** field corresponding to the filename is set to 'column' in [the table table](#the-table-table). The column table contains one row for every column of every configured table. This includes both special configuration tables (such as the column table itself) and user-defined tables.

Below is an example column table, with the special configuration tables omitted:

table  | column  | label    | nulltype | default | datatype     | structure            | description
---    | ---     | ---      | ---      | ---     | ---          | ---                  | ---
table1 | column1 | Column 1 |          | value1  |              |                      |
table1 | column2 | Column 2 | empty    |         | integer      | from(table2.column2) |
table2 | column1 | Column 1 |          |         | trimmed_line | primary              |
table2 | column2 | Column 2 |          |         | integer      | unique               |
table3 | column1 | Column 1 |          |         | word         | primary              |
table3 | column2 | Column 2 | empty    |         | word         | tree(column1)        |

The columns of the column table have the following significance:
- **table**: The name of the table to which the column belongs
- **column**: The name of the column
- **label**: If not empty, then instead of **column**, use **label** as the header for the column when saving the table.
- **nulltype**: The datatype, defined in [the datatype table](#the-datatype-table), used to represent a null value in the column. For instance the datatype 'empty' in the example above is defined to match the string '' (see the example from the section on [the datatype table](#the-datatype-table)). If a column has a **nulltype**, then values of the column that match the **nulltype**'s associated datatype are considered to be valid values for the column. If a column has no **nulltype**, this means that a null value (by default, an empty string) for the column is considered to be an invalid value.
- **default** (optional column): The default value to use for the column when inserting a row of data to the database. Note that in the database this implies that a `DEFAULT` constraint will be declared for the column.
- **datatype**: The column's datatype, which must be one of the valid datatypes defined in the [the datatype table](#the-datatype-table)
- **structure**: Valve recognises the following four structural constraints on columns:
  - `primary`: The column is the primary key for the table to which it belongs; values must therefore be unique. Note that in the database this implies that a `PRIMARY KEY` constraint will be declared for the column.
  - `unique`: The column's values must be unique. Note that in the database this implies that a `UNIQUE` constraint will be declared for the column.
  - `from(foreign_table.foreign_column)`: All non-null values of the column must exist in the column `foreign_column` of the table `foreign_table`. Note that in the database this implies that a `FOREIGN KEY` constraint will be declared for the column, unless the column's datatype is a list datatype (see [the datatype table](#the-datatype-table)), and it also implies that a `UNIQUE` constraint will be declared for `foreign_table.foreign_column`, unless a `unique` structure has already been declared for that column in the column table.
  - `tree(column_name)`: All non-null values of the column must exist in the column `column_name` of the same table
- **description**: A description of the contents and/or the purpose of the column.

#### The datatype table

In addition to the table table and the column table, Valve also requires the user to configure a datatype table. The datatype table configuration is normally stored in a file called 'datatype.tsv', though in principle any filename may be used as long as the **type** field corresponding to the filename is set to 'datatype in [the table table](#the-table-table). The datatype table stores the definitions of the datatypes referred to in the **datatype** column of the column table.

Below is a subset of the rows of an example datatype table:

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
- **datatype**: The name of the datatype
- **parent**: The more generic datatype, if any, that this datatype is a special case of. When the value of a given column violates its datatype **condition**, Valve will move up the datatype hierarchy to determine whether its ancestors' datatype conditions have also been violated, and if so, Valve will add validation error messages corresponding to these further violations to the error messages it assigns to that value in that column.
- **condition**: The logical condition used to validate whether a given data value conforms to the datatype (see [datatype-conditions](#datatype-conditions) below)
- **description**: A description of the datatype and/or its purpose.
- **sql_type**: The SQL type to use for columns that have the given datatype in the database. If empty, the SQL type of the nearest ancestor for which a SQL type has been defined will be used.
- **HTML type** (optional column): The HTML type corresponding to the datatype.
- **format** (optional column): The sprintf-style format string to apply to values of the datatype when saving them.

##### Datatype conditions

- `match(/REGEX/)`: Violated if a given value does not match `REGEX`.
- `exclude(/REGEX/)`: Violated if a given value contains an instance of `REGEX`.
- `search(/REGEX/)`: Violated if a given value does not contain an instance of `REGEX`.
- `equals(VAL)`: Violated if a given value is not equal to `VAL`.
- `in(VAL1, ...)`: Violated if a given value is not one of the values in the list: `VAL1, ...`
- `list(ITEM_DATATYPE, SEPARATOR)`: Values of the given column are in the form of a sequence of items, each of datatype `ITEM_DATATYPE`, separated by the string `SEPARATOR`. This condition is violated whenever a value of the column is not in this form, otherwise it is violated if any of the items in the given list fail to conform to `ITEM_DATATYPE`.

#### Required datatypes

Valve requires that the following datatypes be defined:
- `text`, `empty`, `line`, `trimmed_line`, `nonspace`, `word`

The recommended datatype configurations for these four datatypes are the following:

datatype     | parent       | condition              | description | sql_type | HTML type | format
---          | ---          | ---                    | ---         | ---      | ---       | ---
text         |              |                        |             | TEXT     | textarea  |
empty        | text         | equals('')             |             | NULL     |           |
line         | text         | exclude(/\n/)          |             |          | input     |
trimmed_line | line         | match(/\S([^\n]*\S)*/) |             |          |           |
nonspace     | trimmed_line | exclude(/\s/)          |             |          |           |
word         | nonspace     | exclude(/\W/)          |             |          |           | %s

#### The rule table

In addition to the table table, the column table, and the datatype table, it is also possible (but optional) to configure a table of type 'rule', or a rule table. When it is configured, the rule table configuration is normally stored in a file called 'rule.tsv', though in principle any filename may be used as long as the **type** field corresponding to the filename is set to 'rule in [the table table](#the-table-table).

The rule table is used to define a number of rules of the following form:

&nbsp; &nbsp; &nbsp; &nbsp; *when `CONDITION_ON_COLUMN_1` is satisfied then `CONDITION_ON_COLUMN_2` must also be satisfied* &nbsp; &nbsp; &nbsp; &nbsp; (1)

where `CONDITION_ON_COLUMN_1` and `CONDITION_ON_COLUMN_2` are defined with respect to the same table.

Below is an example rule table:

table  | when column | when condition | then column | then condition | level | description
---    | ---         | ---            | ---         | ---            | ---   | ---
table1 | foo         | null           | bar         | not null       | error |
table2 | foo         | negative_int   | bar         | positive_int   | error |

The columns of the rule table have the following significance:

- **table**: The name of the table to which the rule is applicable.
- **when column**: The column that the **when condition** will be checked against.
- **when condition**: The condition to apply to values of **when column**
- **then column**: The column that the **then condition** will be checked against.
- **then condition**: The condition to apply to values of **then column** whenever the **when condition** has been satisfied for **when column**.
- **level**: The severity of the violation
- **description**: A description of the rule and/or its purpose.

#### Using **guess**

In some cases it is useful to be able to try and infer what the table table and column table configuration should be, given the current state of the Valve instance, for a given data table not currently managed by Valve. To do this one may use Valve's command line interface to run the **guess** subcommand as follows:

    ontodev_valve guess [OPTIONS] SOURCE DESTINATION TABLE_TSV

where:
- `SOURCE` is the location of the '.tsv' representing the table table.
- `DESTINATION` is the path to a PostgreSQL or SQLite database.
- `TABLE_TSV` is the '.tsv' file representing the data table whose column configuration is to be guessed.

For the list of possible options, and for general information on Valve's command line interface, see [command line usage](#command-line-usage). Below is an example of using **guess**:

    $ ./valve guess test/guess_test_data/table.tsv build/valve_guess.db test/guess_test_data/ontology/table2.tsv 

    The following row will be inserted to "table":
    table                                     path                                      type                                      description                               
    table2                                    test/guess_test_data/ontology/table2.tsv                                                                                      

    The following row will be inserted to "column":
    table               column              label               nulltype            datatype            structure           description         
    table2              zork                                                        integer             primary                                 
    table2              zindy                                                       IRI                 unique                                  
    table2              xyzzy                                                       suffix              unique                                  
    table2              foo                                                         prefix                                                      
    table2              bar                                                         IRI                 unique                                  

    Do you want to write this updated configuration to the database? [y/N] y


## Command line usage

The basic syntax when calling Valve on the command line is:

```rust
ontodev_valve [OPTIONS] <SUBCOMMAND <SUBCOMMAND POSITIONAL PARAMETERS>>
```

To view the list of possible subcommands and global options, run:
```rust
ontodev_valve --help
```
To get help on a particular subcommand, and on the positional parameters and options that are specific to it, run:
```rust
ontodev_valve SUBCOMMAND --help
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

## Troubleshooting

TODO.

## Python bindings
See [valve.py](https://github.com/ontodev/valve.py)

License: BSD-3-Clause

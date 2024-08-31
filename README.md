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

Valve reads in the contents of user-defined data tables and represents them using a spreadsheet-like structure incorporating "rich" or annotated cells that may subsequently be edited and saved. The annotations are of two kinds:

1. **Data validation messages** indicating that the value of a particular cell violates one or more of a number of user-defined data validation rules.
2. **Data update messages** indicating that the value of a particular cell has changed as well as the details of the change.

After reading and optionally validating the initial data, the validated information is loaded into a database that can then be queried directly using your favourite SQL client (e.g., [sqlite3](https://sqlite.org/cli.html) or [psql](https://www.postgresql.org/docs/current/app-psql.html)) as in the examples below. The currently supported databases are [SQLite](https://sqlite.org) and [PostgreSQL](https://www.postgresql.org). Valve further provides an [Application-programmer interface (API)](#api) that can be used to incorporate Valve's data validation and manipulation functionality and to visualize the validated data within your own [rust project](https://www.rust-lang.org/). For an example of a rust project that uses Valve, see [ontodev/nanobot.rs](https://github.com/ontodev/nanobot.rs). Finally, Valve also provides a [Command line interface (CLI)](#command-line-usage) for some of its more important functions like `load`, `save`, and `guess`.

### The Valve database

#### Data tables and views

Below is an example of one of the views Valve provides into a data table in which three of the five rows contain invalid data, and one row has been edited since it was initially loaded. The output is from a [psql](https://www.postgresql.org/docs/current/app-psql.html) session connected to a PostgreSQL database called 'valve_postgres'.

    valve_postgres=> select * from table11_text_view

 row_number | row_order |                                                                     message                                                    | history | child | xyzzy | foo | bar | parent 
------------|-----------|--------------------------------------------------------------------------------------------------------------------------------|---------|-------|-------|-----|-----|--------
 1 | 1000      |                                                                                                                                |         | a     | c     | d   | e   | b
 2 | 2000      |                                                                                                                                |         | b     | d     | e   | f   | c
 3 | 3000      | `[{"column":"foo","value":"d","level":"error","rule":"key:primary","message":"Values of foo must be unique"}]` | `[[{"column":"child","level":"update","message":"Value changed from 'z' to 'g'","old_value":"z","value":"g"}]]`        | g     | e     | d   | c   | f
 4 | 4000      | `[{"column":"foo","value":"e","level":"error","rule":"key:primary","message":"Values of foo must be unique"}]` |         | f     | x     | e   | z   | g
 5 | 5000      | `[{"column":"foo","value":"e","level":"error","rule":"key:primary","message":"Values of foo must be unique"}]` |         | d     | y     | e   | w   | h

    (5 rows)

Here, **row_number** is a fixed identifier assigned to each row at the time of creation, while **row_order**, which in principle might change multiple times throughout the lifetime of a given row, is used to manipulate the logical order of rows in the table. The **message** column, in this example, conveys that the value 'd' of the column "foo" in row 3, and the value 'e' of the same column in rows 4 and 5, are duplicates of already existing values of that column, which happens to be a primary key, and are therefore invalid. We can also see, from the **history** column, that there has been one change, in which one of values in row 3, namely the value of the column "child", has been changed from 'z' to 'g'. Finally, the names of the columns to the right of the **history** column correspond to the column names of the source table and will therefore vary from table to table. Normally these column names are specified in the header of a '.tsv' file from which the source data is read, though see [below](#further-information-on-path) for alternate input data formats and associated table options. In any case the data contained in the columns to the right of the **history** column will exactly match the contents of the source table unless the data has been edited since it was initially loaded.

For the example below we will assume that a file named `table6.tsv` exists on your hard disk in your current working directory with the following contents:

child | parent | xyzzy | foo | bar
------|--------|-------|-----|----
1     | 2      | 4     | e   |
2     | 3      | 5     |     | 25
3     | 4      | 6     | e   | 25
4     | 5      | 7     | e   | 23
5     | 6      | 8     |     |
6     | 7      | 1     |     |
7     | 8      | 26    |     |
8     |        |       |     |
9     |        |       |     |

In order for Valve to read this table it must first be configured to do so. This is done using a number of special data tables called [configuration tables](#configuration), usually represented using further '.tsv' files, that contain information about:

1. Where to find the data tables and other general properties of each managed data table (i.e., the [table table](#the-table-table)).
2. The datatypes represented in the various data tables (i.e., the [datatype table](#the-datatype-table)).
3. Information about the columns of each data table: their associated datatypes, data dependencies between the values in one table column and some other one, and any other constraints or restrictions on individual columns (i.e., the [column table](#the-column-table)).
4. Rules constraining the joint values of two different cells in a single given row of data (i.e., the [rule table](#the-rule-table)).

For our example we will assume that Valve's configuration tables contain the following entries:

* **Table table**

table  | path       | description | type | options
-------|------------|-------------|------|---------
table6 | table6.tsv |             |      |

* **Column table**

table  | column | label | nulltype | default | datatype | structure          | description
-------|--------|-------|----------|---------|----------|--------------------|------------
table6 | child  |       |          |         | integer  | from(table4.child) |
table6 | parent |       | empty    |         | integer  | tree(child)        |
table6 | xyzzy  |       | empty    |         | integer  |                    |
table6 | foo    |       | empty    |         | text     |                    |
table6 | bar    |       | empty    |         | integer  |                    |

* **Datatype table**

datatype     | parent       | condition              | description | sql_type
-------------|--------------|------------------------|-------------|---------
integer      | nonspace     | match(/-?\d+/)         |             | INTEGER
nonspace     | trimmed_line | exclude(/\s/)          |             |
trimmed_line | line         | match(/\S([^\n]*\S)*/) |             |
line         | text         | exclude(/\n/)          |             |
word         | nonspace     | exclude(/\W/)          |             |
empty        | text         | equals('')             |             |
text         |              |                        |             | TEXT

* **Rule table**

table  | when_column | when_condition | then_column | then_condition | level | description
------ |-------------|----------------|-------------|----------------|-------|------------
table6 | foo         | null           | bar         | null           | error | bar must be null whenever foo is null
table6 | foo         | not null       | bar         | not null       | error | bar cannot be null if foo is not null
table6 | foo         | nonspace       | bar         | word           | error | bar must be a word if foo is nonspace
table6 | foo         | equals(e)      | bar         | in(25, 26)     | error | bar must be 25 or 26 if foo = 'e'

For the meanings of all of the columns in the configuration tables above, see the section on [configuration](#configuration). In the rest of this section we'll refrain from explaining the meaning of a particular configuration table column unless and until it becomes relevant to our example. What is relevant at this point is only that each configuration table is _also_ a data table whose contents are themselves subject to validation by Valve. In other words Valve will not necessarily fail to run if there are errors in its configuration (as long as those errors aren't critical) and it can moreover help to identify what those errors are. The upshot is that almost everything I mention below regarding `table6` also applies to the special configuration tables `table`, `column`, `datatype` and `rule` unless otherwise noted.

Once it has been read in by Valve from its source file, a given logical table will be represented, in the database, by between one and two database tables and by as many as two database views. In our example, the source data, contained in the file 'table6.tsv', represents (according to the table table configuration) a normal data table with the default options set, which means that all four database tables and views will be created. These are:

1. `table6`: The database table, having the same name as the logical table, that normally contains the bulk of the table's data.
2. `table6_conflict` (only when the [_conflict_ option](#the-table-table) is set): Valve aims to represent user data whether or not that data is valid. When it is not valid, however, this presents an obstacle when it comes to representing it in a relational database like SQLite or PostgreSQL. For instance, if a table contains a primary key column, then data rows containing values for the column that already exist in the table should be marked as invalid, but still somehow represented by Valve. However the implied database constraint on the database table will prevent us from inserting duplicate values to a primary key column. A similar issue arises for unique and foreign key constraints. To get around the database limitations that are due to database constraints such as primary, unique, or foreign keys, Valve constructs a `_conflict` table that is identical to the normal version of the table, but that does not include those keys. In our example, any rows containing duplicate values of the primary key column will be inserted to the database table called `table6_conflict`, while the other rows will be inserted to `table6`.
3. `table6_view` (only when the [_conflict_ option](#the-table-table) is set): When Valve validates a row from the logical table, `table6`, the validation messages it generates are not stored in the database table called `table6` but instead in a [special internal database table](#the-message-and-history-tables) called `message`. Similarly, when Valve adds, updates, or deletes a row from `table6`, a record of the change is not stored in `table6`, but in a [special internal database table](#the-message-and-history-tables) called `history`. Because it can be convenient for information about the validity of a particular cell to be presented side by side with the value of the cell itself, Valve constructs a table called `table6_view` which combines information from `table6` and `table6_conflict` with the `message` and `history` tables.
4. `table6_text_view` (only when the [_conflict_ option](#the-table-table) is set): In addition to the restrictions associated with primary, unique, and foreign key constraints, the database will also not allow us to represent values that are invalid due to an incorrect SQL type, in particular when one attempts to insert a non-numeric string value to a numeric column of a database table. To get around this limitation, Valve constructs a database view called `table6_text_view`. Unlike `table6_view`, which is defined such that the datatype of each column in `table6_view` exactly matches the datatype of the corresponding datatype in `table6`, in `table6_text_view` all of the columns are cast to TEXT so that no SQL datatype errors can occur when representing the data.

After loading the data from `table6.tsv` into the database, these four tables and views will be found to have the following contents:

    valve_postgres=> select * from table6;

row_number | row_order | child | parent | xyzzy | foo | bar 
-----------|-----------|-------|--------|-------|-----|-----
1          | 1000      | 1     | 2      | 4     | e   |
2          | 2000      | 2     | 3      | 5     |     | 25
3          | 3000      | 3     | 4      | 6     | e   | 25
4          | 4000      | 4     | 5      | 7     | e   | 23
5          | 5000      | 5     | 6      | 8     |     |
6          | 6000      | 6     | 7      | 1     |     |
7          | 7000      | 7     | 8      | 26    |     |
8          | 8000      | 8     |        |       |     |

    (8 rows)

    valve_postgres=> select * from table6_conflict;

row_number | row_order | child | parent | xyzzy | foo | bar 
-----------|-----------|-------|--------|-------|-----|-----
9          | 9000      | 9     |        |       |     |
(1 row)

    valve_postgres=> select * from table6_view order by row_order;

 row_number | row_order | child | parent | xyzzy | foo | bar | message | history 
------------|-----------|-------|--------|-------|-----|-----|---------|---------
 1          | 1000      | 1     | 2      | 4     | e   |     | [{"column":"foo","value":"e","level":"error","rule":"rule:foo-2","message":"bar cannot be null if foo is not null"}, {"column":"foo","value":"e","level":"error","rule":"rule:foo-4","message":"bar must be 25 or 26 if foo = 'e'"}] |
 2 |      2000 |     2 |      3 |     5 |     |  25 | [{"column":"foo","value":"","level":"error","rule":"rule:foo-1","message":"bar must be null whenever foo is null"}] |
 3 |      3000 |     3 |      4 |     6 | e   |  25 | |
 4 |      4000 |     4 |      5 |     7 | e   |  23 | [{"column":"foo","value":"e","level":"error","rule":"rule:foo-4","message":"bar must be 25 or 26 if foo = 'e'"}] |
 5 |      5000 |     5 |      6 |     8 |     |     | |
 6 |      6000 |     6 |      7 |     1 |     |     | |
 7 |      7000 |     7 |      8 |    26 |     |     | |
 8 |      8000 |     8 |        |       |     |     | |
 9 |      9000 |     9 |        |       |     |     | [{"column":"child","value":"9","level":"error","rule":"key:foreign","message":"Value '9' of column child exists only in table4_conflict.numeric_foreign_column"}] |

    (9 rows)

    valve_postgres=> select * from table6_text_view order by row_order;

 row_number | row_order | child | parent | xyzzy | foo | bar | message | history 
------------|-----------|-------|--------|-------|-----|-----|---------|---------
 1          | 1000      | 1     | 2      | 4     | e   |     | [{"column":"foo","value":"e","level":"error","rule":"rule:foo-2","message":"bar cannot be null if foo is not null"}, {"column":"foo","value":"e","level":"error","rule":"rule:foo-4","message":"bar must be 25 or 26 if foo = 'e'"}] |
 2 |      2000 |     2 |      3 |     5 |     |  25 | [{"column":"foo","value":"","level":"error","rule":"rule:foo-1","message":"bar must be null whenever foo is null"}] |
 3 |      3000 |     3 |      4 |     6 | e   |  25 | |
 4 |      4000 |     4 |      5 |     7 | e   |  23 | [{"column":"foo","value":"e","level":"error","rule":"rule:foo-4","message":"bar must be 25 or 26 if foo = 'e'"}] |
 5 |      5000 |     5 |      6 |     8 |     |     | |
 6 |      6000 |     6 |      7 |     1 |     |     | |
 7 |      7000 |     7 |      8 |    26 |     |     | |
 8 |      8000 |     8 |        |       |     |     | |
 9 |      9000 |     9 |        |       |     |     | [{"column":"child","value":"9","level":"error","rule":"key:foreign","message":"Value '9' of column child exists only in table4_conflict.numeric_foreign_column"}] |

    (9 rows)

#### The **message** and **history** tables

In the previous section we mentioned that `table6_view` and `table6_text_view` are defined in terms of a query that draws on data from `table6` and `table6_conflict` as well as from the special internal tables called `message` and `history`. The information in these tables can also be queried directly. After loading the data from `table6.tsv` into the database, one will find that the `message` table contains the following contents:

    valve_postgres=> select * from message where "table" = 'table6';

message_id | table  | row | column | value | level | rule     | message
-----------|--------|-----|--------|-------|-------|----------|---------
36 | table6 | 1 | foo    | e     | error | rule:foo-2  | bar cannot be null if foo is not null
37 | table6 | 1 | foo    | e     | error | rule:foo-4  | bar must be 25 or 26 if foo = 'e'
38 | table6 | 2 | foo    |       | error | rule:foo-1  | bar must be null whenever foo is null
39 | table6 | 4 | foo    | e     | error | rule:foo-4  | bar must be 25 or 26 if foo = 'e'
40 | table6 | 9 | child  | 9     | error | key:foreign | Value '9' of column child exists only in table4_conflict.numeric_foreign_column

    (5 rows)

Here,
- **message_id** is a unique identifier for this message assigned when it is created.
- **table** is the table that this message is associated with.
- **row** is the row_number of the table that this message is associated with.
- **column** is the column in the row of the table that this message is associated with.
- **value** is what the message is about.
- **level** is the severity of the message.
- **rule** identifies the rule that, when applied to the given value, resulted in the message.
- **message** is the text of the message.

The **history** table will be empty immediately after the initial loading of the database.

    valve_postgres=> select * from history where "table" = 'table6';

history_id | table | row | from | to | summary | user | undone_by | timestamp 
-----------|-------|-----|------|----|---------|------|-----------|-----------
 |     |     |     |    |       |     |          |

    (0 rows)

Here,
- **history_id** is a unique identifier for this history record that is assigned when it is created.
- **table** is the table that this record is associated with.
- **row** is the row number of the table that this record is associated with.
- **from** is a JSON representation of the row as it was before the change (see the section on [data validation](#data-validation)).
- **to** is a JSON representation of the row after the change (see the section on [data validation](#data-validation)).
- **user** is the name of the user that initiated the change, or who undid or redid the change (if applicable).
- **undone_by** is the name of the user that undid the change (if applicable).
- **timestamp** records the time of the change, or the time that it was undone or redone (if applicable).
- **summary** is a JSON representation of a summary of the change in the form of an array of records corresponding to each column that has changed. e.g.,
```json
        [
            {
                "column":"bar",
                "level":"update",
                "message":"Value changed from '' to 2",
                "old_value":"",
                "value":"2"
            },
            {
                "column":"child",
                "level":"update",
                "message":"Value changed from 1 to 2",
                "old_value":"1",
                "value":"2"
            },
            {
                "column":"foo",
                "level":"update",
                "message":"Value changed from 'e' to 'a'",
                "old_value":"e",
                "value":"a"
            },
            {
                "column":"xyzzy",
                "level":"update",
                "message":"Value changed from 4 to 23",
                "old_value":"4",
                "value":"23"
            }
        ]
```

Note that the **summary** column of the **history** table is where the information in the view columns `table6_view.history` and `table6_text_view.history` is taken from, which is why these view columns are always in the form of an array of arrays, i.e., an array of summary records for the given row.

### Data validation and editing

#### Representing validated data

For the purposes of this section we will define a *cell* as a single value, that can either be valid or invalid, of one of the columns of a given data table. We will further define a *row* of data, from a given data table, to be an array of correlated cells, such that for every column in the table there is one and only one cell in the row corresponding to it. Once a given row of data has been inserted into the database and (optionally) validated, Valve represents it using a `ValveRow` struct (see the [API reference](#api)) containing the following information:

1. The row's fixed, unique, identifier, or `row_number`.
1. A map from the names of the columns contained in the row, to the `ValveCell` struct associated with each, where the latter represents the results of running Valve's validation engine (see [the validation process](#the-validation-process)) on the cell associated with that column.

In particular, each `ValveCell` contains the following information:

1. The value of the cell.
1. Whether the value should be interpreted as a null value of the given column, and if so, the column's nulltype.
1. Whether or not the value is valid given the column's [column configuration](#the-column-table).
1. A list of `ValveCellMessage` structs representing the validation messages associated with this cell.

Each `ValveCellMessage`, in turn, contains the following information:

1. The level, or severity, of the message (e.g., "error", "warn", "info").
1. An alphanumeric identifier for the rule violation described by the message (see the section on [rule violation identifiers](#rule-violation-identifiers)).
1. The text of the message.

The following is a textual representation of an example of a `ValveRow`:

```rust
ValveRow {
    row_number: Some(
        11,
    ),
    contents: {
        "id": ValveCell {
            nulltype: None,
            value: String("BFO:0000027"),
            valid: true,
            messages: [],
        },
        "name": ValveCell {
            nulltype: None,
            value: String("Mike"),
            valid: true,
            messages: [],
        },
        "location": ValveCell {
            nulltype: None,
            value: String("baree"),
            valid: false,
            messages: [
                ValveCellMessage {
                    level: "error",
                    rule: "key:foreign",
                    message: "Value 'baree' of column location is not in cities.city_name",
                },
            ],
        },
        "preferred_seafood": ValveCell {
            nulltype: Some("empty"),
            value: String(""),
            valid: true,
            messages: [],
        },
    },
}
```

##### Rule violation identifiers

Valve uses the following to identify the rule that has been violated by a given cell value in the `ValveCellMessage` associated with the violation:

- **key:foreign**: The column that the given value belongs to has a `from()` structure (see [the column table](#the-column-table) that references some column, F, in another table, but the given value is not in F.
- **key:primary**: The column that the given value belongs to has a `primary` structure, and the given value already exists in the column.
- **key:unique**: The column that the given value belongs to has a `unique` structure, and the given value already exists in the column.
- **option:unrecognized** (table table only): The list of options specified in the **options** column of the [table table](#the-table-table) contains an unrecognized option.
- **option:redundant**: (table table only): The list of options specified in the **options** column of the table table contains an option that is already implied by one of the other options.
- **option:reserved**: (table table only): The list of options specified in the **options** column of the table table contains an option keyword that is reserved for internal use.
- **option:overrides**: (table table only): The list of options specified in the **options** column of the table table contains an option that overrides one of the other options.
- **tree:foreign**: The column that the given value belongs to has a `tree()` structure that references some other column, T, of the same table; but the given value is not in T.
- **datatype:_DATATYPE_**: The column that the given value belongs to has the datatype, _DATATYPE_, but applying _DATATYPE_'s associated condition to the given value results in a failure.
- **rule:_COLUMN_-_N_**: The given value of _COLUMN_ causes the _Nth_ rule in the [rule table](#the-rule-table) whose `when_column` is _COLUMN_ to be violated.

#### The validation process

##### Validating a row of data

For a given row of data in a given data table, Valve's validation process consists in a series of checks that are schematically represented in the flowchart below and subsequently explained in more detail.

```mermaid
flowchart TD
    node1["1. Determine the nulltype (if any) of each cell in the row"]
    node1 -- "Then, for each cell:" --> node2
    node2["2. Validate rules"]
    node2 --> modal1
    modal1{"Does the cell have a nulltype?"}
    modal1 -- No --> node3
    modal1 -- Yes, skip further validation for this cell --> modal3
    node3["3. Validate datatype"]
    node3 --> modal2
    modal2{"Does the cell value contain a SQL type error?"}
    modal2 -- No --> node4
    modal2 -- Yes --> modal3
    node4["4. Validate foreign constraints"]
    node4 --> node5
    node5[5. "Validate primary and unique Constraints"]
    node5 --> modal3
    modal3{"Have we iterated over all of the cells in the row?"}
    modal3 -- Yes, then over the table as a whole: --> node6
    modal3 -- No, go on to the next cell --> node2
    node6[6. "Validate tree-foreign keys"]
```

###### Determining the nulltype of a cell

The validation process begins by determining, for each cell in the row, whether the value of that cell matches the nulltype (if any) of its associated column, as defined in the [column table](#the-column-table). In particular, if the value of the cell matches the nulltype of its associated column, then the `nulltype` field of the `ValveCell` struct used to represent the cell will be set to indicate that the value is a null value of that type. Otherwise the `nulltype` field will remain unset, indicating that the value is not a null value. For instance, suppose that the cell value is '' (i.e., the empty string), and that the nulltype for its associated column, as defined in the [column table](#the-column-table), is `empty`. Since `empty`'s associated condition, as defined in the [datatype table](#the-datatype-table) is `equals('')`, applying it to the cell value will result in a match, and Valve will set the `nulltype` field for the `ValveCell` representing this particular cell to `empty`. In the case where the value of the cell does *not* match the condition associated with the datatype, `empty`, (i.e., when the cell value is something other than an empty string), the validation process will leave the `nulltype` field of the `ValveCell` unset.

###### Validating rules

This step of the validation process determines whether any of the rules in the [rule table](#the-rule-table) that are applicable to a cell have been violated. A rule in the rule table is applicable to a cell when the cell's associated column is the same as the **when_column** associated with the rule. Note that since the rules in the rule table correspond to **if-then** conditionals, such that the antecedent and consequent of a given conditional refer (in general) to two distinct columns, a rule violation may indicate that there is a problem with the value of either or both. Whenever a rule violation occurs, a `ValveCellMessage` struct is added to the list of messages associated with the cell, identifying the particular violation that occurred (see the section on [rule violation IDs](#rule-violation-identifiers)) and its associated `level` and `description` as found in the [rule table](#the-rule-table).

###### Validating datatypes

This step of the validation process determines whether a cell's value violates the datatype condition, as defined in the [datatype table](#the-datatype-table), for the datatype associated with the cell's column in the [column table](#the-column-table). When a datatype violation occurs, a `ValveCellMessage` struct is added to the list of messages associated with the cell, identifying the particular datatype violation that occurred (see the section on [rule violation IDs](#rule-violation-identifiers)). The text of the message is taken from the description of the datatype that has been violated (see the [datatype table](#the-datatype-table)).

###### Validating foreign constraints

This step in the validation process verifies, for a given cell, that if the cell's associated, `C`, column has been configured with a structure of the form `from(T, F)` (see the [column-table](#the-column-table)), where `T` is a foreign table and `F` is a column in `T`, then the cell's value (or values if `C`'s datatype is a [list datatype](#the-datatype-table)) is (are) among the values of `F`. Note that if the foreign table has a `_conflict` version (see [the table table](#the-table-table)), then this function will distinguish between (a) the case in which a given value is not found in either the foreign table or its associated conflict table, and (b) the case in which a given value is found only in the conflict table. When a foreign constraint violation occurs, a `ValveCellMessage` struct is added to the list of messages associated with the cell with the identifier `key:foreign` (see also the section on [rule violation IDs](#rule-violation-identifiers)). The text of the message will be of the form: `Value 'V' of column C is not in T.F`, whenever `V` is neither found in the normal version of the table nor (if applicable) its conflict version, and it will be of the form `Value 'V' of column C exists only in T_conflict.F` whenever `V` exists in `T_conflict` but not in `T`.

###### Validating primary and unique constraints

This step in the validation process verifies, for a given cell, that if the cell's associated column has been configured with either a `primary` or a `unique` constraint (see the [column-table](#the-column-table)), then the cell's value is not among the values of the column that have already been inserted into the table, neither in the normal version of the table nor in the conflict version (in the case where the *conflict* option has been set). When a primary or unique constraint violation occurs, a `ValveCellMessage` struct is added to the list of messages associated with the cell with the appropriate [rule violation ID](#rule-violation-identifiers). The text of the message is of the form `Values of COLUMN must be unique`.

###### Validating tree-foreign keys

When a column, `column2` has a structure `tree(column1)` defined on it in the [column table](#the-column-table), then all non-null values of `column2` must exist in `column1`. This step in the validation process function verifies, for a given table, whether any of the values of any of the cells in any of the rows violate any of the table's `tree()` conditions. When a primary or unique constraint violation occurs, a `ValveCellMessage` struct is added to the list of messages associated with the cell with the appropriate [rule violation ID](#rule-violation-identifiers). The text of the message is of the form `Value 'VAL' of column COLUMN2 is not in COLUMN1`.

##### Batch validation

The algorithm described in [the previous section](#validating-a-row-of-data) is applicable to a single row of data. When initially loading the many rows of a data table from its source into the database, however, a number of optimizations may be used to speed up the validation process. In particular, some of the steps for validating a row's cells from the previous section do not need to refer to any data external to a given row, and may therefore be performed in parallel. These are steps 1&ndash;3, i.e., the nulltype, rules, and datatype validation steps. Only the remaining steps: foreign and unique/primary constraint validation, need be performed row by row.

#### Inserting a new row

TODO.

#### Updating a row

TODO.

#### Deleting a row

TODO.

#### Moving a row

TODO.

## Installation and configuration

### Prerequisites

TODO.

#### Differences between PostgreSQL and SQLite

TODO.

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

Valve is configured primarily using a number of special configuration tables that can be represented as '.tsv' files. The most important of these is the table called 'table', also known as the table table. A [table table](#the-table-table) configuration is required to use Valve. A [column table](#the-column-table) and [datatype table](#the-datatype-table) configuration are required as well. Optionally, the user may also specify a [rule table](#the-rule-table) configuration.

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
user_readonly_table1 | schema/user/user_readonly_table1.tsv |             |          | no-edit no-save no-conflict
user_view1           | schema/user/user_view1.sql           |             |          | db_view
user_view2           | schema/user/user_view2.sh            |             |          | db_view
user_view3           |                                      |             |          | db_view

Note that in the first row above the table being described is the table table itself. In general the columns of the table table have the following significance:
- **table**: the name of the table.
- **path**: where to find information about the contents of the table (see below). Note that the path for one of the special configuration tables must be a '.tsv' file.
- **description**: An optional description of the contents and/or the purpose of the table.
- **type**: Valve recognizes four special configuration table types that can be specified using the **type** column of the table table. These are the `table`, `column`, `datatype`, and `rule` table types. Data tables (e.g., the 'user_*' tables in the above example) should not explicitly specify a type, and in general if a type other than the ones just mentioned is specified, Valve will exit with an "Unrecognized table type" error.
- **options** (optional column): Allows the user to specify a number of further options for the table (see below).

##### Further information on **path**

The **path** column indicates where the data for a given table may be found. It can be (a) a '.tsv' file, (b) a '.sql' file, (c) some other executable file, or (d) it may be empty. In each case it will have the following consequences for the possible values of **type** and and for the possible **options** that may be used with the table.

1. If **path** ends in '.tsv':
   - Its associated **type** may be any one of `table`, `column`, `datatype`, `rule`, or it may be empty.
   - The *db_view* option is not allowed. If set to true, Valve will fail with a "'.tsv' files are not supported for views" error.

2. If **path** does not end in '.tsv':
   - Its associated **type** must be empty. Note that this implies that Valve's [configuration tables](#configuration) _must_ be specified using '.tsv' files.
   - The *edit* option is not allowed. If set to true, Valve will fail with an "Editable tables require a path that ends in '.tsv'" error.

3. If **path** either ends in '.sql', or represents a generic executable:
   - If the *db_view* option is set, Valve assumes that the '.sql' file or generic executable indicated contains the instructions necessary to create the view and that the view displays the appropriate data.
   - If the *db_view* option is not set, Valve will take care of creating the table but it will expect that the '.sql' file or generic executable contains the statements necessary to load the data.
   - If **path** ends in '.sql', Valve reads in the statements contained in the '.sql' file and then executes them against the database.
   - If **path** represents a generic executable, then it is executed as a shell process. Note that the executable script or binary must accept two arguments indicating the location of the database and the name of the table that the path corresponds to.

5. If **path** is empty:
   - If the *db_view* option is set, Valve assumes that the view already exists in the database.
   - If the *db_view* option is not set, Valve will take care of creating the table but it will not attempt to load it.

The above is conveniently summarized in the following table:

path               | possible types           | possible options      | created by                                    | loaded by&dagger;
-------------------|--------------------------|-----------------------|-----------------------------------------------|----------
Ends in '.tsv'     | any valid type, or empty | *db_view* not allowed | Valve                                         | Valve
Ends in '.sql'     | must be empty            | *edit* not allowed    | *db_view*: the '.sql' file, *db_table*: Valve | Valve
Generic executable | must be empty            | *edit* not allowed    | *db_view*: the executable, *db_table*: Valve  | Valve
empty              | must be empty            | *edit* not allowed    | No one (assumed to already exist)             | No one (assumed to be already loaded)

&dagger; Note that loading is only applicable when the *db_view* option has not been set.

##### Further information on **options**

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

##### Commonly used path and option combinations

Here are some examples of commonly used table table configurations:

table                | path                                 | description                                                                              | type     | options
---------------------| -------------------------------------| -----------------------------------------------------------------------------------------| ---------| ------------
plain                | src/data/plain.tsv                   | A database table with default options set, backed by a '.tsv' file                       |          |
appendonly           | src/data/appendonly.tsv              | A database table which is appended to but never truncated                                |          | no-truncate
readonly1            | src/data/readonly1.tsv               | A read-only table that is backed by a '.tsv' file                                        |          | no-edit no-save
readonly2            | src/data/readonly2.sh                | A read-only table whose contents are assumed to be valid, backed by a generic executable |          | no-edit no-conflict no-validate-on-load
view1                | src/data/view1.sql                   | A view backed by a '.sql' file                                                           |          | db_view
table                | src/table.tsv                        | the table table                                                                          | table    |
column               | src/column.tsv                       | the column table                                                                         | column   |
datatype             | src/datatype.tsv                     | the datatype table                                                                       | datatype |
rule                 | src/rule.tsv                         | the rule table                                                                           | rule     |

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

In addition to the table table and the column table, Valve also requires the user to configure a datatype table. The datatype table configuration is normally stored in a file called 'datatype.tsv', though in principle any filename may be used as long as the **type** field corresponding to the filename is set to 'datatype' in [the table table](#the-table-table). The datatype table stores the definitions of the datatypes referred to in the **datatype** column of the column table.

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
- **condition**: The logical condition used to validate whether a given data value conforms to the datatype (see [condition types](#condition-types) below)
- **description**: A description of the datatype and/or its purpose.
- **sql_type**: The SQL type to use for columns that have the given datatype in the database. If empty, the SQL type of the nearest ancestor for which a SQL type has been defined will be used.
- **HTML type** (optional column): The HTML type corresponding to the datatype.
- **format** (optional column): The sprintf-style format string to apply to values of the datatype when saving them.

##### Condition types

- `match(/REGEX/)`: Violated if a given value does not match `REGEX`.
- `exclude(/REGEX/)`: Violated if a given value contains an instance of `REGEX`.
- `search(/REGEX/)`: Violated if a given value does not contain an instance of `REGEX`.
- `equals(VAL)`: Violated if a given value is not equal to `VAL`.
- `in(VAL1, ...)`: Violated if a given value is not one of the values in the list: `VAL1, ...`
- `list(ITEM_DATATYPE, SEPARATOR)`: Violated if a given value is not in the form of a sequence of items, each of datatype `ITEM_DATATYPE`, separated by the string `SEPARATOR`. Otherwise the condition is violated if any of the items in the given list fail to conform to `ITEM_DATATYPE`.

#### Required datatypes

Valve requires that the following datatypes be defined:
- `text`, `empty`, `line`, `trimmed_line`, `nonspace`, `word`

The recommended datatype configurations for these four datatypes are the following (note that the `HTML type` and `format` columns are optional):

datatype     | parent       | condition              | description | sql_type | HTML type | format
---          | ---          | ---                    | ---         | ---      | ---       | ---
text         |              |                        |             | TEXT     | textarea  |
empty        | text         | equals('')             |             | NULL     |           |
line         | text         | exclude(/\n/)          |             |          | input     |
trimmed_line | line         | match(/\S([^\n]*\S)*/) |             |          |           |
nonspace     | trimmed_line | exclude(/\s/)          |             |          |           |
word         | nonspace     | exclude(/\W/)          |             |          |           | %s

#### The rule table

In addition to the table table, the column table, and the datatype table, it is also possible (but optional) to configure a table of type 'rule', or a rule table. When it is configured, the rule table configuration is normally stored in a file called 'rule.tsv', though in principle any filename may be used as long as the **type** field corresponding to the filename is set to 'rule' in [the table table](#the-table-table).

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
- **when condition**: The condition to apply to values of **when column**. This can either be one of the recognized [condition types](#condition-types), or it can be the name of a datatype in which case the condition corresponding to the datatype will be used.
- **then column**: The column that the **then condition** will be checked against.
- **then condition**: The condition to apply to values of **then column** whenever the **when condition** has been satisfied for **when column**. This can either be one of the recognized [condition types](#condition-types), or it can be the name of a datatype in which case the condition corresponding to the datatype will be used.
- **level**: The severity of the violation
- **description**: A description of the rule and/or its purpose.

#### Using **guess**

In some cases it is useful to be able to try and guess what the table table and column table configuration should be, using information about the current state of the Valve instance, for a given data table not currently managed by Valve. To do this one may use Valve's command line interface to run the **guess** subcommand as follows:

    ontodev_valve guess [OPTIONS] SOURCE DESTINATION TABLE_TSV

where:
- `SOURCE` is the location of the '.tsv' representing the table table.
- `DESTINATION` is the path to a PostgreSQL or SQLite database.
- `TABLE_TSV` is the '.tsv' file representing the data table whose column configuration is to be guessed.

For the list of possible options, and for general information on Valve's command line interface, see [command line usage](#command-line-usage).

Below is an example of using **guess**:

    $ ontodev_valve guess test/guess_test_data/table.tsv build/valve_guess.db test/guess_test_data/ontology/table2.tsv 

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

The API (application-programmer interface) reference documentation for the latest release of Valve can normally be found on [docs.rs](https://docs.rs/ontodev_valve/latest/ontodev_valve/). In case you would like to generate the API documentation for a newer version of Valve, run:

```rust
cargo doc
```

in the the root folder of your local copy of the valve.rs source code repository (see [Installation and configuration](#installation-and-configuration)), and then open the file

    <valve.rs root folder>/target/doc/ontodev_valve/index.html

in your favourite browser. For a higher level See also the [Design and concepts](#design-and-concepts) section.

## Troubleshooting

TODO.

## Python bindings
See [valve.py](https://github.com/ontodev/valve.py)

License: BSD-3-Clause

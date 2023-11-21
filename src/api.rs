fn example() {
    let valve = Valve::build("src/schema/table.tsv");
    valve.connect("valve.db");
    valve.create_all_tables();
    valve.load_all_tables();
    valve.truncate_all_tables();
    valve.validate_tables(vec!["table", "column", "datatype"]);
    valve.validate_all_tables();
    valve.add_row("table", json!("{...}"));
    valve.update_row("table", 1, json!("{...}"));
    valve.delete_row("table", 1);
    valve.next_undo();
    valve.undo();
    valve.next_redo();
    valve.redo();
    valve.save_all_tables();
}

pub type ValveRow = serde_json::Map<String, SerdeValue>;

pub struct Valve {
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool, // maybe default to an in-memory SQLite database
}

impl Valve {
    /// Given a path to a table table,
    /// read it, configure VALVE, and return a new Valve struct.
    /// Return an error if reading or configuration fails.
    pub fn build(table_path: &str) -> Result<Self, ConfigError> {
        todo!();
        self
    }

    /// Given a database connection string,
    /// create a database connection for VALVE to use.
    /// Drop and replace any current database connection.
    /// Return an error if the connection cannot be created.
    pub fn connect(&mut self, connection: &str) -> Result<Self, DatabaseError> {
        todo!();
        self
    }

    /// Create all configured database tables and views
    /// if they do not already exist as configured.
    /// Return an error on database problems.
    pub fn create_all_tables(&mut self) -> Result<Self, DatabaseError> {
        todo!();
        self
    }

    /// Drop all configured tables, in reverse dependency order.
    /// Return an error on database problem.
    pub fn drop_all_tables(self) -> Result<Self, DatabaseError> {
        todo!();
        self
    }

    /// Given a vector of table names,
    /// drop those tables, in the given order.
    /// Return an error on invalid table name or database problem.
    pub fn drop_tables(self, tables: Vec<&str>) -> Result<Self, ConfigOrDatabaseError> {
        todo!();
        self
    }

    /// Truncate all configured tables, in reverse dependency order.
    /// Return an error on database problem.
    pub fn truncate_all_tables(self) -> Result<Self, DatabaseError> {
        todo!();
        self
    }

    /// Given a vector of table names,
    /// truncate those tables, in the given order.
    /// Return an error on invalid table name or database problem.
    pub fn truncate_tables(self, tables: Vec<&str>) -> Result<Self, ConfigOrDatabaseError> {
        self.create_all_tables();
        todo!();
        self
    }

    /// Load all configured tables without validating, in dependency order.
    /// Return an error on database problem,
    /// including database conflicts that prevent rows being inserted.
    pub fn load_all_tables(self) -> Result<Self, DatabaseError> {
        self.create_all_tables();
        self.truncate_all_tables();
        todo!();
        self
    }

    /// Given a vector of table names,
    /// load those tables without validating, in the given order.
    /// Return an error on invalid table name or database problem.
    pub fn load_tables(self, tables: Vec<&str>) -> Result<Self, ConfigOrDatabaseError> {
        self.create_all_tables();
        self.truncate_tables(tables);
        todo!();
        self
    }

    /// Load and validate all configured tables in dependency order.
    /// Return an error on database problem.
    pub fn validate_all_tables(self) -> Result<Self, DatabaseError> {
        self.create_all_tables();
        self.truncate_all_tables();
        todo!();
        self
    }

    /// Given a vector of table names,
    /// load and validate those tables in the given order.
    /// Return an error on invalid table name or database problem.
    pub fn validate_tables(self, tables: Vec<&str>) -> Result<Self, ConfigOrDatabaseError> {
        self.create_all_tables();
        self.truncate_tables(tables);
        todo!();
        self
    }

    /// Save all configured tables to their 'path's.
    /// Return an error on writing or database problem.
    pub fn save_all_tables(self) -> Result<Self, WriteOrDatabaseError> {
        todo!();
        self
    }

    /// Given a vector of table names,
    /// Save thosee tables to their 'path's, in the given order.
    /// Return an error on writing or database problem.
    pub fn save_tables(self, tables: Vec<&str>) -> Result<Self, WriteOrDatabaseError> {
        todo!();
        self
    }

    /// Given a table name and a row as JSON,
    /// return the validated row.
    /// Return an error on database problem.
    pub fn validate_row(self, &str: table_name, &ValveRow: row) -> Result<ValveRow, DatabaseError> {
        todo!();
    }

    /// Given a table name and a row as JSON,
    /// add the row to the table in the database,
    /// and return the validated row, including its new row_number.
    /// Return an error invalid table name or database problem.
    pub fn add_row(
        self,
        table_name: &str,
        row: &ValveRow,
    ) -> Result<ValveRow, ConfigOrDatabaseError> {
        todo!();
    }

    /// Given a table name, a row number, and a row as JSON,
    /// update the row in the database,
    /// and return the validated row.
    /// Return an error invalid table name or row number or database problem.
    pub fn update_row(
        self,
        table_name: &str,
        row_number: usize,
        row: &ValveRow,
    ) -> Result<ValveRow, ConfigOrDatabaseError> {
        todo!();
    }

    /// Given a table name and a row number,
    /// delete that row from the table.
    /// Return an error invalid table name or row number or database problem.
    pub fn delete_row(
        self,
        table_name: &str,
        row_number: usize,
    ) -> Result<(), ConfigOrDatabaseError> {
        todo!();
    }

    /// Return the next change to undo, or None.
    /// Return an error on database problem.
    pub fn next_undo(self) -> Result<Option<ValveChange>, DatabaseError> {
        todo!();
    }

    /// Return the next change to redo, or None.
    /// Return an error on database problem.
    pub fn next_redo(self) -> Result<Option<ValveChange>, DatabaseError> {
        todo!();
    }

    /// Undo one change and return the change record
    /// or None if there was no change to undo.
    /// Return an error on database problem.
    pub fn undo(self) -> Result<Option<ValveChange>, DatabaseError> {
        todo!();
    }

    /// Redo one change and return the change record
    /// or None if there was no change to redo.
    /// Return an error on database problem.
    pub fn redo(self) -> Result<Option<ValveChange>, DatabaseError> {
        todo!();
    }
}

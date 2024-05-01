//! API tests

use anyhow::Result;
use indoc::indoc;
use ontodev_valve::{
    toolkit::SerdeMap,
    valve::{Valve, ValveError},
};
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use rand::{random, thread_rng};
use serde_json::json;
use sqlx::{any::AnyPool, query as sqlx_query, Row, ValueRef};

async fn test_matching(valve: &Valve) -> Result<()> {
    eprint!("Running test_matching() ... ");

    // Test the get_matching_values() function:
    let matching_values = valve.get_matching_values("table2", "child", None).await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"a","label":"a","order":1},
            {"id":"b","label":"b","order":2},
            {"id":"c","label":"c","order":3},
            {"id":"d","label":"d","order":4},
            {"id":"e","label":"e","order":5},
            {"id":"f","label":"f","order":6},
            {"id":"g","label":"g","order":7},
            {"id":"h","label":"h","order":8},
            {"id":"k","label":"k","order":9}

        ])
    );

    let matching_values = valve
        .get_matching_values("table6", "child", Some("7"))
        .await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"7","label":"7","order":1}
        ])
    );

    eprintln!("done.");
    Ok(())
}

async fn test_update_1(valve: &Valve) -> Result<()> {
    eprint!("Running test_update_1() ... ");

    let row = json!({
        "child": "b",
        "parent": "f",
        "xyzzy": "w",
        "foo": 1,
        "bar": "B",
    });

    let row_order_before = valve.get_row_order("table2", &1).await?;
    valve
        .update_row("table2", &1, &row.as_object().unwrap())
        .await?;
    let row_order_after = valve.get_row_order("table2", &1).await?;
    assert_eq!(row_order_before, row_order_after);

    eprintln!("done.");
    Ok(())
}

async fn test_insert_1(valve: &Valve) -> Result<()> {
    eprint!("Running test_insert_1() ... ");

    let row = json!({
        "id": "BFO:0000027",
        "label": "bazaar",
        "parent": "barrie",
        "source": "BFOBBER",
        "type": "owl:Class",
    });

    let (new_row_num, _new_row) = valve
        .insert_row("table3", row.as_object().unwrap(), None, None)
        .await?;
    let row_order = valve.get_row_order("table3", &new_row_num).await?;
    assert_eq!(row_order, new_row_num as f32);

    eprintln!("done.");
    Ok(())
}

async fn test_update_2(valve: &Valve) -> Result<()> {
    eprint!("Running test_update_2() ... ");

    let row = json!({
        "child": 2,
        "parent": 6,
        "xyzzy": 23,
        "foo": 'a',
        "bar": 2,
    });

    let row_order_before = valve.get_row_order("table6", &1).await?;
    valve
        .update_row("table6", &1, row.as_object().unwrap())
        .await?;
    let row_order_after = valve.get_row_order("table6", &1).await?;
    assert_eq!(row_order_before, row_order_after);

    eprintln!("done.");
    Ok(())
}

async fn test_insert_2(valve: &Valve) -> Result<()> {
    eprint!("Running test_insert_2() ... ");

    let row = json!({
        "child": 2,
        "parent": 6,
        "xyzzy": 23,
        "foo": 'a',
        "bar": 2,
    });

    let (new_row_num, _new_row) = valve
        .insert_row("table6", row.as_object().unwrap(), None, None)
        .await?;
    let row_order = valve.get_row_order("table6", &new_row_num).await?;
    assert_eq!(row_order, new_row_num as f32);

    eprintln!("done.");
    Ok(())
}

async fn test_dependencies(valve: &Valve) -> Result<()> {
    eprint!("Running test_dependencies() ... ");

    // Test cases for updates/inserts/deletes with dependencies.
    let row = json!({
        "foreign_column": "w",
        "other_foreign_column": "z",
        "numeric_foreign_column": "",
    });

    valve
        .update_row("table10", &1, &row.as_object().unwrap())
        .await?;

    let row = json!({
        "child": "b",
        "parent": "c",
        "xyzzy": "d",
        "foo": "d",
        "bar": "f",
    });

    valve
        .update_row("table11", &2, &row.as_object().unwrap())
        .await?;

    valve.delete_row("table11", &4).await?;

    let row = json!({
        "foreign_column": "i",
        "other_foreign_column": "i",
        "numeric_foreign_column": "9",
    });

    let (_new_row_num, _new_row) = valve
        .insert_row("table10", &row.as_object().unwrap(), None, None)
        .await?;

    eprintln!("done.");
    Ok(())
}

// TODO: Add Move
#[derive(Clone, Debug, PartialEq, Eq)]
enum DbOperation {
    Insert,
    Delete,
    Update,
    Undo,
    Redo,
}

async fn generate_operation_sequence(pool: &AnyPool) -> Result<Vec<DbOperation>> {
    /*
    Algorithm:
    ----------
    1. Determine the number of "modify" operations to randomly generate. Then for each operation do
       the following:
    2. Generate a modify/undo pair
    3. Do the modify
    4. Either add an undo immediately after the given modify, or defer the undo by adding it to an
       undo stack.
    5. Possibly generate a redo/undo pair such that the undo comes immediately after the undo, or
       is deferred to the undo stack.
    6. Once all of the modify operations have been processed, go through the undo stack:
       a. For each undo, once it's been processed, possibly generate a redo/undo pair such that the
          undo comes immediately after the undo, or is deferred to the undo stack.

    After this function returns, the database should be in the same logical state as it was before.
     */

    let list_len = {
        let between = Uniform::from(25..51);
        let mut rng = thread_rng();
        between.sample(&mut rng)
    };

    let mut operations = vec![];
    let mut undo_stack = vec![];
    for _ in 0..list_len {
        let between = Uniform::from(0..3);
        let mut rng = thread_rng();
        match between.sample(&mut rng) {
            0 => operations.push(DbOperation::Insert),
            1 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Delete)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            2 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Update)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            _ => unreachable!(),
        };
        // Randomly either add an undo immediately after the modify, or add it to the undo_stack:
        if random::<bool>() == true {
            operations.push(DbOperation::Undo);
            // Randomly add a redo as well:
            if random::<bool>() == true {
                operations.push(DbOperation::Redo);
                // Randomly either add an undo either immediately after the redo, or to the
                // undo_stack:
                if random::<bool>() == true {
                    operations.push(DbOperation::Undo);
                } else {
                    undo_stack.push(DbOperation::Undo);
                }
            }
        } else {
            undo_stack.push(DbOperation::Undo);
        }
    }

    // Go through the items in the undo stack:
    let mut further_operations = vec![];
    let mut further_undo_stack = vec![];
    while let Some(_) = undo_stack.pop() {
        // Add the undo to the list of further operations to perform:
        further_operations.push(DbOperation::Undo);
        // Randomly add a redo as well:
        if random::<bool>() == true {
            further_operations.push(DbOperation::Redo);
            // Randomly add an undo either immediately after the redo, or to a further
            // stack of undos to be performed at the end:
            if random::<bool>() == true {
                further_operations.push(DbOperation::Undo);
            } else {
                further_undo_stack.push(DbOperation::Undo);
            }
        }
    }

    operations.append(&mut further_operations);
    // Since further_undo_stack is a stack, we need to reverse it:
    further_undo_stack.reverse();
    operations.append(&mut further_undo_stack);
    Ok(operations)
}

async fn test_randomized_api_test_with_undo_redo(valve: &Valve) -> Result<()> {
    // Randomly generate a number of insert/update/delete operations, possibly followed by undos
    // and/or redos.
    eprint!("Running test_randomized_api_test_with_undo_redo() ... ");

    fn generate_value() -> String {
        let mut value = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        while random::<bool>() && random::<bool>() {
            value.push_str(" ");
            value.push_str(&Alphanumeric.sample_string(&mut rand::thread_rng(), 10));
        }
        if random::<bool>() && random::<bool>() {
            value.push_str(" ");
        }
        value
    }

    fn generate_row() -> SerdeMap {
        let mut row = SerdeMap::new();
        row.insert("prefix".to_string(), json!(generate_value()));
        row.insert("base".to_string(), json!(generate_value()));
        row.insert("ontology IRI".to_string(), json!(generate_value()));
        row.insert("version IRI".to_string(), json!(generate_value()));
        row
    }

    let operations_list = generate_operation_sequence(&valve.pool).await?;
    for operation in operations_list {
        match operation {
            DbOperation::Delete => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(&valve.pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(ValveError::DataError("No rows in table1_view".into()).into());
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    valve.delete_row("table1", &row_number).await?;
                }
            }
            DbOperation::Update => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(&valve.pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(ValveError::DataError("No rows in table1_view".into()).into());
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    let row = generate_row();
                    valve.update_row("table1", &row_number, &row).await?;
                }
            }
            DbOperation::Insert => {
                let row = generate_row();
                let (_rn, _r) = valve.insert_row("table1", &row, None, None).await?;
            }
            DbOperation::Undo => {
                valve.undo().await?;
            }
            DbOperation::Redo => {
                valve.redo().await?;
            }
        };
    }

    eprintln!("done.");
    Ok(())
}

async fn test_undo_redo(valve: &Valve) -> Result<()> {
    eprint!("Running test_undo_redo() ... ");

    // Undo/redo tests
    let row_1 = json!({
        "foreign_column": "j",
        "other_foreign_column": "j",
        "numeric_foreign_column": "10",
    });
    let row_2 = json!({
        "foreign_column": "k",
        "other_foreign_column": "k",
        "numeric_foreign_column": "11",
    });

    // Undo/redo test 1:
    let (_rn, _r) = valve
        .insert_row("table10", &row_1.as_object().unwrap(), None, None)
        .await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 2:
    valve
        .update_row("table10", &8, &row_2.as_object().unwrap())
        .await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 3:
    valve.delete_row("table10", &8).await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.undo().await?;

    // Undo/redo test 4:
    let (rn, _row) = valve
        .insert_row("table10", &row_1.as_object().unwrap(), None, None)
        .await?;

    valve
        .update_row("table10", &rn, &row_2.as_object().unwrap())
        .await?;

    // Undo update:
    valve.undo().await?;

    // Redo update:
    valve.redo().await?;

    valve.delete_row("table10", &rn).await?;

    // Undo delete:
    valve.undo().await?;

    // Undo update:
    valve.undo().await?;

    // Undo insert:
    valve.undo().await?;

    // Undo/redo test 5:
    valve.move_row("table10", &1, &5).await?;

    valve.undo().await?;

    valve.redo().await?;

    valve.move_row("table10", &3, &2).await?;

    valve.undo().await?;

    eprintln!("done.");
    Ok(())
}

async fn test_modes(valve: &Valve) -> Result<()> {
    eprint!("Running test_modes() ... ");
    let readonly_row = json!({
        "study_name": "FAKE123",
        "sample_number": 100,
        "species": "Adelie Penguin (Pygoscelis adeliae)",
        "region": "Anvers",
        "island": "Briscoe",
        "stage": "Adult, 1 Egg Stage",
        "individual_id": "ZH64",
        "clutch_completion": "Yes",
        "date_egg": "2024-01-01",
        "culmen_length": 38.7,
        "culmen_depth": 20.4,
        "flipper_length": 215,
        "body_mass": 2401,
        "sex": "FEMALE",
        "delta_15_n": 8.233,
        "delta_13_c": -23.2,
        "comments": "",
    });
    let readonly_row = readonly_row.as_object().unwrap();
    let expected_vrow = indoc! {r#"ValveRow {
                                       row_number: None,
                                       contents: {
                                           "body_mass": ValveCell {
                                               nulltype: None,
                                               value: Number(2401),
                                               valid: true,
                                               messages: [],
                                           },
                                           "clutch_completion": ValveCell {
                                               nulltype: None,
                                               value: String("Yes"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "comments": ValveCell {
                                               nulltype: Some(
                                                   "empty",
                                               ),
                                               value: String(""),
                                               valid: true,
                                               messages: [],
                                           },
                                           "culmen_depth": ValveCell {
                                               nulltype: None,
                                               value: Number(20.4),
                                               valid: true,
                                               messages: [],
                                           },
                                           "culmen_length": ValveCell {
                                               nulltype: None,
                                               value: Number(38.7),
                                               valid: true,
                                               messages: [],
                                           },
                                           "date_egg": ValveCell {
                                               nulltype: None,
                                               value: String("2024-01-01"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "delta_13_c": ValveCell {
                                               nulltype: None,
                                               value: Number(-23.2),
                                               valid: true,
                                               messages: [],
                                           },
                                           "delta_15_n": ValveCell {
                                               nulltype: None,
                                               value: Number(8.233),
                                               valid: true,
                                               messages: [],
                                           },
                                           "flipper_length": ValveCell {
                                               nulltype: None,
                                               value: Number(215),
                                               valid: true,
                                               messages: [],
                                           },
                                           "individual_id": ValveCell {
                                               nulltype: None,
                                               value: String("ZH64"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "island": ValveCell {
                                               nulltype: None,
                                               value: String("Briscoe"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "region": ValveCell {
                                               nulltype: None,
                                               value: String("Anvers"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "sample_number": ValveCell {
                                               nulltype: None,
                                               value: Number(100),
                                               valid: true,
                                               messages: [],
                                           },
                                           "sex": ValveCell {
                                               nulltype: None,
                                               value: String("FEMALE"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "species": ValveCell {
                                               nulltype: None,
                                               value: String("Adelie Penguin (Pygoscelis adeliae)"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "stage": ValveCell {
                                               nulltype: None,
                                               value: String("Adult, 1 Egg Stage"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "study_name": ValveCell {
                                               nulltype: None,
                                               value: String("FAKE123"),
                                               valid: true,
                                               messages: [],
                                           },
                                       },
                                   }"#};
    let vrow = valve.validate_row("readonly1", &readonly_row, None).await?;
    assert_eq!(format!("{:#?}", vrow), expected_vrow);

    let view_row = json!({
        "foo": "a",
        "bar": "b",
    });
    let view_row = view_row.as_object().unwrap();
    let expected_vrow = indoc! {r#"ValveRow {
                                       row_number: None,
                                       contents: {
                                           "bar": ValveCell {
                                               nulltype: None,
                                               value: String("b"),
                                               valid: true,
                                               messages: [],
                                           },
                                           "foo": ValveCell {
                                               nulltype: None,
                                               value: String("a"),
                                               valid: true,
                                               messages: [],
                                           },
                                       },
                                   }"#};
    let vrow = valve.validate_row("view1", &view_row, None).await?;
    assert_eq!(format!("{:#?}", vrow), expected_vrow);

    let result = valve
        .insert_row("readonly1", &readonly_row, None, None)
        .await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Inserting to table 'readonly1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.insert_row("view1", &view_row, None, None).await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Inserting to table 'view1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.update_row("readonly1", &1, &readonly_row).await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Updating table 'readonly1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.update_row("view1", &1, &view_row).await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Updating table 'view1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.delete_row("readonly1", &1).await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Deleting from table 'readonly1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    let result = valve.delete_row("view1", &1).await;
    match result {
        Err(e) => assert_eq!(
            format!("{:?}", e),
            r#"InputError("Deleting from table 'view1' is not allowed")"#,
        ),
        _ => assert!(false, "Expected an error result but got an OK result"),
    };

    eprintln!("done.");
    Ok(())
}

async fn test_default(valve: &Valve) -> Result<()> {
    eprint!("Running test_default() ... ");

    // Default value of text type:
    let row = json!({
        "prefix": "g",
        "ontology_IRI": "foo",
        "version_IRI": "bar",
    });
    let (_, new_row) = valve
        .insert_row("table8", row.as_object().unwrap(), None, None)
        .await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      3,
                                  ),
                                  contents: {
                                      "ontology_IRI": ValveCell {
                                          nulltype: None,
                                          value: String("foo"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "prefix": ValveCell {
                                          nulltype: None,
                                          value: String("g"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "version_IRI": ValveCell {
                                          nulltype: None,
                                          value: String("bar"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table8"
            WHERE "prefix" = 'g'
              AND "ontology_IRI" = 'foo'
              AND "version_IRI" = 'bar'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let base = row.get::<&str, &str>("base");
    assert_eq!("b", base);

    // Default value of numeric type:
    let row = json!({
        "child": "f",
        "parent": "d",
        "xyzzy": "x",
        "bar": "w",
    });
    let (_, new_row) = valve
        .insert_row("table9", row.as_object().unwrap(), None, None)
        .await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      10,
                                  ),
                                  contents: {
                                      "bar": ValveCell {
                                          nulltype: None,
                                          value: String("w"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "child": ValveCell {
                                          nulltype: None,
                                          value: String("f"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "parent": ValveCell {
                                          nulltype: None,
                                          value: String("d"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "xyzzy": ValveCell {
                                          nulltype: None,
                                          value: String("x"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table9"
            WHERE "bar" = 'w'
              AND "child" = 'f'
              AND "parent" = 'd'
              AND "xyzzy" = 'x'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let foo: i32 = row.get("foo");
    assert_eq!(1, foo);

    // Override default value:
    let row = json!({
        "child": "g",
        "parent": "e",
        "xyzzy": "y",
        "bar": "x",
        "foo": 2,
    });
    let (_, new_row) = valve
        .insert_row("table9", row.as_object().unwrap(), None, None)
        .await?;
    let expected = indoc! {r#"ValveRow {
                                  row_number: Some(
                                      11,
                                  ),
                                  contents: {
                                      "bar": ValveCell {
                                          nulltype: None,
                                          value: String("x"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "child": ValveCell {
                                          nulltype: None,
                                          value: String("g"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "foo": ValveCell {
                                          nulltype: None,
                                          value: Number(2),
                                          valid: true,
                                          messages: [],
                                      },
                                      "parent": ValveCell {
                                          nulltype: None,
                                          value: String("e"),
                                          valid: true,
                                          messages: [],
                                      },
                                      "xyzzy": ValveCell {
                                          nulltype: None,
                                          value: String("y"),
                                          valid: true,
                                          messages: [],
                                      },
                                  },
                              }"#};
    assert_eq!(format!("{:#?}", new_row), expected);
    let query = sqlx_query(
        r#"SELECT * FROM "table9"
            WHERE "bar" = 'x'
              AND "child" = 'g'
              AND "parent" = 'e'
              AND "xyzzy" = 'y'"#,
    );
    let rows = query.fetch_all(&valve.pool).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let foo: i32 = row.get("foo");
    assert_eq!(2, foo);
    eprintln!("done.");
    Ok(())
}

async fn test_move(valve: &Valve) -> Result<()> {
    eprint!("Running test_move() ... ");

    async fn get_rows_in_order(valve: &Valve) -> Result<Vec<u32>> {
        let query = sqlx_query("SELECT row_number FROM table4_view ORDER BY row_order");
        let rows = query.fetch_all(&valve.pool).await?;
        let rows: Vec<i64> = rows.iter().map(|r| r.get("row_number")).collect::<Vec<_>>();
        let rows = rows.iter().map(|n| *n as u32).collect::<Vec<_>>();
        Ok(rows)
    }

    valve.move_row("table4", &5, &2).await?;
    assert_eq!(
        vec![1, 2, 5, 3, 4, 6, 7, 8, 9, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table4", &7, &5).await?;
    assert_eq!(
        vec![1, 2, 5, 7, 3, 4, 6, 8, 9, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table4", &9, &5).await?;
    assert_eq!(
        vec![1, 2, 5, 9, 7, 3, 4, 6, 8, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table4", &5, &11).await?;
    assert_eq!(
        vec![1, 2, 9, 7, 3, 4, 6, 8, 10, 11, 5],
        get_rows_in_order(valve).await?
    );
    valve.move_row("table4", &2, &0).await?;
    assert_eq!(
        vec![2, 1, 9, 7, 3, 4, 6, 8, 10, 11, 5],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 9, 7, 3, 4, 6, 8, 10, 11, 5],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 9, 7, 3, 4, 6, 8, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 7, 3, 4, 6, 8, 9, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 5, 3, 4, 6, 7, 8, 9, 10, 11],
        get_rows_in_order(valve).await?
    );
    valve.undo().await?;
    assert_eq!(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        get_rows_in_order(valve).await?
    );

    // TODO: Once the recording of moves to the history table has been implemented, verify that
    // it has been done correctly here.

    eprintln!("done.");
    Ok(())
}

// TODO: Remove this from the test suite. We don't need to run it all the time.
async fn test_circular_move(valve: &Valve) -> Result<()> {
    for row in 2..11 {
        //println!("Moving row {}", row);
        let prev_row = row - 1;
        valve.move_row("penguin", &row, &prev_row).await?;
    }
    Ok(())
}

pub async fn run_api_tests(table: &str, database: &str) -> Result<()> {
    let valve = Valve::build(table, database).await?;

    // NOTE that you must use an external script to fetch the data from the database and run a diff
    // against a known good sample to verify that these tests yield the expected results:
    //test_matching(&valve).await?;
    //test_update_1(&valve).await?;
    //test_insert_1(&valve).await?;
    //test_update_2(&valve).await?;
    //test_insert_2(&valve).await?;
    //test_dependencies(&valve).await?;
    //test_undo_redo(&valve).await?;
    //test_randomized_api_test_with_undo_redo(&valve).await?;
    //test_modes(&valve).await?;
    //test_default(&valve).await?;
    test_move(&valve).await?;
    //test_circular_move(&valve).await?;

    // When the first argument to Valve::build() is not a string ending in .tsv, the table table
    // should be read from the database string (given by the second argument) instead, i.e., valve
    // will look in the given database and read the configuration from the "table" db table. Here
    // we just make sure that this is possible. If there is a problem an error will be returned.
    let _valve = Valve::build("ignored", database).await?;

    eprintln!("done.");
    Ok(())
}

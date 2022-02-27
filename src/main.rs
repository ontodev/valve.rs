use std::env;
use std::process;
use serde_json::{
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    Map as SerdeMap,
    Value as SerdeValue,
};

fn read_config_files(table: &String) -> SerdeMap<String, SerdeValue> {
    let mut config = SerdeMap::new();
    for key in &vec!["table", "datatype", "special", "rule", "constraints"] {
        config.insert(key.to_string(), SerdeValue::Object({
            let mut m = SerdeMap::new();
            m
        }));
    }

    let mut special_table_types = SerdeMap::new();
    for key in vec!["table", "column", "datatype", "rule"] {
        special_table_types.insert(key.to_string(), SerdeValue::Object({
            let mut m = SerdeMap::new();
            m.insert(String::from("required"), {
                if key == "rule" {
                    SerdeValue::Bool(false)
                } else {
                    SerdeValue::Bool(true)
                }
            });
            m
        }));
    }
    let special_table_types = special_table_types;

    if let Some(SerdeValue::Object(m)) = config.get_mut("special") {
        for t in special_table_types.keys() {
            m.insert(t.to_string(), SerdeValue::Null);
        }
    }

    println!("Config: {:#?}", config);
    config
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("Usage: cmi-pb-terminology-rs table db_dir");
        process::exit(1);
    }
    let table = &args[1];
    let db_dir = &args[2];
    println!("Arguments passed: table: {}, db_dir: {}", table, db_dir);

    let config = read_config_files(table);
}

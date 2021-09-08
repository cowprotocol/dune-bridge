extern crate serde_derive;
use crate::models::in_memory_database::DatabaseStruct;
use anyhow::Result;
use primitive_types::H160;
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::models::dune_json_formats::{Data, DuneJson};

pub fn load_dune_data_into_memory<P: AsRef<Path>>(path: P) -> Result<DatabaseStruct> {
    let dune_json = read_dune_data_from_file(path)?;
    load_data_from_json_into_memory(dune_json)
}

fn read_dune_data_from_file<P: AsRef<Path>>(path: P) -> Result<DuneJson> {
    // Open the file in read-only mode with buffer.
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let u = serde_json::from_reader(reader)?;
    Ok(u)
}

fn load_data_from_json_into_memory(dune_download: DuneJson) -> Result<DatabaseStruct> {
    let mut memory_database: HashMap<H160, Vec<Data>> = HashMap::new();
    for user_data in dune_download.user_data {
        let address: H160 = user_data.data.owner;
        let vector_to_insert;
        if let Some(data_vector) = memory_database.get_mut(&address) {
            data_vector.push(user_data.data);
            vector_to_insert = data_vector.to_vec();
        } else {
            vector_to_insert = vec![user_data.data];
        }
        memory_database.insert(address, vector_to_insert);
    }
    let date = dune_download.time_of_download;
    Ok(DatabaseStruct {
        user_data: memory_database,
        updated: date,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::prelude::*;
    use primitive_types::H160;
    use serde_json::json;

    #[test]
    fn test_loading_into_in_memory_object() {
        let value = json!(
            {
                "user_data": [
                    {
                        "data": {
                            "cowswap_usd_volume": 474.26231998787733,
                            "month": "2021-05",
                            "number_of_trades": 3,
                            "owner": "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737",
                            "usd_volume_all_exchanges": null
                        },
                        "__typename": "get_result_template"
                    },
                ],
                "time_of_download": 1630333791
        });
        let memory_database =
            load_data_from_json_into_memory(serde_json::from_value(value).unwrap()).unwrap();
        let test_address_1: H160 = "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737"
            .parse()
            .unwrap();
        assert_eq!(
            memory_database.updated,
            Utc.ymd(2021, 8, 30).and_hms(14, 29, 51)
        );
        assert_eq!(
            memory_database
                .user_data
                .get(&test_address_1)
                .unwrap()
                .get(0)
                .unwrap()
                .number_of_trades,
            Some(3u64)
        );
    }
}

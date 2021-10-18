extern crate serde_derive;
use crate::models::in_memory_database::DatabaseStruct;
use anyhow::Result;
use chrono::prelude::*;
use primitive_types::H160;
use serde_json;
use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io::BufReader;
use std::path::Path;

use crate::models::dune_json_formats::{Data, DuneJson};

pub fn load_dune_data_into_memory<P: AsRef<Path>>(path: P) -> Result<DatabaseStruct> {
    let mut memory_database: HashMap<H160, Vec<Data>> = HashMap::new();
    let mut date = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);
    for entry in read_dir(path)? {
        let entry = entry?;
        let dune_json = read_dune_data_from_file(entry.path())?;
        let date_of_file = load_data_from_json_into_memory(&mut memory_database, dune_json);
        if date < date_of_file {
            date = date_of_file;
        }
    }
    Ok(DatabaseStruct {
        user_data: memory_database,
        updated: date,
    })
}

fn read_dune_data_from_file<P: AsRef<Path>>(path: P) -> Result<DuneJson> {
    // Open the file in read-only mode with buffer.
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let u = serde_json::from_reader(reader)?;
    Ok(u)
}

pub fn load_data_from_json_into_memory(
    memory_database: &mut HashMap<H160, Vec<Data>>,
    dune_download: DuneJson,
) -> DateTime<Utc> {
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
    dune_download.time_of_download
}

#[cfg(test)]
mod tests {
    use super::*;
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
                            "day": "2021-05-05",
                            "number_of_trades": 3,
                            "owner": "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737",
                            "usd_volume_all_exchanges": null,
                            "referrals": [
                                "0x94e61b6b34f2bb82d59a57dba08243d33a083c7e",
                                "0x9dcfad0b490378826774cb402e4959fc39c0a9a4"
                            ],
                            "total_referred_volume": 247.135594313237,
                        },
                        "__typename": "get_result_template"
                    },
                ],
                "time_of_download": 1630333791
        });
        let mut memory_database = HashMap::new();
        let date = load_data_from_json_into_memory(
            &mut memory_database,
            serde_json::from_value(value).unwrap(),
        );
        let test_address_1: H160 = "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737"
            .parse()
            .unwrap();
        assert_eq!(date, Utc.ymd(2021, 8, 30).and_hms(14, 29, 51));
        assert_eq!(
            memory_database
                .get(&test_address_1)
                .unwrap()
                .get(0)
                .unwrap()
                .number_of_trades,
            Some(3u64)
        );
    }
}

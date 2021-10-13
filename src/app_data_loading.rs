use anyhow::Result;
use chrono::serde::ts_seconds;
use chrono::DateTime;
use chrono::Utc;
use rustc_hex::FromHexError;
use serde_json;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use primitive_types::H256;
use serde::{Deserialize, Serialize};
use substring::Substring;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DuneAppDataDownload {
    pub app_data: Vec<UserData>,
    #[serde(with = "ts_seconds")]
    pub time_of_download: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserData {
    pub data: Data,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Data {
    pub appdata: String,
}

fn read_dune_data_from_file<P: AsRef<Path>>(path: P) -> Result<DuneAppDataDownload> {
    // Open the file in read-only mode with buffer.
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let u = serde_json::from_reader(reader)?;
    Ok(u)
}
pub fn load_app_data_from_json(dune_download: DuneAppDataDownload) -> Vec<H256> {
    let (parsed_hashes, errors): (Vec<Result<H256, FromHexError>>, Vec<_>) = dune_download
        .app_data
        .iter()
        .map(|data_point| data_point.data.appdata.substring(3, 67).parse())
        .partition(Result::is_ok);
    for error in errors {
        tracing::error!("Error while parsing the app_data download: {:?}", error);
    }
    parsed_hashes.iter().filter_map(|hash| hash.ok()).collect()
}

pub fn load_distinct_app_data_from_json(dune_data_file: String) -> Result<Vec<H256>> {
    let dune_download = read_dune_data_from_file(dune_data_file)?;
    Ok(load_app_data_from_json(dune_download))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_app_data_loading_from_json() {
        let value = json!(
                {
                    "app_data": [
                        {
                            "data": {
                                "appdata": "\"0xe9f29ae547955463ed535162aefee525d8d309571a2b18bc26086c8c35d781eb\""
                            },
                            "__typename": "get_result_template"
                        },
                        {
                            "data": {
                                "appdata": "\"0xe4d1ab10f2c9ffe7bdd23c315b03f18cff90888d6b2bb5022bacd46ab9cddf24\""
                            },
                            "__typename": "get_result_template"
                        }
                    ],
                    "time_of_download": 1630333791
                }
        );
        let app_data = load_app_data_from_json(serde_json::from_value(value).unwrap());
        let expected_app_data: Vec<H256> = vec![
            "0xe9f29ae547955463ed535162aefee525d8d309571a2b18bc26086c8c35d781eb"
                .parse()
                .unwrap(),
            "0xe4d1ab10f2c9ffe7bdd23c315b03f18cff90888d6b2bb5022bacd46ab9cddf24"
                .parse()
                .unwrap(),
        ];
        assert_eq!(app_data, expected_app_data);
    }
}

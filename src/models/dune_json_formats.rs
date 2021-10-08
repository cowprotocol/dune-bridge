extern crate serde_derive;
use chrono::prelude::*;
use chrono::serde::ts_seconds;
use primitive_types::H160;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
pub struct DuneJson {
    pub user_data: Vec<UserData>,
    #[serde(with = "ts_seconds")]
    pub time_of_download: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
pub struct UserData {
    pub data: Data,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
pub struct Data {
    pub cowswap_usd_volume: Option<f64>,
    pub day: String,
    pub number_of_trades: Option<u64>,
    pub owner: H160,
    pub usd_volume_all_exchanges: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
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
                            "usd_volume_all_exchanges": null
                        },
                        "__typename": "get_result_template"
                    }
                ],
                "time_of_download": 1630333791
        });
        let data = Data {
            cowswap_usd_volume: Some(474.26231998787733f64),
            day: String::from("2021-05-05"),
            number_of_trades: Some(3u64),
            owner: "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737"
                .parse()
                .unwrap(),
            usd_volume_all_exchanges: None,
        };
        let user_data = UserData { data };
        let expected_value = DuneJson {
            user_data: vec![user_data],
            time_of_download: Utc.ymd(2021, 8, 30).and_hms(14, 29, 51),
        };
        let derived_dune_json: DuneJson = serde_json::from_value(value).unwrap();
        assert!(derived_dune_json == expected_value);
    }
}

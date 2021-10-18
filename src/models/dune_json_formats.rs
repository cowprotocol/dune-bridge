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
    pub referrals: Vec<H160>,
    pub total_referred_volume: Option<f64>,
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
                            "day": "2021-10-18T00:00:00+00:00",
                            "number_of_trades": 3,
                            "owner": "0x36416d81e590ff67370e4523b9cd3257aa0a853c",
                            "referrals": [
                                "0x94e61b6b34f2bb82d59a57dba08243d33a083c7e",
                                "0x9dcfad0b490378826774cb402e4959fc39c0a9a4"
                            ],
                            "total_referred_volume": 247.135594313237,
                        },
                        "__typename": "get_result_template"
                    }
                ],
                "time_of_download": 1630333791
        });
        let data = Data {
            cowswap_usd_volume: Some(474.26231998787733f64),
            day: String::from("2021-10-18T00:00:00+00:00"),
            number_of_trades: Some(3u64),
            owner: "0x36416d81e590ff67370e4523b9cd3257aa0a853c"
                .parse()
                .unwrap(),
            referrals: vec![
                "0x94e61b6b34f2bb82d59a57dba08243d33a083c7e"
                    .parse()
                    .unwrap(),
                "0x9dcfad0b490378826774cb402e4959fc39c0a9a4"
                    .parse()
                    .unwrap(),
            ],
            total_referred_volume: Some(247.135594313237f64),
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

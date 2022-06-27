//! Contains the app_data file structures, as they are stored in ipfs
//!
use crate::models::u256_decimal;
use primitive_types::{H160, U256};
use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Referrer {
    pub address: H160,
    pub version: String,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct Quote {
    #[serde(with = "u256_decimal")]
    pub sell_amount: U256,
    #[serde(with = "u256_decimal")]
    pub buy_amount: U256,
    pub version: String,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Metadata {
    // we make all of the field optional, in order to be compatible with all versions
    pub environment: Option<String>,
    pub referrer: Option<Referrer>,
    pub quote: Option<Quote>,
}
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub version: String,
    pub app_code: String,
    pub environment: Option<String>,
    pub metadata: Option<Metadata>,
}

impl AppData {
    pub fn read_referrer(&self) -> Option<H160> {
        self.metadata
            .as_ref()
            .and_then(|metadata| Some(metadata.referrer.as_ref()?.address))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_loading_json_v1_and_reading_referral() {
        let value = json!({
                "version":"1.2.3",
                "appCode":"MooSwap",
                "metadata":{
                    "environment": "production",
                    "referrer":{
                        "kind":"referrer",
                        "address":"0x8c35B7eE520277D14af5F6098835A584C337311b",
                        "version":"6.6.6"
                }
            }
        });
        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: "1.2.3".to_string(),
            app_code: "MooSwap".to_string(),
            environment: None,
            metadata: Some(Metadata {
                environment: Some("production".to_string()),
                referrer: Some(Referrer {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                    version: "6.6.6".to_string(),
                }),
                quote: None,
            }),
        };

        assert_eq!(json, expected);
    }
    #[test]
    fn test_loading_json_v3_and_reading_referral() {
        let value = json!({
                "version":"1.2.3",
                "appCode":"MooSwap",
                "environment": "production",
                "metadata":{
                    "environment": "production",
                    "referrer":{
                        "kind":"referrer",
                        "address":"0x8c35B7eE520277D14af5F6098835A584C337311b",
                        "version":"6.6.6"
                },
                "quote": {
                    "version": "1.0",
                    "sellAmount": "23426235345",
                    "buyAmount": "2341253523453",
                }
            }
        });
        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: "1.2.3".to_string(),
            app_code: "MooSwap".to_string(),
            environment: Some("production".to_string()),
            metadata: Some(Metadata {
                environment: Some("production".to_string()),
                referrer: Some(Referrer {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                    version: "6.6.6".to_string(),
                }),
                quote: Some(Quote {
                    version: String::from("1.0"),
                    sell_amount: U256::from_dec_str("23426235345").unwrap(),
                    buy_amount: U256::from_dec_str("2341253523453").unwrap(),
                }),
            }),
        };

        assert_eq!(json, expected);
    }
}

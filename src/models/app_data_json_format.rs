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

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "version")]
pub enum Quote {
    #[serde(rename = "0.1.0")]
    V1(QuoteV1),
    #[serde(rename = "0.2.0")]
    V2(QuoteV2),
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct QuoteV1 {
    #[serde(with = "u256_decimal")]
    pub sell_amount: U256,
    #[serde(with = "u256_decimal")]
    pub buy_amount: U256,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct QuoteV2 {
    // This value does not need a large uint type
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub slippage_bips: u32,
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
                    "version": "0.1.0",
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
                quote: Some(Quote::V1(QuoteV1 {
                    sell_amount: U256::from_dec_str("23426235345").unwrap(),
                    buy_amount: U256::from_dec_str("2341253523453").unwrap(),
                })),
            }),
        };

        assert_eq!(json, expected);
    }
    #[test]
    fn test_loading_json_v4() {
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
                    "version": "0.2.0",
                    "slippageBips": "5"
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
                quote: Some(Quote::V2(QuoteV2 {
                    slippage_bips: 5,
                })),
            }),
        };

        assert_eq!(json, expected);
    }

    #[test]
    fn test_loading_quote() {
        let v1_value = json!({
            "version": "0.1.0",
            "sellAmount": "123",
            "buyAmount": "4567",
        });
        let v2_value = json!({
            "version": "0.2.0",
            "slippageBips": "100"
        });
        let json_1: Quote = serde_json::from_value(v1_value).unwrap();
        let json_2: Quote = serde_json::from_value(v2_value).unwrap();
        let expected_1 = Quote::V1(QuoteV1 {
            sell_amount: U256::from_dec_str("123").unwrap(),
            buy_amount: U256::from_dec_str("4567").unwrap(),
        });

        let expected_2 = Quote::V2(QuoteV2 {
            slippage_bips: 100,
        });

        assert_eq!(json_1, expected_1);
        assert_eq!(json_2, expected_2);
    }
}

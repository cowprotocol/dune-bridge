//! Contains the app_data file structures, as they are stored in ipfs
//!
use crate::models::u256_decimal;
use primitive_types::{H160, U256};
use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "version")]
pub enum Referrer {
    #[serde(rename = "0.1.0")]
    V1(ReferrerV1),
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerV1 {
    pub address: H160,
}

impl Referrer {
    pub fn address(&self) -> H160 {
        match self {
            Self::V1(value) => value.address,
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "version")]
pub enum Quote {
    #[serde(rename = "0.1.0")]
    V1(QuoteV1),
    #[serde(rename = "0.2.0")]
    V2(QuoteV2),
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteV1 {
    #[serde(with = "u256_decimal")]
    pub sell_amount: U256,
    #[serde(with = "u256_decimal")]
    pub buy_amount: U256,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteV2 {
    // This value does not need a large uint type
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub slippage_bips: u32,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "version")]
pub enum Class {
    #[serde(rename = "0.1.0")]
    V1(ClassV1),
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderClass {
    Market,
    Limit,
    Liquidity,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct ClassV1 {
    #[serde(rename = "type")]
    pub value: OrderClass,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
pub struct Metadata {
    // we make all of the field optional, in order to be compatible with all versions
    pub referrer: Option<Referrer>,
    pub quote: Option<Quote>,
    pub class: Option<Class>,
}

#[derive(Eq, PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Version {
    #[serde(rename = "0.1.0")]
    V1,
    #[serde(rename = "0.2.0")]
    V2,
    #[serde(rename = "0.3.0")]
    V3,
    #[serde(rename = "0.4.0")]
    V4,
    #[default]
    #[serde(rename = "0.5.0")]
    V5,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub version: Version,
    pub app_code: String,
    pub environment: Option<String>,
    pub metadata: Option<Metadata>,
}

impl AppData {
    pub fn read_referrer(&self) -> Option<H160> {
        Some(self.metadata.as_ref()?.referrer.as_ref()?.address())
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
            "version": "0.1.0",
            "appCode": "MooSwap",
            "metadata": {
                "referrer": {
                    "version":"0.1.0",
                    "address":"0x8c35B7eE520277D14af5F6098835A584C337311b",
                },
            },
        });

        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: Version::V1,
            app_code: "MooSwap".to_string(),
            metadata: Some(Metadata {
                referrer: Some(Referrer::V1(ReferrerV1 {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(json, expected);
    }
    #[test]
    fn test_loading_json_v3_and_reading_referral() {
        let value = json!({
            "version": "0.3.0",
            "appCode": "MooSwap",
            "environment": "production",
            "metadata":{
                "referrer": {
                    "version":"0.1.0",
                    "address":"0x8c35B7eE520277D14af5F6098835A584C337311b",
                },
                "quote": {
                    "version": "0.1.0",
                    "sellAmount": "23426235345",
                    "buyAmount": "2341253523453",
                },
            },
        });

        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: Version::V3,
            app_code: "MooSwap".to_string(),
            environment: Some("production".to_string()),
            metadata: Some(Metadata {
                referrer: Some(Referrer::V1(ReferrerV1 {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                })),
                quote: Some(Quote::V1(QuoteV1 {
                    sell_amount: U256::from_dec_str("23426235345").unwrap(),
                    buy_amount: U256::from_dec_str("2341253523453").unwrap(),
                })),
                ..Default::default()
            }),
        };

        assert_eq!(json, expected);
    }

    #[test]
    fn test_loading_json_v4() {
        let value = json!({
            "version": "0.4.0",
            "appCode": "MooSwap",
            "environment": "production",
            "metadata":{
                "referrer":{
                    "version": "0.1.0",
                    "address": "0x8c35B7eE520277D14af5F6098835A584C337311b",
                },
                "quote": {
                    "version": "0.2.0",
                    "slippageBips": "5"
                }
            }
        });
        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: Version::V4,
            app_code: "MooSwap".to_string(),
            environment: Some("production".to_string()),
            metadata: Some(Metadata {
                referrer: Some(Referrer::V1(ReferrerV1 {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                })),
                quote: Some(Quote::V2(QuoteV2 { slippage_bips: 5 })),
                ..Default::default()
            }),
        };

        assert_eq!(json, expected);
    }

    #[test]
    fn test_loading_json_v5() {
        let value = json!({
            "version": "0.5.0",
            "appCode": "MooSwap",
            "environment": "production",
            "metadata": {
                "environment": "production",
                "referrer":{
                    "version": "0.1.0",
                    "address": "0x8c35B7eE520277D14af5F6098835A584C337311b",
                },
                "quote": {
                    "version": "0.2.0",
                    "slippageBips": "5",
                },
                "class": {
                    "version": "0.1.0",
                    "type": "limit",
                },
            },
        });

        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: Version::V5,
            app_code: "MooSwap".to_string(),
            environment: Some("production".to_string()),
            metadata: Some(Metadata {
                referrer: Some(Referrer::V1(ReferrerV1 {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                })),
                quote: Some(Quote::V2(QuoteV2 { slippage_bips: 5 })),
                class: Some(Class::V1(ClassV1 {
                    value: OrderClass::Limit,
                })),
            }),
        };

        assert_eq!(json, expected);
    }

    #[test]
    fn test_loading_json_with_additional_fields() {
        let value = json!({
            "version": "0.1.0",
            "appCode": "MooSwap",
            "additionalField": "isIngored",
        });

        let json: AppData = serde_json::from_value(value).unwrap();
        let expected = AppData {
            version: Version::V1,
            app_code: "MooSwap".to_string(),
            ..Default::default()
        };

        assert_eq!(json, expected);
    }

    #[test]
    fn test_loading_quote_ok() {
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

        let expected_2 = Quote::V2(QuoteV2 { slippage_bips: 100 });

        assert_eq!(json_1, expected_1);
        assert_eq!(json_2, expected_2);
    }

    #[test]
    fn test_loading_quote_err() {
        // V2 version with V1 data
        assert!(serde_json::from_value::<Quote>(json!({
            "version": "0.2.0",
            "sellAmount": "123",
            "buyAmount": "4567",
        }))
        .is_err());
        // V1 version with V2 data
        assert!(serde_json::from_value::<Quote>(json!({
            "version": "0.1.0",
            "slippageBips": "100"
        }))
        .is_err());
        // Invalid Version Data
        assert!(serde_json::from_value::<Quote>(json!({
            "version": "invalid version",
            "slippageBips": "100"
        }))
        .is_err());
    }
}

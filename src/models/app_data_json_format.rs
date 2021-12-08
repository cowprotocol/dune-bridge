//! Contains the app_data file structures, as they are stored in ipfs
//!
use primitive_types::H160;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Referrer {
    pub address: H160,
    pub version: String,
}

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Metadata {
    pub environment: Option<String>,
    pub referrer: Option<Referrer>,
}

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub version: String,
    pub app_code: String,
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
    fn test_loading_json_and_reading_referral() {
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
            metadata: Some(Metadata {
                environment: Some("production".to_string()),
                referrer: Some(Referrer {
                    address: "0x8c35B7eE520277D14af5F6098835A584C337311b"
                        .parse()
                        .unwrap(),
                    version: "6.6.6".to_string(),
                }),
            }),
        };

        assert_eq!(json, expected);
    }
}

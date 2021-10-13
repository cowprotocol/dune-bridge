//! Contains the app_data file structures, as they are stored in ipfs
//!
use primitive_types::H160;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Referrer {
    pub address: H160,
}

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Metadata {
    pub referrer: Option<Referrer>,
}

#[serde_as]
#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub metadata: Option<Metadata>,
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_loading_json_and_reading_referral() {
        let value = json!(
                {"version":"1.0.0","appCode":"CowSwap","metadata":{"referrer":{"kind":"referrer","address":"0x8c35B7eE520277D14af5F6098835A584C337311b","version":"1.0.0"}}}
        );
        let json: AppData = serde_json::from_value(value).unwrap();
        let expected_referral: H160 = "0x8c35B7eE520277D14af5F6098835A584C337311b"
            .parse()
            .unwrap();
        assert_eq!(
            json.metadata.unwrap().referrer.unwrap().address,
            expected_referral
        );
    }
}

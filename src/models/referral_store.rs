extern crate serde;
extern crate serde_derive;
use crate::models::app_data_json_format::AppData;
use primitive_types::H256;
use serde::ser::{Serialize, SerializeMap, Serializer};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AppDataEntry {
    Data(Option<AppData>),
    TryToFetchXTimes(u64),
}

#[derive(Debug)]
pub struct AppDataStruct {
    pub app_data: HashMap<H256, AppDataEntry>,
}

#[derive(Debug)]
pub struct ReferralStore(pub Mutex<AppDataStruct>);

impl ReferralStore {
    pub fn new(app_data_hashes: Vec<H256>) -> Self {
        let mut hm = HashMap::new();
        for hash in app_data_hashes {
            hm.insert(hash, AppDataEntry::TryToFetchXTimes(3));
        }
        let app_data_struct = AppDataStruct { app_data: hm };
        ReferralStore(Mutex::new(app_data_struct))
    }
}

impl Serialize for AppDataStruct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.app_data.keys().len()))?;
        for (hash, data) in &self.app_data {
            // Skip every entry that is not `TryToFetchXTimes` or None
            if let AppDataEntry::Data(Some(app_data)) = data {
                map.serialize_entry(&hash, &app_data)?
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::app_data_json_format::{Metadata, Referrer};
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_serialization_of_referral() {
        let mut app_data_struct: AppDataStruct = AppDataStruct {
            app_data: HashMap::new(),
        };
        let key: H256 = "0x2947be33ebfa25686ec204857135dd1c676f35d6c252eb066fffaf9b493a01b4"
            .parse()
            .unwrap();
        let entry = AppData {
            version: "1.2.3".to_string(),
            app_code: "MooSwap".to_string(),
            metadata: Some(Metadata {
                environment: Some("production".to_string()),
                referrer: Some(Referrer {
                    // Note that serialization turns everything to lowercase...
                    address: "0x8c35b7ee520277d14af5f6098835a584c337311b"
                        .parse()
                        .unwrap(),
                    version: "6.6.6".to_string(),
                }),
            }),
        };

        app_data_struct
            .app_data
            .insert(key, AppDataEntry::Data(Some(entry)));

        let expected_serialization = json!(
        {
            "0x2947be33ebfa25686ec204857135dd1c676f35d6c252eb066fffaf9b493a01b4":{
                 "version":"1.2.3",
                 "appCode":"MooSwap",
                 "metadata":{
                     "environment": "production",
                     "referrer":{
                         "address":"0x8c35b7ee520277d14af5f6098835a584c337311b",
                         "version":"6.6.6"
                 }
            }
         }
        });
        assert_eq!(
            serde_json::to_value(&app_data_struct).unwrap(),
            expected_serialization
        );
    }
}

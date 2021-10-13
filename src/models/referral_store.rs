extern crate serde;
extern crate serde_derive;
use primitive_types::{H160, H256};
use serde::ser::{Serialize, SerializeMap, Serializer};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Referral {
    Address(Option<H160>),
    TryToFetchXTimes(u64),
}

#[derive(Debug)]
pub struct AppDataStruct {
    pub app_data: HashMap<H256, Referral>,
}

#[derive(Debug)]
pub struct ReferralStore(pub Mutex<AppDataStruct>);

impl ReferralStore {
    pub fn new(app_data_hashes: Vec<H256>) -> Self {
        let mut hm = HashMap::new();
        for hash in app_data_hashes {
            hm.insert(hash, Referral::TryToFetchXTimes(3));
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
        for (hash, address) in &self.app_data {
            let mut bytes = [0u8; 2 + 32 * 2];
            bytes[..2].copy_from_slice(b"0x");
            // Can only fail if the buffer size does not match but we know it is correct.
            hex::encode_to_slice(hash, &mut bytes[2..]).unwrap();
            // Hex encoding is always valid utf8.
            let hash_serialized = std::str::from_utf8(&bytes).unwrap();

            match address {
                Referral::Address(Some(address_bytes)) => {
                    let mut bytes = [0u8; 2 + 20 * 2];
                    bytes[..2].copy_from_slice(b"0x");
                    // Can only fail if the buffer size does not match but we know it is correct.
                    hex::encode_to_slice(address_bytes, &mut bytes[2..]).unwrap();
                    let address_serialized = std::str::from_utf8(&bytes).unwrap();
                    map.serialize_entry(&hash_serialized, &address_serialized.to_string())?
                }
                _ => {}
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_serialization_of_referral() {
        let mut app_data_struct: AppDataStruct = AppDataStruct {
            app_data: HashMap::new(),
        };
        app_data_struct.app_data.insert(
            "0x2947be33ebfa25686ec204857135dd1c676f35d6c252eb066fffaf9b493a01b4"
                .parse()
                .unwrap(),
            Referral::Address(Some(
                "0x8c35b7ee520277d14af5f6098835a584c337311b"
                    .parse()
                    .unwrap(),
            )),
        );
        let expected_serialization = json!(
           { "0x2947be33ebfa25686ec204857135dd1c676f35d6c252eb066fffaf9b493a01b4":"0x8c35b7ee520277d14af5f6098835a584c337311b"});

        assert_eq!(
            serde_json::to_string(&app_data_struct).unwrap(),
            serde_json::to_string(&expected_serialization).unwrap()
        );
    }
}

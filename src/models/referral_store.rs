extern crate serde;
extern crate serde_derive;
use primitive_types::{H160, H256};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct AppDataStruct {
    pub app_data: HashMap<H256, Option<H160>>,
}

#[derive(Debug)]
pub struct ReferralStore(pub Mutex<AppDataStruct>);

impl ReferralStore {
    pub fn new(app_data_hashes: Vec<H256>) -> Self {
        let mut hm = HashMap::new();
        for hash in app_data_hashes {
            hm.insert(hash, None);
        }
        let app_data_struct = AppDataStruct { app_data: hm };
        ReferralStore(Mutex::new(app_data_struct))
    }
}

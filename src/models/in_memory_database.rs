extern crate serde_derive;
use super::dune_json_formats::Data;
use anyhow::Result;
use chrono::prelude::*;
use primitive_types::H160;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Debug)]
pub struct DatabaseStruct {
    pub user_data: HashMap<H160, Vec<Data>>,
    pub updated: DateTime<Utc>,
}

impl Default for DatabaseStruct {
    fn default() -> Self {
        DatabaseStruct {
            user_data: HashMap::new(),
            updated: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
        }
    }
}

#[derive(Debug)]
pub struct InMemoryDatabase(pub Mutex<DatabaseStruct>);

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    total_trades: u64,
    total_referrals: u64,
    trade_volume_usd: f64,
    referral_volume_usd: f64,
    last_updated: Option<DateTime<Utc>>,
}

impl InMemoryDatabase {
    pub fn get_profile_from_raw_data(&self, user: H160) -> Result<Profile> {
        let guard = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match guard.user_data.get(&user) {
            Some(data) => Ok(Profile {
                total_trades: data
                    .iter()
                    .map(|data| data.number_of_trades.unwrap_or(0u64))
                    .sum(),
                total_referrals: {
                    let mut vec_referrals: Vec<H160> = data
                        .iter()
                        .map(|data| data.referrals.clone())
                        .flatten()
                        .collect();
                    let set: HashSet<_> = vec_referrals.drain(..).collect();
                    set.len() as u64
                },
                trade_volume_usd: data
                    .iter()
                    .map(|data| data.cowswap_usd_volume.unwrap_or(0f64))
                    .sum(),
                referral_volume_usd: data
                    .iter()
                    .map(|data| data.total_referred_volume.unwrap_or(0f64))
                    .sum(),
                last_updated: Some(guard.updated),
            }),
            None => Ok(Default::default()),
        }
    }
}

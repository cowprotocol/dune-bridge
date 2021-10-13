use crate::app_data_loading::load_distinct_app_data_from_json;
use crate::models::app_data_json_format::AppData;
use crate::models::referral_store::{Referral, ReferralStore};
use anyhow::{anyhow, Result};
use cid::Cid;
use primitive_types::{H160, H256};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

pub async fn referral_maintainance(memory_database: Arc<ReferralStore>, dune_data_folder: String) {
    loop {
        match maintenaince_tasks(Arc::clone(&memory_database), dune_data_folder.clone()).await {
            Ok(_) => {}
            Err(err) => tracing::debug!("Error during maintenaince_task for referral: {:?}", err),
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

pub async fn maintenaince_tasks(db: Arc<ReferralStore>, dune_data_folder: String) -> Result<()> {
    // 1st step: getting all possible app_data from file and store them in ReferralStore,
    // if not yet existing
    let vec_with_all_app_data = match load_distinct_app_data_from_json(
        dune_data_folder + "app_data/distinct_app_data.json",
    ) {
        Ok(vec) => vec,
        Err(err) => {
            tracing::error!("Could not load distinct app data, due to: {:?}", err);
            return Ok(());
        }
    };
    for app_data in vec_with_all_app_data {
        {
            let mut guard = match db.0.lock() {
                Ok(guard) => guard,
                Err(_) => return Err(anyhow!("Mutex poisoned")),
            };
            guard
                .app_data
                .entry(app_data)
                .or_insert(Referral::TryToFetchXTimes(3));
        }
    }
    // 2st step: get all unintialized referrals
    let uninitialized_app_data_hashes: Vec<H256> = {
        let guard = match db.0.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(anyhow!("Mutex poisoned")),
        };
        guard
            .app_data
            .iter()
            .filter(|(_, referral)| matches!(referral, Referral::TryToFetchXTimes(_)))
            .map(|(hash, _)| hash)
            .copied()
            .collect()
    };
    // 3. try to retrieve all ipfs data for hashes and store them
    for hash in uninitialized_app_data_hashes.iter() {
        download_referral_from_ipfs_and_store_in_referral_store(db.clone(), *hash).await?;
    }
    Ok(())
}

async fn download_referral_from_ipfs_and_store_in_referral_store(
    db: Arc<ReferralStore>,
    hash: H256,
) -> Result<()> {
    match get_cid_from_app_data(hash) {
        Ok(cid) => {
            tracing::debug!("cid for hash {:?} is {:?}", hash, cid);
            match get_ipfs_file_and_read_referrer(cid.clone()).await {
                Ok(referrer) => {
                    tracing::debug!("Adding the referrer {:?} for the hash {:?}", referrer, hash);
                    let mut guard = match db.0.lock() {
                        Ok(guard) => guard,
                        Err(_) => return Err(anyhow!("Mutex poisoned")),
                    };
                    guard.app_data.insert(hash, Referral::Address(referrer));
                }
                Err(err) => {
                    tracing::debug!(
                        "Could not find referrer in cid {:?}, due to the error {:?}",
                        cid,
                        err
                    );
                    let mut guard = match db.0.lock() {
                        Ok(guard) => guard,
                        Err(_) => return Err(anyhow!("Mutex poisoned")),
                    };
                    guard.app_data.entry(hash).and_modify(|referral_entry| {
                        *referral_entry = match referral_entry.clone() {
                            Referral::TryToFetchXTimes(x) if x > 1u64 => {
                                Referral::TryToFetchXTimes(x - 1)
                            }
                            _ => Referral::Address(None),
                        }
                    });
                }
            }
        }
        Err(err) => {
            tracing::debug!("For the app_data hash {:?}, there could not be found a unique referrer due to {:?}", hash, err);
        }
    }
    Ok(())
}

async fn get_ipfs_file_and_read_referrer(cid: String) -> Result<Option<H160>> {
    // Front-end is uploading the ipfs to pinta, hence we could also get the
    // data from "https://gateway.pinata.cloud/ipfs/{:}", though they are rate limiting us.
    let url = format!("https://ipfs.io/ipfs/{:}", cid);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let body = client.get(url.clone()).send().await?.text().await?;
    let json: AppData = serde_json::from_str(&body)?;

    Ok(json
        .metadata
        .and_then(|metadata| Some(metadata.referrer?.address)))
}

fn get_cid_from_app_data(hash: H256) -> Result<String> {
    let cid_prefix = vec![1u8, 112u8, 18u8, 32u8];
    let cid = Cid::try_from([&cid_prefix, hash.as_bytes()].concat())?;
    Ok(cid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitive_types::H160;
    #[test]
    fn test_get_cid_from_app_data() {
        let test_app_data_hash: H256 =
            "3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
                .parse()
                .unwrap();
        assert_eq!(
            get_cid_from_app_data(test_app_data_hash).unwrap(),
            String::from("bafybeib5q5w6r7gxbfutjhes24y65mcif7ugm7hmub2vsk4hqueb2yylti")
        );
    }
    #[test]
    fn test_get_cid_from_app_data_2() {
        let test_app_data_hash: H256 =
            "1FE7C5555B3F9C14FF7C60D90F15F1A5B11A0DA5B1E8AA043582A1B2E1058D0C"
                .parse()
                .unwrap();
        assert_eq!(
            get_cid_from_app_data(test_app_data_hash).unwrap(),
            String::from("bafybeia747cvkwz7tqkp67da3ehrl4nfwena3jnr5cvainmcugzocbmnbq")
        );
    }
    #[tokio::test]
    #[ignore]
    async fn test_fetching_ipfs() {
        let referral = get_ipfs_file_and_read_referrer(String::from(
            "bafybeib5q5w6r7gxbfutjhes24y65mcif7ugm7hmub2vsk4hqueb2yylti",
        ))
        .await
        .unwrap();
        let expected_referral: H160 = "0x424a46612794dbb8000194937834250Dc723fFa5"
            .parse()
            .unwrap();
        assert_eq!(referral, Some(expected_referral));
    }
    #[tokio::test]
    #[ignore]
    async fn test_maintenaince_tasks2() {
        let test_app_data_hash: H256 =
            "3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
                .parse()
                .unwrap();
        let referral_store = ReferralStore::new(vec![test_app_data_hash]);
        let result =
            maintenaince_tasks(Arc::new(referral_store), (&"./data/dune_data/").to_string()).await;
        assert!(result.is_ok());
    }
}

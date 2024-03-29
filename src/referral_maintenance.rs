use crate::app_data_loading::load_distinct_app_data_from_json;
use crate::models::app_data_json_format::AppData;
use crate::models::referral_store::{AppDataEntry, ContentStore};
use anyhow::{anyhow, Result};
use cid::Cid;
use primitive_types::H256;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);

pub async fn referral_maintenance(
    memory_database: Arc<ContentStore>,
    dune_data_folder: String,
    referral_data_folder: String,
    retrys_for_ipfs_file_fetching: u64,
) {
    let mut referrals_fully_synced = false;
    loop {
        match maintenance_tasks(
            Arc::clone(&memory_database),
            dune_data_folder.clone(),
            referral_data_folder.clone(),
            retrys_for_ipfs_file_fetching,
            &mut referrals_fully_synced,
        )
        .await
        {
            Ok(_) => {}
            Err(err) => tracing::debug!("Error during maintenance_task for referral: {:?}", err),
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

pub async fn maintenance_tasks(
    db: Arc<ContentStore>,
    dune_data_folder: String,
    referral_data_folder: String,
    retrys_for_ipfs_file_fetching: u64,
    referrals_fully_synced: &mut bool,
) -> Result<()> {
    // 1st step: getting all possible app_data
    // 1.1: Load app_data from dune download
    tracing::info!(
        "Loading app data from dune download located at {}",
        dune_data_folder.clone() + "app_data/distinct_app_data.json"
    );
    let mut vec_with_all_app_data = match load_distinct_app_data_from_json(
        dune_data_folder + "app_data/distinct_app_data.json",
    ) {
        Ok(vec) => vec,
        Err(err) => {
            tracing::debug!("Could not load distinct app data, due to: {:?}", err);
            return Ok(());
        }
    };
    // 1.2: Load app_data from solvable_orders api
    tracing::info!("Loading app data from solvable_orders api");
    match load_current_app_data_of_solvable_orders().await {
        Ok(vec_with_app_data) => vec_with_all_app_data.extend(vec_with_app_data),
        Err(err) => tracing::warn!("error while downloading the solvable orders: {:}", err),
    };
    // 2nd step: Store app_data in ReferralStore, if not yet existing
    tracing::info!("Store app_data in ReferralStore, if not yet existing");
    for app_data in vec_with_all_app_data {
        {
            let mut guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
            guard
                .app_data
                .entry(app_data)
                .or_insert(AppDataEntry::TryToFetchXTimes(
                    retrys_for_ipfs_file_fetching,
                ));
        }
    }
    // 3rd step: get all uninitialized referrals
    let uninitialized_app_data_hashes: Vec<H256> = {
        let guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
        guard
            .app_data
            .iter()
            .filter(|(_, referral)| matches!(referral, AppDataEntry::TryToFetchXTimes(_)))
            .map(|(hash, _)| hash)
            .copied()
            .collect()
    };
    // 4th step: try to retrieve all ipfs data for hashes and store them
    tracing::info!("Attempting to retrieve ipfs data for uninitialized hashes");
    for hash in uninitialized_app_data_hashes.iter() {
        download_referral_from_ipfs_and_store_in_referral_store(db.clone(), *hash).await?;
    }
    // 5th step: dump hashmap to json if all referrals are synced
    if !*referrals_fully_synced {
        *referrals_fully_synced = check_referral_sync_status(db.clone()).await?;
    }
    if *referrals_fully_synced {
        tracing::info!(
            "Writing referrals to persistent storage at {}",
            referral_data_folder.clone() + "app_data_referral_relationship.json"
        );
        std::fs::create_dir_all(referral_data_folder.clone())?;
        let mut file = File::create(referral_data_folder + "app_data_referral_relationship.json")?;
        {
            let guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
            let file_content = serde_json::to_string(&*guard)?;
            file.write_all(file_content.as_bytes())?;
        }
    }
    Ok(())
}

async fn check_referral_sync_status(db: Arc<ContentStore>) -> Result<bool> {
    let guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
    for entry in guard.app_data.values() {
        match entry {
            AppDataEntry::TryToFetchXTimes(x) if x > &0u64 => return Ok(false),
            _ => {}
        }
    }
    tracing::info!("Referrals fully synced");
    Ok(true)
}

async fn download_referral_from_ipfs_and_store_in_referral_store(
    db: Arc<ContentStore>,
    hash: H256,
) -> Result<()> {
    match get_cid_from_app_data(hash) {
        Ok(cid) => {
            tracing::debug!("cid for hash {:?} is {:?}", hash, cid);
            match get_ipfs_file_and_read_app_data(cid.clone()).await {
                Ok(app_data) => {
                    tracing::debug!(
                        "found content {:?} for cid {:?}, adding to store",
                        app_data,
                        cid
                    );
                    let mut guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
                    guard
                        .app_data
                        .insert(hash, AppDataEntry::Data(Some(app_data)));
                }
                Err(err) => {
                    tracing::debug!("failed to find AppData for cid {:?} due to {:?}", cid, err);
                    let mut guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
                    guard.app_data.entry(hash).and_modify(|referral_entry| {
                        *referral_entry = match referral_entry.clone() {
                            AppDataEntry::TryToFetchXTimes(x) if x > 1u64 => {
                                AppDataEntry::TryToFetchXTimes(x - 1)
                            }
                            _ => AppDataEntry::Data(None),
                        }
                    });
                }
            }
        }
        Err(err) => {
            tracing::debug!(
                "could not recover cid for app_data hash {:?}: due to {:?}",
                hash,
                err
            );
        }
    }
    Ok(())
}

async fn load_current_app_data_of_solvable_orders() -> Result<Vec<H256>> {
    let urls = vec![
        "https://api.cow.fi/mainnet/api/v1/auction".to_string(),
        "https://api.cow.fi/mainnet/api/v1/auction".to_string(),
    ];
    let mut app_data: Vec<H256> = Vec::new();
    for url in urls.iter() {
        let solvable_orders_body = &make_api_request_to_url(url).await;
        let new_app_data = match solvable_orders_body {
            Ok(body) => parse_app_data_from_api_body(body)?,
            Err(err) => {
                tracing::debug!("Could not get solvable orders, due to error: {:}", err);
                Vec::new()
            }
        };
        app_data.extend(new_app_data);
    }
    tracing::debug!("Newly fetched app data is: {:?}", app_data);
    Ok(app_data)
}

fn parse_app_data_from_api_body(body: &str) -> Result<Vec<H256>> {
    #[derive(Deserialize, Serialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct Order {
        app_data: H256,
    }

    #[derive(Deserialize, Serialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct Result {
        orders: Vec<Order>,
    }
    let result: Result = serde_json::from_str(body).map_err(|err| anyhow!("error: {:}", err))?;
    let orders: Vec<Order> = result.orders;
    Ok(orders.iter().map(|order| order.app_data).collect())
}

async fn make_api_request_to_url(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let send_request = client.get(url).send().await?;
    if !send_request.status().is_success() {
        Err(anyhow!(
            "Request: {} with status: {} caused an error: {:?}",
            url,
            send_request.status(),
            send_request.text().await
        ))
    } else {
        send_request
            .text()
            .await
            .map_err(|err| anyhow!("Error: {:}", err))
    }
}

async fn get_ipfs_file_and_read_app_data(cid: String) -> Result<AppData> {
    let url = format!("https://gnosis.mypinata.cloud/ipfs/{:}", cid);
    let body = make_api_request_to_url(&url).await?;
    let app_data: AppData = serde_json::from_str(&body)?;
    Ok(app_data)
}

fn get_cid_from_app_data(hash: H256) -> Result<String> {
    let cid_prefix = vec![1u8, 112u8, 18u8, 32u8];
    let cid = Cid::try_from([&cid_prefix, hash.as_bytes()].concat())?;
    Ok(cid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::app_data_json_format::{Metadata, Referrer, ReferrerV1, Version};
    use serde_json::json;

    #[test]
    fn test_parsing_solvable_orders() {
        let exemplary_api_body = serde_json::to_string(&json!({"orders":[{"creationDate":"2021-10-18T07:35:06.355093Z","owner":"0x95f14b1cbdf15dd537c866c14c0cceeeff7ba29a","uid":"0x9afed8c5f3e8a83404bf8e389734ca6273d51c366d43fe80ff327ae836531cae95f14b1cbdf15dd537c866c14c0cceeeff7ba29a616d2aad","availableBalance":"1159894420","executedBuyAmount":"0","executedSellAmount":"0","executedSellAmountBeforeFees":"0","executedFeeAmount":"0","invalidated":false,"status":"open","settlementContract":"0x9008d19f58aabd9ed0d60971565aa8510560ab41","fullFeeAmount":"28902602","sellToken":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","buyToken":"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2","receiver":"0x95f14b1cbdf15dd537c866c14c0cceeeff7ba29a","sellAmount":"1136771901","buyAmount":"291855261988800830","validTo":1634544301,"appData":"0x487b02c558d729abaf3ecf17881a4181e5bc2446429a0995142297e897b6eb37","feeAmount":"23122519","kind":"sell","partiallyFillable":false,"signingScheme":"eip712","signature":"0x45c19d4a37df315d8fe9f7deeb72a7b490cc273ae0117f55d82af6ce38bbd0796d073569cdbf847a4e17cfcbbe4eec6cd54e2f2018c3b4d86253bd981da902ec1b","sellTokenBalance":"erc20","buyTokenBalance":"erc20"},{"creationDate":"2021-10-18T07:35:38.201070Z","owner":"0xe63a13eedd01b624958acfe32145298788a7a7ba","uid":"0x7e8da7757ab696f5389f92ef20500064825edb649ace597501a4748b79a9d09de63a13eedd01b624958acfe32145298788a7a7ba616d2442","availableBalance":"100825350880578944614107","executedBuyAmount":"0","executedSellAmount":"0","executedSellAmountBeforeFees":"0","executedFeeAmount":"0","invalidated":false,"status":"open","settlementContract":"0x9008d19f58aabd9ed0d60971565aa8510560ab41","fullFeeAmount":"28923369173382934528","sellToken":"0x6b175474e89094c44da98b954eedeac495271d0f","buyToken":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","receiver":"0xe63a13eedd01b624958acfe32145298788a7a7ba","sellAmount":"24953271843779723527073","buyAmount":"24966098317","validTo":1634542658,"appData":"0x00000000000000000000000055662e225a3376759c24331a9aed764f8f0c9fbb","feeAmount":"7834252255287246848","kind":"buy","partiallyFillable":false,"signingScheme":"ethsign","signature":"0xa887ae70e8754d44cdccdac3b4246d0e620f42e372b15bf5ded889f973451f484f61742c5ead9c1318c1593ca75a51526a1a8c253b3ceb9340b645eb7fde78f01c","sellTokenBalance":"erc20","buyTokenBalance":"erc20"}]})).unwrap();
        let expected_app_data = vec![
            "0x487b02c558d729abaf3ecf17881a4181e5bc2446429a0995142297e897b6eb37"
                .parse()
                .unwrap(),
            "0x00000000000000000000000055662e225a3376759c24331a9aed764f8f0c9fbb"
                .parse()
                .unwrap(),
        ];
        assert_eq!(
            parse_app_data_from_api_body(&exemplary_api_body).unwrap(),
            expected_app_data
        );
    }

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
        let referral = get_ipfs_file_and_read_app_data(String::from(
            "bafybeib5q5w6r7gxbfutjhes24y65mcif7ugm7hmub2vsk4hqueb2yylti",
        ))
        .await
        .unwrap();
        let expected = AppData {
            version: Version::V1,
            app_code: "CowSwap".to_string(),
            metadata: Some(Metadata {
                referrer: Some(Referrer::V1(ReferrerV1 {
                    address: "0x424a46612794dbb8000194937834250dc723ffa5"
                        .parse()
                        .unwrap(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(referral, expected);
    }
    #[tokio::test]
    #[ignore]
    async fn test_maintenance_tasks() {
        let test_app_data_hash: H256 =
            "3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
                .parse()
                .unwrap();
        let referral_store = ContentStore::new(vec![test_app_data_hash]);
        let result = maintenance_tasks(
            Arc::new(referral_store),
            (&"./data/dune_data/").to_string(),
            (&"./data/referral_data/").to_string(),
            2u64,
            &mut true,
        )
        .await;
        assert!(result.is_ok());
    }
}

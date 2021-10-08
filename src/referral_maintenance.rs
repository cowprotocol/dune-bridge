use crate::app_data_loading::load_distinct_app_data_from_json;
use crate::models::referral_store::ReferralStore;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(80);

pub async fn maintenaince_tasks(db: Arc<ReferralStore>, dune_data_folder: String) -> Result<()> {
    // 1st step: getting all possible app_data from file and store them in ReferralStore,
    // if not yet existing
    let vec_with_all_app_data = match load_distinct_app_data_from_json(String::from(
        dune_data_folder + "app_data/distinct_app_data.json",
    )) {
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
                Err(poisoned) => poisoned.into_inner(),
            };
            match guard.app_data.get(&app_data) {
                Some(_) => {}
                None => {
                    guard.app_data.insert(app_data, None);
                }
            };
        }
    }
    Ok(())
}

pub async fn referral_maintainance(memory_database: Arc<ReferralStore>, dune_data_folder: String) {
    loop {
        match maintenaince_tasks(Arc::clone(&memory_database), dune_data_folder.clone()).await {
            Ok(_) => {}
            Err(err) => tracing::debug!("Error during maintenaince_task for referral: {:?}", err),
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

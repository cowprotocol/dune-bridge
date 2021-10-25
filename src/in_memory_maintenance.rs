use crate::dune_data_loading::load_dune_data_into_memory;
use crate::models::in_memory_database::InMemoryDatabase;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3);

pub async fn in_memory_database_maintaince(
    memory_database: Arc<InMemoryDatabase>,
    dune_download_folder: String,
) -> Result<()> {
    let db = Arc::clone(&memory_database);
    loop {
        {
            match load_dune_data_into_memory(dune_download_folder.clone() + "user_data/") {
                Ok(new_data_mutex) => {
                    let mut guard = db.0.lock().map_err(|_| anyhow!("Mutex poisoned"))?;
                    *guard = new_data_mutex;
                }
                Err(err) => match format!("{:?}", err).contains("EOF while parsing") {
                    true => {
                        // Sometimes unexpected EOF error messages are thrown, if the reading of rust is faster than the writting of the python scripts. Since this is expected, we don't error.
                        tracing::debug!("Could not read the dune data, due to error: {:?}, most likely this is due to an running writing operation on the file", err)
                    }
                    false => {
                        tracing::error!("Could not read the dune data, due to error: {:?}", err)
                    }
                },
            };
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

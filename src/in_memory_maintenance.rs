use crate::dune_data_loading::load_dune_data_into_memory;
use crate::models::in_memory_database::InMemoryDatabase;
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3);

pub async fn in_memory_database_maintaince(
    memory_database: Arc<InMemoryDatabase>,
    dune_download_folder: String,
) {
    let db = Arc::clone(&memory_database);
    loop {
        {
            match load_dune_data_into_memory(dune_download_folder.clone() + "user_data/") {
                Ok(new_data_mutex) => {
                    let mut guard = match db.0.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    *guard = new_data_mutex;
                }
                Err(err) => match err.to_string().contains("EOF while parsing a value") {
                    true => {
                        // Sometimes unexpected EOF error messages are thrown, if the reading of rust is faster than the writting of the python scripts. Since this is expected, we don't error.
                        tracing::debug!("Could not read the dune data, due to error: {:?}", err)
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

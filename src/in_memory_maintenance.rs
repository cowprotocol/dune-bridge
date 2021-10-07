use crate::dune_data_loading::load_dune_data_into_memory;
use crate::models::in_memory_database::InMemoryDatabase;
use std::sync::Arc;
use std::time::Duration;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3);

pub async fn in_memory_database_maintaince(
    memory_database: Arc<InMemoryDatabase>,
    dune_download_file: String,
) {
    let db = Arc::clone(&memory_database);
    loop {
        {
            match load_dune_data_into_memory(dune_download_file.clone()) {
                Ok(new_data_mutex) => {
                    let mut guard = match db.0.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    *guard = new_data_mutex;
                }
                Err(err) => {
                    tracing::error!("Could not read the dune data, due to error: {:?}", err)
                }
            };
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

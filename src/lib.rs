pub mod api;
pub mod dune_data_loading;
pub mod in_memory_maintenance;
pub mod models;
extern crate serde_derive;

use models::in_memory_database::InMemoryDatabase;
use std::{net::SocketAddr, sync::Arc};
use tokio::{task, task::JoinHandle};

pub fn serve_task(db: Arc<InMemoryDatabase>, address: SocketAddr) -> JoinHandle<()> {
    let filter = api::handle_all_routes(db);
    tracing::info!(%address, "serving dune data");
    task::spawn(warp::serve(filter).bind(address))
}

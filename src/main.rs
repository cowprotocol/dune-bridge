use gpdata::dune_data_loading::load_dune_data_into_memory;
use gpdata::in_memory_maintenance::in_memory_database_maintaince;
use gpdata::models::in_memory_database::InMemoryDatabase;
use gpdata::serve_task;
use gpdata::tracing_helper::initialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Arguments {
    #[structopt(long, env = "LOG_FILTER", default_value = "warn,debug,info")]
    pub log_filter: String,
    #[structopt(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:8080")]
    bind_address: SocketAddr,
    #[structopt(long, env = "DUNE_DATA_FILE", default_value = "./user_data.json")]
    dune_data_file: String,
}

#[tokio::main]
async fn main() {
    let args = Arguments::from_args();
    initialize(args.log_filter.as_str());
    tracing::info!("running data-server with {:#?}", args);
    let dune_download_file = args.dune_data_file;

    let dune_data = load_dune_data_into_memory(dune_download_file.clone())
        .expect("could not load data into memory");
    let memory_database = Arc::new(InMemoryDatabase(Mutex::new(dune_data)));

    let serve_task = serve_task(memory_database.clone(), args.bind_address);
    let maintance_task = tokio::task::spawn(in_memory_database_maintaince(
        memory_database.clone(),
        dune_download_file,
    ));
    tokio::select! {
        result = maintance_task => tracing::error!(?result, "maintance task exited"),
        result = serve_task => tracing::error!(?result, "serve task exited"),
    };
}

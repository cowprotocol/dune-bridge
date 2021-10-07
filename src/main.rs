use gpdata::dune_data_loading::load_dune_data_into_memory;
use gpdata::models::in_memory_database::InMemoryDatabase;
use gpdata::serve_task;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Arguments {
    #[structopt(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:8080")]
    bind_address: SocketAddr,
    #[structopt(long, env = "DUNE_DATA_FILE", default_value = "./user_data.json")]
    dune_data_file: String,
}

#[tokio::main]
async fn main() {
    let args = Arguments::from_args();
    tracing::info!("running data-server with {:#?}", args);
    let dune_data =
        load_dune_data_into_memory(args.dune_data_file).expect("could not load data into memory");
    let memory_database = Arc::new(InMemoryDatabase(Mutex::new(dune_data)));

    let serve_task = serve_task(memory_database.clone(), args.bind_address);
    tokio::select! {
        result = serve_task => tracing::error!(?result, "serve task exited"),
    };
}

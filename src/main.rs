use gpdata::dune_data_loading::load_dune_data_into_memory;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Arguments {
    #[structopt(long, env = "DUNE_DATA_FILE", default_value = "./user_data.json")]
    dune_data_file: String,
}

fn main() {
    let args = Arguments::from_args();
    tracing::info!("running data-server with {:#?}", args);
    let dune_data =
        load_dune_data_into_memory(args.dune_data_file).expect("could not load data into memory");
    println!("{:?}", dune_data);
}

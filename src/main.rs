mod dataset;
mod gcp;
mod utils;
use clap::Parser;
//use dotenv::dotenv;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// task name
    #[arg(short, long)]
    task: String,
}

#[tokio::main]
async fn main() {
    //dotenv().ok();
    let args = Args::parse();

    match args.task.as_str() {
        "collect" => dataset::collect::from_binance_data().await,
        "balance" => dataset::balance::balance_categories().await,
        _ => print!("No task, exiting..."),
    };
}

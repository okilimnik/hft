mod dataset;
mod gcp;
mod utils;
use clap::Parser;
use dotenv::dotenv;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// task name
    #[arg(short, long)]
    task: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    dotenv().ok();
    env_logger::init();
    let args = Args::parse();

    match args.task.as_str() {
        "collect" => dataset::collect::from_binance_data().await,
        _ => print!("No task, exiting..."),
    };
}

mod dataset;
mod gcp;
mod lightgbm;
mod trade;
mod ui;
use clap::Parser;
use dotenv::dotenv;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// task name
    #[arg(short, long)]
    task: String,
}

fn main() {
    dotenv().ok();
    env_logger::init();
    let args = Args::try_parse();
    match args {
        Ok(command) => match command.task.as_str() {
            "split" => dataset::utils::split(),
            "train" => lightgbm::train(),
            _ => println!("Unknown task, exiting..."),
        },
        Err(_) => dataset::collect::from_binance_data(),
    };
}

mod dataset;
mod gcp;
mod utils;
use dotenv::dotenv;

fn main() {
    dotenv().ok();
    dataset::from_binance_data();
}

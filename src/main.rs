mod dataset;
mod db;
mod gcp;
use dotenv::dotenv;

fn main() {
    dotenv().ok();
    dataset::from_binance_data();
}

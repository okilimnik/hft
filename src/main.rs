mod dataset;
//mod lightgbm;
mod db;
mod server;
//mod stats;
use dotenv::dotenv;
use std::thread;

fn main() {
    dotenv().ok();
    thread::spawn(move || {
        dataset::from_binance_data();
    });
    // let _ = server::start();
    // lightgbm::train();
}

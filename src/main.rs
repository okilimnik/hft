mod dataset;
mod lightgbm;
mod server;
mod stats;
use std::thread;

fn main() {
    thread::spawn(move || {
        dataset::maintain();
    });
    let _ = server::start();
    // lightgbm::train();
}

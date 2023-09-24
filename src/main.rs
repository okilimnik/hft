mod dataset;
mod gcp;
mod utils;
//use dotenv::dotenv;

#[tokio::main]
async fn main() {
    //dotenv().ok();
    dataset::collect::from_binance_data().await;
}

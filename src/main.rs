mod dataset;
mod gcp;
mod utils;
//use dotenv::dotenv;

#[tokio::main]
async fn main() {
    //dotenv().ok();
    dataset::from_binance_data().await;
}

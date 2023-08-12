use std::time::Duration;
use std::collections::HashMap;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};

const URL: &'static str = "https://api.binance.com";
const SYMBOL: &'static str = "BTCTUSD";

pub async fn fetch(endpoint: String) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let url = URL.to_owned() + &endpoint;
    let resp = reqwest::get(&url).await?.json::<HashMap<String, String>>().await?;
    Ok(resp)
}

pub async fn depth() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    fetch(format!("/api/v3/depth?symbol={}", SYMBOL)).await
}

pub async fn collect() -> Result<(), JobSchedulerError> {
    let schedulder = JobScheduler::new().await?;
    schedulder
        .add(Job::new("1/10 * * * * *", |_uuid, _l| {
            tokio::spawn(async move {
                let data = depth().await;
                println!("{:?}", data);
            });
        })?)
        .await?;
    schedulder.start().await?;
    tokio::time::sleep(Duration::from_secs(100)).await;
    Ok(())
}

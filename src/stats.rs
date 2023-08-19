use average::{Variance, Quantile, Estimate, concatenate};
use binance::model::OrderBook;
use serde::{Deserialize, Serialize};

concatenate!(Estimator,
    [Variance, variance, mean, sample_variance],
    [Quantile, quantile, quantile]);

pub fn calc() {
    let s: Estimator = (1..6).map(f64::from).collect();
    println!("{:?}", s.sample_variance());
    println!("{:?}", s.mean());  
    println!("{:?}", s.quantile());  
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookSnapshotStats {
    pub variance: f64,
    pub mean: f64,
    pub quantile: f64
}

pub fn extract_order_book_snapshot_stats(order_book: &OrderBook) -> OrderBookSnapshotStats {
    let s: Estimator = order_book
        .asks
        .iter()
        .map(|y| {
            y.price
        })
        .collect(); 
    OrderBookSnapshotStats {
        variance: s.sample_variance(),
        mean: s.mean(),
        quantile: s.quantile()
    }
}
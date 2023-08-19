use average::{Variance, Quantile, Estimate, concatenate};
use binance::model::OrderBook;
use serde::{Deserialize, Serialize};
use linfa_preprocessing::linear_scaling::LinearScaler;

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
    pub quantile: f64,
    pub weighted_variance: f64,
    pub weighted_mean: f64,
    pub weighted_quantile: f64
}

pub fn extract_order_book_snapshot_stats(order_book: &OrderBook) -> OrderBookSnapshotStats {
    let s: Estimator = order_book
        .asks
        .iter()
        .map(|y| {
            y.price
        })
        .collect(); 
    let weighted_s: Estimator = order_book
        .asks
        .iter()
        .map(|y| {
            y.price * y.qty
        })
        .collect(); 
    OrderBookSnapshotStats {
        variance: s.sample_variance(),
        mean: s.mean(),
        quantile: s.quantile(),
        weighted_variance: weighted_s.sample_variance(),
        weighted_mean: weighted_s.mean(),
        weighted_quantile: weighted_s.quantile(),
    }
}
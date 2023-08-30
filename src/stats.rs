use average::{concatenate, Estimate, Quantile, Variance};
use binance::model::OrderBook;
use serde::{Deserialize, Serialize};
use std::fmt;

const MAX_BTC_PRICE: f64 = 40000.0;
const MIN_BTC_PRICE: f64 = 20000.0;

concatenate!(
    Estimator,
    [Variance, variance, mean, sample_variance],
    [Quantile, quantile, quantile]
);

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookSnapshotStats {
    pub price: f64,
    pub asks_variance: f64,
    pub asks_mean: f64,
    pub asks_quantile: f64,
    pub asks_weighted_variance: f64,
    pub asks_weighted_mean: f64,
    pub asks_weighted_quantile: f64,
    pub bids_variance: f64,
    pub bids_mean: f64,
    pub bids_quantile: f64,
    pub bids_weighted_variance: f64,
    pub bids_weighted_mean: f64,
    pub bids_weighted_quantile: f64,
}

impl fmt::Display for OrderBookSnapshotStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TSV format
        write!(f, "{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}\t{:.5}", min_max_scale(self.price), 
        self.asks_variance, self.asks_mean, self.asks_quantile, self.asks_weighted_variance, self.asks_weighted_mean, self.asks_weighted_quantile,
        self.bids_variance, self.bids_mean, self.bids_quantile, self.bids_weighted_variance, self.bids_weighted_mean, self.bids_weighted_quantile)
    }
}

fn min_max_scale(price: f64) -> f64 {
    (price - MIN_BTC_PRICE) / (MAX_BTC_PRICE - MIN_BTC_PRICE)
}

pub fn extract_order_book_snapshot_stats(
    price: f64,
    order_book: &OrderBook,
) -> OrderBookSnapshotStats {
    let asks_s: Estimator = order_book
        .asks
        .iter()
        .map(|y| min_max_scale(y.price))
        .collect();
    let asks_weighted_s: Estimator = order_book
        .asks
        .iter()
        .map(|y| min_max_scale(y.price) * y.qty)
        .collect();
    let asks_weight: f64 = order_book.asks.iter().map(|y| y.qty).sum();
    let bids_s: Estimator = order_book
        .bids
        .iter()
        .map(|y| min_max_scale(y.price))
        .collect();
    let bids_weighted_s: Estimator = order_book
        .bids
        .iter()
        .map(|y| min_max_scale(y.price) * y.qty)
        .collect();
    let bids_weight: f64 = order_book.bids.iter().map(|y| y.qty).sum();
    OrderBookSnapshotStats {
        price,
        asks_variance: asks_s.sample_variance(),
        asks_mean: asks_s.mean(),
        asks_quantile: asks_s.quantile(),
        asks_weighted_variance: asks_weighted_s.sample_variance() / asks_weight,
        asks_weighted_mean: asks_weighted_s.mean() / asks_weight,
        asks_weighted_quantile: asks_weighted_s.quantile() / asks_weight,
        bids_variance: bids_s.sample_variance(),
        bids_mean: bids_s.mean(),
        bids_quantile: bids_s.quantile(),
        bids_weighted_variance: bids_weighted_s.sample_variance() / bids_weight,
        bids_weighted_mean: bids_weighted_s.mean() / bids_weight,
        bids_weighted_quantile: bids_weighted_s.quantile() / bids_weight,
    }
}

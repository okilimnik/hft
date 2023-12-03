use binance::model::{Asks, Bids, DepthOrderBookEvent, OrderBook};
use itertools::Itertools;
use merge_hashmap::Merge;
use rustc_hash::FxHashMap;

#[derive(Clone, Merge)]
pub struct OrderBookState {
    pub last_update_id: u64,
    pub bids: FxHashMap<i64, f64>,
    pub asks: FxHashMap<i64, f64>,
}

impl OrderBookState {
    pub fn from(last_update_id: u64, bids_vec: Vec<Bids>, asks_vec: Vec<Asks>) -> OrderBookState {
        let mut bids_map: FxHashMap<i64, f64> = FxHashMap::default();
        for bid in bids_vec
            .iter()
            .map(|x| ((x.price / 10.0).floor() as i64 * 10, x.qty))
        {
            *bids_map.entry(bid.0).or_insert(0f64) += bid.1;
        }
        let bids: FxHashMap<i64, f64> = bids_map
            .iter()
            .sorted_by_key(|a| a.0)
            .rev()
            .take(20)
            .map(|a| (a.0.to_owned(), a.1.to_owned()))
            .collect();

        let mut asks_map: FxHashMap<i64, f64> = FxHashMap::default();
        for ask in asks_vec
            .iter()
            .map(|x| ((x.price / 10.0).floor() as i64 * 10, x.qty))
        {
            *asks_map.entry(ask.0).or_insert(0f64) += ask.1;
        }
        let asks: FxHashMap<i64, f64> = asks_map
            .iter()
            .sorted_by_key(|a| a.0)
            .rev()
            .take(20)
            .map(|a| (a.0.to_owned(), a.1.to_owned()))
            .collect();

        OrderBookState {
            bids,
            asks,
            last_update_id,
        }
    }

    pub fn merge(&mut self, updates: OrderBookState) {
        for entry in updates.bids.iter() {
            *self.bids.entry(*entry.0).or_insert(0f64) += entry.1;
        }
        self.bids = self
            .bids
            .iter()
            .sorted_by_key(|a| a.0)
            .rev()
            .take(20)
            .map(|a| (a.0.to_owned(), a.1.to_owned()))
            .collect();
        for entry in updates.asks.iter() {
            *self.asks.entry(*entry.0).or_insert(0f64) += entry.1;
        }
        self.asks = self
            .asks
            .iter()
            .sorted_by_key(|a| a.0)
            .rev()
            .take(20)
            .map(|a| (a.0.to_owned(), a.1.to_owned()))
            .collect();
    }
}

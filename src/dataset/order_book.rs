use binance::model::{DepthOrderBookEvent, OrderBook};
use merge_hashmap::Merge;
use std::collections::HashMap;

#[derive(Clone, Merge)]
pub struct OrderBookState {
    #[merge(strategy = merge_hashmap::ord::max)]
    pub last_update_id: u64,
    #[merge(strategy = merge_hashmap::hashmap::overwrite)]
    pub bids: HashMap<String, f64>,
    #[merge(strategy = merge_hashmap::hashmap::overwrite)]
    pub asks: HashMap<String, f64>,
}

impl OrderBookState {
    pub fn from1(order_book: OrderBook) -> OrderBookState {
        OrderBookState {
            bids: order_book
                .bids
                .iter()
                .map(|x| (format!("{}", x.price), x.qty))
                .collect(),
            asks: order_book
                .asks
                .iter()
                .map(|x| (format!("{}", x.price), x.qty))
                .collect(),
            last_update_id: order_book.last_update_id,
        }
    }

    pub fn from2(order_book: DepthOrderBookEvent) -> OrderBookState {
        OrderBookState {
            bids: order_book
                .bids
                .iter()
                .map(|x| (format!("{}", x.price), x.qty))
                .collect(),
            asks: order_book
                .asks
                .iter()
                .map(|x| (format!("{}", x.price), x.qty))
                .collect(),
            last_update_id: order_book.final_update_id,
        }
    }

    pub fn filter(&mut self) {
        let filtered_asks: HashMap<String, f64> = self
            .asks
            .iter()
            .filter(|x| *x.1 > 0f64)
            .map(|x| (x.0.to_owned(), *x.1))
            .collect();
        let filtered_bids: HashMap<String, f64> = self
            .bids
            .iter()
            .filter(|x| *x.1 > 0f64)
            .map(|x| (x.0.to_owned(), *x.1))
            .collect();
        self.asks = filtered_asks;
        self.bids = filtered_bids;
    }
}

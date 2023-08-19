use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook};
use binance::websockets::*;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;
use serde::{Deserialize, Serialize};
use crate::stats;
use crate::stats::OrderBookSnapshotStats;

const SYMBOL: &'static str = "BTCTUSD";

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OrderBookStats {
    variance: Vec<f64>,
    mean: Vec<f64>,
    quantile: Vec<f64>,
    weighted_variance: Vec<f64>,
    weighted_mean: Vec<f64>,
    weighted_quantile: Vec<f64>,
}

impl OrderBookStats {
    fn push(&mut self, snapshot_stats: OrderBookSnapshotStats) {
        self.variance.push(snapshot_stats.variance);
        self.mean.push(snapshot_stats.mean);
        self.quantile.push(snapshot_stats.quantile);
        self.weighted_variance.push(snapshot_stats.variance);
        self.weighted_mean.push(snapshot_stats.mean);
        self.weighted_quantile.push(snapshot_stats.quantile);
    }
}

fn to_file(filename: &str, data: String, append: bool) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .open(format!("./{}", filename))
        .expect(&format!("Unable to write {}", filename));
    if let Err(e) = writeln!(file, "{}", data) {
        eprintln!("Couldn't write to file: {}", e);
    };
}

fn maintain_order_book(event: DepthOrderBookEvent, order_book: &mut OrderBook, market: &Market) {
    if order_book.last_update_id == 0 {
        order_book.clone_from(
            &market
                .get_custom_depth(SYMBOL, 5000)
                .expect("Failed to get initial order book."),
        );
    } else {
        if event.final_update_id > order_book.last_update_id {
            order_book.last_update_id = event.final_update_id;
            for x in event.bids.iter() {
                let mut bid_level_present = false;
                order_book.bids = order_book
                    .bids
                    .iter()
                    .map(|y| {
                        if x.price == y.price {
                            bid_level_present = true;
                            x.clone()
                        } else {
                            y.clone()
                        }
                    })
                    .filter(|x| x.qty > 0f64)
                    .collect();
                if !bid_level_present && x.qty > 0f64 {
                    order_book.bids.insert(0, x.clone());
                }
            }
            for x in event.asks.iter() {
                let mut ask_level_present = false;
                order_book.asks = order_book
                    .asks
                    .iter()
                    .map(|y| {
                        if x.price == y.price {
                            ask_level_present = true;
                            x.clone()
                        } else {
                            y.clone()
                        }
                    })
                    .filter(|x| x.qty > 0f64)
                    .collect();
                if !ask_level_present && x.qty > 0f64 {
                    order_book.asks.insert(0, x.clone());
                }
            }
        }
    }
    to_file(
        "order_book.json",
        serde_json::to_string(order_book).expect("Failed to stringify order book."),
        false,
    );
}

fn maintain_order_book_stats(order_book: &OrderBook, order_book_stats: &mut OrderBookStats) {
    order_book_stats.push(stats::extract_order_book_snapshot_stats(&order_book));
    to_file("order_book_stats.json", serde_json::to_string(order_book_stats).unwrap(), false);
}

fn add_to_model_inputs(order_book_stats: &OrderBookStats) {
    to_file("input.txt", serde_json::to_string(order_book_stats).unwrap(), true);
}

pub fn maintain() {
    let streams = [
        format!("{}@depth@100ms", SYMBOL.to_lowercase()),
        format!("{}@aggTrade", SYMBOL.to_lowercase()),
    ];
    let market: Market = Binance::new(None, None);
    let mut order_book = OrderBook {
        last_update_id: 0,
        bids: vec![],
        asks: vec![],
    };
    let mut order_book_stats = OrderBookStats {
        variance: vec![],
        mean: vec![],
        quantile: vec![],
        weighted_variance: vec![],
        weighted_mean: vec![],
        weighted_quantile: vec![]
    };
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        match event {
            WebsocketEvent::DepthOrderBook(event) => {
                maintain_order_book(event, &mut order_book, &market);
                maintain_order_book_stats(&order_book, &mut order_book_stats);
                add_to_model_inputs(&order_book_stats);
            }
            WebsocketEvent::AggrTrades(event) => {}
            _ => (),
        };

        Ok(())
    });

    let keep_running = AtomicBool::new(true);
    web_socket
        .connect_multiple_streams(&streams)
        .expect("Cannot connect to ws streams");
    if let Err(e) = web_socket.event_loop(&keep_running) {
        match e {
            err => {
                eprintln!("Error: {}", err);
            }
        }
    }
}

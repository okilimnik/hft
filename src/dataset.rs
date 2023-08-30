use crate::stats;
use crate::stats::OrderBookSnapshotStats;
use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook};
use binance::websockets::*;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;

const SYMBOL: &str = "BTCTUSD";
const DATA_SIZE: usize = 360;
const FILENAME_TRAIN: &str = "lgbm.train";
const FILENAME_VALID: &str = "lgbm.valid";
const FILENAME_TEST: &str = "lgbm.test";

fn to_file(filename: &str, data: String, append: bool) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .open(format!("./{}", filename))
        .unwrap_or_else(|_| panic!("Unable to write {}", filename));
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
    } else if event.final_update_id > order_book.last_update_id {
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
    };
}

fn maintain_order_book_stats(
    price: f64,
    order_book: &OrderBook,
    order_book_stats: &mut Vec<OrderBookSnapshotStats>,
) {
    order_book_stats.push(stats::extract_order_book_snapshot_stats(price, order_book));
}

fn add_to_model_inputs(order_book_stats: &[OrderBookSnapshotStats]) {
    let i = DATA_SIZE / 2 - 1;
    let next_prices: &Vec<f64> = &order_book_stats[i + 1..i + DATA_SIZE / 2]
        .iter()
        .map(|x| x.price)
        .collect();
    let mut next_prices_sorted = next_prices.clone();
    next_prices_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min_price = next_prices_sorted.first().unwrap();
    let max_price = next_prices_sorted.last().unwrap();
    let current_price = order_book_stats[i].price;
    let bullish_change = max_price - current_price;
    let bearish_change = current_price - min_price;
    let up = bullish_change > bearish_change;
    let percentage_change = if up {
        bullish_change / current_price
    } else {
        bearish_change / current_price
    };
    if percentage_change.abs() > 0.001 {
        let label = format!("{:.5}", percentage_change);
        let inputs = &order_book_stats[0..i + 1];
        let svm_row = inputs.iter().fold(label, |acc, x| format!("{acc}\t{x}"));
        to_file(FILENAME_TRAIN, svm_row, true);
    }
}

pub fn maintain() {
    let streams = [
        format!("{}@kline_1m", SYMBOL.to_lowercase()),
        format!("{}@depth", SYMBOL.to_lowercase()),
        // format!("{}@aggTrade", SYMBOL.to_lowercase()),
    ];
    let market: Market = Binance::new(None, None);
    let mut order_book = OrderBook {
        last_update_id: 0,
        bids: vec![],
        asks: vec![],
    };
    let mut current_price = 0f64;
    let mut order_book_stats: Vec<OrderBookSnapshotStats> = vec![];
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        match event {
            WebsocketEvent::Kline(event) => {
                current_price = event.kline.close.parse::<f64>().unwrap();
            }
            WebsocketEvent::DepthOrderBook(event) => {
                maintain_order_book(event, &mut order_book, &market);
                if current_price > 0.0 {
                    maintain_order_book_stats(current_price, &order_book, &mut order_book_stats);
                    if order_book_stats.len() > DATA_SIZE {
                        // we don't need more than DATA_SIZE values, first DATA_SIZE / 2 for the input, next DATA_SIZE / 2 for the label
                        order_book_stats =
                            order_book_stats[order_book_stats.len() - DATA_SIZE..].to_vec();
                        add_to_model_inputs(&order_book_stats);
                    }
                }
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
        {
            eprintln!("Error: {}", e);
        }
    }
}

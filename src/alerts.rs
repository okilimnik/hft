use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook, WindowTickerEvent};
use binance::websockets::*;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::process::Command;
use std::sync::atomic::AtomicBool;
use serde::{Deserialize, Serialize};
use crate::stats;

const SYMBOL: &'static str = "BTCTUSD";

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OrderBookStats {
    variance: Vec<f64>,
    mean: Vec<f64>,
    quantile: Vec<f64>
}

fn play_sound() {
    Command::new("afplay")
        .arg("alert.wav")
        .output()
        .expect("Couldn't play sound");
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
    println!("order_book.last_update_id = {}", order_book.last_update_id);
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

fn notify_on_big_moves(events: Vec<WindowTickerEvent>) {
    let mut symbols: HashSet<String> = vec![].into_iter().collect();
    for event in events {
        let change: f32 = event
            .price_change_percent
            .parse()
            .expect("Cannot parse price_change_percent");
        let symbol = event.symbol;
        if change >= 5.0 && !symbols.contains(&symbol) && symbol.ends_with("USDT") {
            symbols.insert(symbol.clone());
            play_sound();
            to_file("alerts.txt", format!("{} - {}", symbol, change), true);
        }
        if change >= 5.0 && !symbols.contains(&symbol) && symbol.ends_with("USDT") {
            symbols.insert(symbol.clone());
            play_sound();
            to_file("alerts.txt", format!("{} - {}", symbol, change), true);
        }
    }
}

fn maintain_order_book_stats(order_book: &OrderBook, order_book_stats: &mut OrderBookStats) {
    let order_book_snapshot_stats = stats::extract_order_book_snapshot_stats(&order_book);
    order_book_stats.variance.push(order_book_snapshot_stats.variance);
    order_book_stats.mean.push(order_book_snapshot_stats.mean);
    order_book_stats.quantile.push(order_book_snapshot_stats.quantile);
    to_file("order_book_stats.json", serde_json::to_string(order_book_stats).unwrap(), false);
}

pub fn subscribe() {
    let keep_running = AtomicBool::new(true);
    let streams = [
        //String::from("!ticker_1h@arr"),
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
        quantile: vec![]
    };
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        match event {
            WebsocketEvent::WindowTickerAll(events) => {
                //notify_on_big_moves(events);
            }
            WebsocketEvent::DepthOrderBook(event) => {
                maintain_order_book(event, &mut order_book, &market);
                maintain_order_book_stats(&order_book, &mut order_book_stats);
            }
            WebsocketEvent::AggrTrades(event) => {}
            _ => (),
        };

        Ok(())
    });

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

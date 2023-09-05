use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook};
use binance::websockets::*;
use itertools::Itertools;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;

use crate::db;

const SYMBOL: &str = "BTCTUSD";
const FILENAME_ORDER_BOOK: &str = "./datasets/order_book.txt";

fn to_file(filename: &str, data: String, append: bool) {
    fs::create_dir_all("./datasets").unwrap();
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

fn prepare(data: &[(f64, f64)]) -> Vec<(String, f64)> {
    data.iter()
        .group_by(|e| format!("{:.0}", e.0))
        .into_iter()
        .map(|e| -> (String, f64) {
            let (key, group) = e;
            let sum: f64 = group.map(|e| e.1).sum();
            (key, sum)
        })
        .sorted_by_key(|e| e.clone().0)
        .collect_vec()
}

fn order_book_to_svm(label: &f64, order_book: &OrderBook) -> String {
    let mut row: Vec<(String, f64)> = vec![];
    let list: &Vec<(f64, f64)> = &order_book
        .bids
        .clone()
        .into_iter()
        .map(|e| (e.price.round(), e.qty))
        .collect();
    let mut bids = prepare(list);
    let list: &Vec<(f64, f64)> = &order_book
        .asks
        .clone()
        .into_iter()
        .map(|e| (e.price.round(), -e.qty))
        .collect();
    let mut asks = prepare(list);
    row.append(&mut bids);
    row.append(&mut asks);
    row.into_iter().fold(format!("{}", label), |acc, e| {
        acc + " " + &e.0 + ":" + &format!("{:.5}", e.1)
    })
}

fn maintain_order_book(event: DepthOrderBookEvent, order_book: &mut OrderBook, market: &Market) {
    let price = &market
        .get_price(SYMBOL)
        .expect("Failed to get current price.")
        .price;
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
    db::insert(*price, order_book.clone()).await;
    /*to_file(
        FILENAME_ORDER_BOOK,
        order_book_to_svm(price, order_book),
        true,
    );*/
}

pub fn maintain() {
    let market: Market = Binance::new(None, None);
    let mut order_book = OrderBook {
        last_update_id: 0,
        bids: vec![],
        asks: vec![],
    };
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        if let WebsocketEvent::DepthOrderBook(event) = event {
            maintain_order_book(event, &mut order_book, &market);
        }
        Ok(())
    });
    let keep_running = AtomicBool::new(true);
    web_socket
        .connect(&format!("{}@depth", SYMBOL.to_lowercase()))
        .expect("Cannot connect to ws streams");
    if let Err(e) = web_socket.event_loop(&keep_running) {
        {
            eprintln!("Error: {}", e);
        }
    }
}

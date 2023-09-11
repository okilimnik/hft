use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook};
use binance::websockets::*;
use image::{GenericImage, GenericImageView, ImageBuffer, RgbImage};
use itertools::Itertools;
use std::collections::VecDeque;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;

const SYMBOL: &str = "BTCTUSD";
const FILENAME_ORDER_BOOK: &str = "./datasets/order_book.txt";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 5;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;

struct OrderBookSnapshot {
    order_book: OrderBook,
    best_ask: f64,
    best_bid: f64,
    min_ask: f64,
    max_bid: f64,
    max_shift: f64,
}

impl OrderBookSnapshot {
    fn from(order_book: OrderBook) -> OrderBookSnapshot {
        let best_ask = order_book
            .asks
            .iter()
            .max_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let min_ask = order_book
            .asks
            .iter()
            .min_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let best_bid = order_book
            .bids
            .iter()
            .min_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let max_bid = order_book
            .bids
            .iter()
            .max_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let max_shift = [max_bid - best_bid, best_ask - min_ask]
            .iter()
            .fold(0. / 0., f64::max);
        OrderBookSnapshot {
            order_book,
            best_ask,
            best_bid,
            min_ask,
            max_bid,
            max_shift,
        }
    }
}

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

pub fn to_image() {
    let img = ImageBuffer::from_fn(512, 512, |x, y| {
        if x % 2 == 0 {
            image::Luma([0u8])
        } else {
            image::Luma([255u8])
        }
    });
    if let Err(e) = img.save("./test.png") {
        eprintln!("Cannot save dataset image on disk: {}", e);
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

fn order_book_to_svm(label: &f64, order_book: &VecDeque<OrderBook>) -> Option<String> {
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
    let svm = row.into_iter().fold(format!("{}", label), |acc, e| {
        acc + " " + &e.0 + ":" + &format!("{:.5}", e.1)
    });
    Some(svm)
}

fn update_order_book_snapshots(
    order_book_snapshots: &mut VecDeque<OrderBookSnapshot>,
    event: DepthOrderBookEvent,
    market: &Market,
) {
    if order_book_snapshots.is_empty() {
        let snapshot = market
            .get_custom_depth(SYMBOL, 5000)
            .expect("Failed to get initial order book.");
        order_book_snapshots.push_back(OrderBookSnapshot::from(snapshot));
    };
    if event.final_update_id
        > order_book_snapshots
            .get(order_book_snapshots.len() - 1)
            .unwrap()
            .order_book
            .last_update_id
    {
        let mut order_book = order_book_snapshots
            .get(order_book_snapshots.len() - 1)
            .unwrap()
            .order_book
            .clone();
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
        order_book_snapshots.push_back(OrderBookSnapshot::from(order_book));
    }
    if order_book_snapshots.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_snapshots.pop_front();
    }
}

fn create_input_image(order_book_snapshots: &VecDeque<OrderBookSnapshot>) {
    let prepared_data: Vec<Vec<&f64>> = order_book_snapshots
        .iter()
        .map(|snapshot| -> Vec<&f64> {
            let price_level_shift = snapshot.max_shift / HISTORY_SIZE as f64 / 2.0;
            (0..HISTORY_SIZE)
                .map(|y| -> &f64 {
                    if y >= HISTORY_SIZE / 2 {
                        &snapshot
                            .order_book
                            .bids
                            .iter()
                            .fold(0f64, |acc, bid| -> f64 {
                                let price_level =
                                    (((snapshot.max_bid - bid.price) / price_level_shift).floor())
                                        as u32;
                                if price_level == (y - HISTORY_SIZE / 2) as u32 {
                                    acc + bid.qty
                                } else {
                                    acc
                                }
                            })
                    } else {
                        &snapshot
                            .order_book
                            .asks
                            .iter()
                            .fold(0f64, |acc, ask| -> f64 {
                                let price_level =
                                    (((ask.price - snapshot.min_ask) / price_level_shift).floor())
                                        as u32;
                                if (HISTORY_SIZE / 2) as u32 - price_level == y as u32 {
                                    acc + ask.qty
                                } else {
                                    acc
                                }
                            })
                    }
                })
                .collect()
        })
        .collect();
    let max_quantities: Vec<&&f64> = prepared_data
        .iter()
        .map(|x| x.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap())
        .collect();
    let max_quantity = max_quantities
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
        .to_owned()
        .to_owned();
    let quantity_level_shift = max_quantity / 255f64;
    let img = ImageBuffer::from_fn(HISTORY_SIZE as u32, HISTORY_SIZE as u32, |x, y| {
        let x_data = prepared_data.get(x as usize).unwrap();
        let y_data = x_data.get(y as usize).unwrap().to_owned();
        let color = (y_data / quantity_level_shift).floor() as u8;
        image::Luma([color])
    });
    if let Err(e) = img.save("./test.png") {
        eprintln!("Cannot save dataset image on disk: {}", e);
    };
}

pub fn from_binance_data() {
    let market: Market = Binance::new(None, None);
    let mut order_book_snapshots: VecDeque<OrderBookSnapshot> = VecDeque::new();
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        if let WebsocketEvent::DepthOrderBook(event) = event {
            update_order_book_snapshots(&mut order_book_snapshots, event, &market);
            if order_book_snapshots.len() == ORDER_BOOK_QUEUE_SIZE {
                create_input_image(&order_book_snapshots);
            }
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

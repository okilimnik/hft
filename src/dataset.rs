use crate::gcp;
use binance::api::*;
use binance::market::*;
use binance::model::{DepthOrderBookEvent, OrderBook};
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use image::ImageBuffer;
use lazy_static::lazy_static;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::task;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 10;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
static IMAGES_COUNT: AtomicUsize = AtomicUsize::new(0);
static KEEP_RUNNING: AtomicBool = AtomicBool::new(true);

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
    static ref ORDER_BOOK: tokio::sync::Mutex<VecDeque<OrderBookState>> =
        tokio::sync::Mutex::new(VecDeque::new());
}

#[derive(Clone)]
struct OrderBookState {
    order_book: OrderBook,
    best_ask: f64,
    best_bid: f64,
    max_ask: f64,
    min_bid: f64,
}

impl OrderBookState {
    fn from(order_book: OrderBook) -> OrderBookState {
        let best_ask = order_book
            .asks
            .iter()
            .min_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let min_bid = order_book
            .bids
            .iter()
            .min_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let best_bid = order_book
            .bids
            .iter()
            .max_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        let max_ask = order_book
            .asks
            .iter()
            .max_by_key(|x| (x.price * 100000.0).round() as i64)
            .unwrap()
            .price;
        OrderBookState {
            order_book,
            best_ask,
            best_bid,
            max_ask,
            min_bid,
        }
    }
}

async fn calc_new_order_book_state(event: DepthOrderBookEvent) -> VecDeque<OrderBookState> {
    let mut order_book_state_series = ORDER_BOOK.lock().await;
    if order_book_state_series.is_empty() {
        let new_order_book = task::spawn_blocking(move || {
            MARKET
                .get_custom_depth(SYMBOL, 5000)
                .expect("Failed to get initial order book.")
        })
        .await
        .unwrap();
        order_book_state_series.push_back(OrderBookState::from(new_order_book));
    };
    if event.final_update_id
        > order_book_state_series
            .back()
            .unwrap()
            .order_book
            .last_update_id
    {
        let mut new_order_book = order_book_state_series.back().unwrap().order_book.clone();
        new_order_book.last_update_id = event.final_update_id;
        for x in event.bids.iter() {
            let mut bid_level_present = false;
            new_order_book.bids = new_order_book
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
                new_order_book.bids.insert(0, x.clone());
            }
        }
        for x in event.asks.iter() {
            let mut ask_level_present = false;
            new_order_book.asks = new_order_book
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
                new_order_book.asks.insert(0, x.clone());
            }
        }
        order_book_state_series.push_back(OrderBookState::from(new_order_book));
    }
    if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_state_series.pop_front();
    }
    order_book_state_series.clone()
}

// we define price change levels by step of 5%
// -0.1% -0.075% -0.05% -0.025% 0% 0.025% 0.05% 0.075% 0.1% becomes -4 -3 -2 -1 0 1 2 3 4
// al levels that expands more than 4 level become 4 level
// we don't want create images if price change is 0
fn calc_label(current_price: f64, next_price: f64) -> Option<String> {
    let shift = next_price - current_price;
    let mut change_level = ((shift.abs() * 100.0) / (current_price * 0.025)).floor() as i32;
    if shift < 0f64 {
        change_level = -change_level;
    }
    if change_level > 4 {
        change_level = 4;
    }
    if change_level < -4 {
        change_level = -4;
    }
    let label: String = (-4..5).fold("".to_string(), |acc: String, i: i32| -> String {
        if i == 0 {
            acc
        } else if i == change_level {
            format!("{acc}{}", "1")
        } else {
            format!("{acc}{}", "0")
        }
    });
    println!("{}", label);
    if label == "00000000" {
        None
    } else {
        Some(label)
    }
}

async fn create_input_image(order_book_snapshots: &VecDeque<OrderBookState>) {
    let max_price = order_book_snapshots
        .iter()
        .max_by(|a, b| a.max_ask.partial_cmp(&b.max_ask).unwrap())
        .unwrap()
        .max_ask;
    let min_price = order_book_snapshots
        .iter()
        .min_by(|a, b| a.min_bid.partial_cmp(&b.min_bid).unwrap())
        .unwrap()
        .min_bid;
    let price_level_shift = (max_price - min_price) / HISTORY_SIZE as f64;
    let prepared_data: Vec<Vec<f64>> = order_book_snapshots
        .iter()
        .map(|snapshot| -> Vec<f64> {
            (0..HISTORY_SIZE)
                .map(|y| -> f64 {
                    let bids: &Vec<(f64, f64, bool)> = &snapshot
                        .order_book
                        .bids
                        .iter()
                        .map(|x| (x.price, x.qty, true))
                        .collect();
                    let asks: &Vec<(f64, f64, bool)> = &snapshot
                        .order_book
                        .asks
                        .iter()
                        .map(|x| (x.price, x.qty, false))
                        .collect();
                    let mut prices: Vec<(f64, f64, bool)> = vec![];
                    prices.append(bids.clone().as_mut());
                    prices.append(asks.clone().as_mut());
                    let acc_qty = &prices.iter().fold(0f64, |acc, price| -> f64 {
                        let price_level =
                            ((price.0 - min_price) / price_level_shift).floor() as u32;
                        if price_level == y as u32 {
                            if price.2 {
                                acc + price.1
                            } else {
                                acc - price.1
                            }
                        } else {
                            acc
                        }
                    });
                    *acc_qty
                })
                .collect()
        })
        .collect();
    let max_quantities: Vec<&f64> = prepared_data
        .iter()
        .map(|x| {
            x.iter()
                .max_by(|a, b| a.abs().partial_cmp(&b.abs()).unwrap())
                .unwrap()
        })
        .collect();
    let max_quantity = (**(max_quantities
        .iter()
        .max_by(|a, b| a.abs().partial_cmp(&b.abs()).unwrap())
        .unwrap()))
    .abs();
    let quantity_level_shift = max_quantity / 128f64;
    let img = ImageBuffer::from_fn(HISTORY_SIZE as u32, HISTORY_SIZE as u32, |x, y| {
        let x_data = prepared_data.get(x as usize).unwrap();
        let y_data = x_data.get(y as usize).unwrap().to_owned();
        let color = 127 + (y_data / quantity_level_shift).floor() as u8;
        image::Luma([color])
    });
    let current_snapshot = order_book_snapshots.get(HISTORY_SIZE - 1).unwrap();
    //let the best ask in the current snapshot be the current price
    let current_price = current_snapshot.best_ask;
    println!("current_price: {}", current_price);
    // let the min best ask in the prediction head be the next price
    let next_prices: Vec<f64> = (HISTORY_SIZE..HISTORY_SIZE + PREDICTION_HEAD)
        .map(|x| -> f64 {
            let snapshot = order_book_snapshots.get(x).unwrap();
            snapshot.best_ask
        })
        .collect();
    let next_price = next_prices
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();
    println!("next price: {}", next_price);
    if let Some(label) = calc_label(current_price, *next_price) {
        let images_count = IMAGES_COUNT.fetch_add(1, Ordering::SeqCst);
        fs::create_dir_all("./dataset").unwrap();
        let filename = format!("{}_{}.png", label, images_count);
        let filepath = format!("./dataset/{}", filename);
        if let Err(e) = img.save(filepath.clone()) {
            eprintln!("Cannot save dataset image on disk: {}", e);
        };
        if let Err(e) = gcp::create_file(filename.clone(), filepath.clone()).await {
            eprintln!("Cannot save dataset file in cloud: {}", e);
        }
        if let Err(e) = fs::remove_file(filepath) {
            eprintln!("Cannot remove dataset file after saving in cloud: {}", e);
        }
    }
}

pub async fn from_binance_data() {
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        if let WebsocketEvent::DepthOrderBook(event) = event {
            tokio::spawn(async move {
                let order_book_state_series = calc_new_order_book_state(event).await;
                if order_book_state_series.len() == ORDER_BOOK_QUEUE_SIZE {
                    create_input_image(&order_book_state_series).await;
                }
            });
        }
        Ok(())
    });
    web_socket
        .connect(&format!("{}@depth", SYMBOL.to_lowercase()))
        .expect("Cannot connect to ws streams");
    if let Err(e) = web_socket.event_loop(&KEEP_RUNNING) {
        {
            eprintln!("Error: {}", e);
        }
    }
}

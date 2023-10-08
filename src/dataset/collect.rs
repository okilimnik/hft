use crate::gcp;
use binance::api::*;
use binance::market::*;
use binance::model::DepthOrderBookEvent;
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use image::ImageBuffer;
use itertools::Itertools;
use lazy_static::lazy_static;
use merge_hashmap::Merge;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task;
use tungstenite::Message;

use crate::dataset::order_book::OrderBookState;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 10;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
static IMAGES_COUNT: AtomicUsize = AtomicUsize::new(0);
const BTC_TRADING_AMOUNT: f64 = 0.02f64;

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
    static ref ORDER_BOOK: tokio::sync::Mutex<VecDeque<OrderBookState>> =
        tokio::sync::Mutex::new(VecDeque::new());
}

async fn calc_new_state(event: DepthOrderBookEvent) {
    let mut order_book_state_series = ORDER_BOOK.lock().await;
    if order_book_state_series.is_empty() {
        let new_order_book = task::spawn_blocking(move || {
            MARKET
                .get_custom_depth(SYMBOL, 5000)
                .expect("Failed to get initial order book.")
        })
        .await
        .unwrap();
        order_book_state_series.push_back(OrderBookState::from1(new_order_book));
    };
    let mut new_order_book = order_book_state_series.back().unwrap().clone();
    if event.final_update_id > new_order_book.last_update_id {
        new_order_book.merge(OrderBookState::from2(event));
        new_order_book.filter();
        order_book_state_series.push_back(new_order_book);
    }
    if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_state_series.pop_front();
    }
    if order_book_state_series.len() == ORDER_BOOK_QUEUE_SIZE {
        create_input_image(&order_book_state_series).await;
    }
}

// we define price change levels by step of 0.025%
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

fn get_max_price(states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)>) -> f64 {
    states
        .iter()
        .map(|x| x.0.iter().chain(x.1.iter()).max_by_key(|x| x.0).unwrap().0)
        .max()
        .unwrap()
        .parse()
        .unwrap()
}

fn get_min_price(states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)>) -> f64 {
    states
        .iter()
        .map(|x| x.0.iter().chain(x.1.iter()).min_by_key(|x| x.0).unwrap().0)
        .min()
        .unwrap()
        .parse()
        .unwrap()
}

fn get_acc_quantity(
    prices: Vec<&(f64, f64, bool)>,
    price_level_shift: f64,
    min_price: f64,
    level: usize,
) -> f64 {
    prices.iter().fold(0f64, |acc, price| -> f64 {
        let price_level = ((price.0 - min_price) / price_level_shift).floor() as u32;
        if price_level == level as u32 {
            acc + price.1
        } else {
            acc
        }
    })
}

fn denoise(
    states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)>,
    max_price: f64,
    min_price: f64,
) -> (Vec<HashMap<u32, f64>>, Vec<HashMap<u32, f64>>) {
    let shift = (max_price - min_price) / HISTORY_SIZE as f64;
    let ask_qts: Vec<HashMap<u32, f64>> = states
        .iter()
        .map(|x| {
            x.0.iter().fold(
                HashMap::new(),
                |mut acc: HashMap<u32, f64>, a: &(String, f64)| -> HashMap<u32, f64> {
                    let level = ((a.0.parse::<f64>().unwrap() - min_price) / shift).round() as u32;
                    *acc.entry(level).or_insert(0f64) += a.1;
                    acc
                },
            )
        })
        .collect();
    let bid_qts: Vec<HashMap<u32, f64>> = states
        .iter()
        .map(|x| {
            x.1.iter().fold(
                HashMap::new(),
                |mut acc: HashMap<u32, f64>, a: &(String, f64)| -> HashMap<u32, f64> {
                    let level = ((a.0.parse::<f64>().unwrap() - min_price) / shift).round() as u32;
                    *acc.entry(level).or_insert(0f64) += a.1;
                    acc
                },
            )
        })
        .collect();
    let filtered_states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)> = states
        .iter()
        .enumerate()
        .map(|(idx, s)| -> (Vec<(String, f64)>, Vec<(String, f64)>) {
            let filtered_asks = s
                .0
                .iter()
                .filter(|a| -> bool {
                    let level = ((a.0.parse::<f64>().unwrap() - min_price) / shift).round() as u32;
                    let qty = ask_qts[idx].get(&level).unwrap();
                    *qty > 0f64
                })
                .map(|x| x.to_owned())
                .collect();
            let filtered_bids = s
                .1
                .iter()
                .filter(|a| -> bool {
                    let level = ((a.0.parse::<f64>().unwrap() - min_price) / shift).round() as u32;
                    let qty = ask_qts[idx].get(&level).unwrap();
                    *qty > 0f64
                })
                .map(|x| x.to_owned())
                .collect();
            (filtered_asks, filtered_bids)
        })
        .collect();
    let new_max_price = get_max_price(filtered_states) + 0.000001;
    let new_min_price = get_min_price(filtered_states);
    if max_price != new_max_price || min_price != new_min_price {
        denoise(filtered_states, new_max_price, new_min_price)
    } else {
        (ask_qts, bid_qts)
    }
}

fn get_current_price(state: &OrderBookState) -> String {
    state
        .bids
        .iter()
        .filter_map(|x| -> Option<&String> {
            if *x.1 >= BTC_TRADING_AMOUNT {
                Some(x.0)
            } else {
                None
            }
        })
        .min()
        .unwrap()
        .to_owned()
}

fn get_next_price(states: Vec<&OrderBookState>) -> String {
    states
        .iter()
        .map(|x| x.asks)
        .concat()
        .iter()
        .filter_map(|x| -> Option<&String> {
            if *x.1 >= BTC_TRADING_AMOUNT {
                Some(x.0)
            } else {
                None
            }
        })
        .max()
        .unwrap()
        .to_owned()
}

async fn create_input_image(states: &VecDeque<OrderBookState>) {
    let iterable_states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)> = states
        .iter()
        .map(|s| {
            (
                s.asks.into_iter().map(|a| (a.0, a.1)).collect(),
                s.bids.into_iter().map(|b| (b.0, b.1)).collect(),
            )
        })
        .collect();
    let max_price = get_max_price(iterable_states) + 0.000001;
    let min_price = get_min_price(iterable_states);
    let (ask_qts, bid_qts) = denoise(iterable_states, max_price, min_price);

    let qty_iter = ask_qts
        .iter()
        .chain(bid_qts.iter())
        .map(|x| x.values().collect_vec())
        .concat();
    let max_qty = qty_iter
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
        .to_owned()
        .to_owned()
        + 0.000001;
    let min_qty = qty_iter
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
        .to_owned()
        .to_owned();
    let quantity_level_shift = max_qty / 255f64;
    let img = ImageBuffer::from_fn(HISTORY_SIZE as u32, HISTORY_SIZE as u32, |x, y| {
        let r = (ask_qts[x as usize].get(&y).unwrap() / quantity_level_shift).round() as u8;
        let g = (bid_qts[x as usize].get(&y).unwrap() / quantity_level_shift).round() as u8;
        image::Rgb([r, g, 0])
    });
    let state = states.get(HISTORY_SIZE - 1).unwrap();
    let next_states = states
        .range(HISTORY_SIZE..ORDER_BOOK_QUEUE_SIZE)
        .collect_vec();
    let current_price = get_current_price(state);
    let next_price = get_next_price(next_states);
    println!("next price: {}", next_price);
    if let Some(label) = calc_label(current_price, next_price) {
        let images_count = IMAGES_COUNT.fetch_add(1, Ordering::SeqCst);
        fs::create_dir_all("./dataset").unwrap();
        let filename = format!("{}_{}.png", label, images_count);
        let filepath = format!("./dataset/{}", filename);
        tokio::spawn(async move {
            if let Err(e) = img.save(filepath.clone()) {
                eprintln!("Cannot save dataset image on disk: {}", e);
            }
            if let Err(e) = gcp::create_file(filename.clone(), filepath.clone()).await {
                eprintln!("Cannot save dataset file in cloud: {}", e);
            }
            if let Err(e) = fs::remove_file(filepath) {
                eprintln!("Cannot remove dataset file after saving in cloud: {}", e);
            }
        });
    }
}

pub async fn from_binance_data() {
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        if let WebsocketEvent::DepthOrderBook(event) = event {
            tokio::spawn(async move {
                calc_new_state(event).await;
            });
        }
        Ok(())
    });
    web_socket
        .connect(&format!("{}@depth", SYMBOL.to_lowercase()))
        .expect("Cannot connect to ws streams");
    loop {
        if let Some(ref mut socket) = web_socket.socket {
            let message = socket.0.read_message().unwrap();
            match message {
                Message::Text(msg) => {
                    if let Err(e) = web_socket.handle_msg(&msg) {
                        println!("Error on handling stream message: {:?}", e);
                    }
                }
                Message::Ping(_) => {
                    socket.0.write_message(Message::Pong(vec![])).unwrap();
                }
                Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => (),
                Message::Close(e) => println!("Disconnected {:?}", e),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[test]
fn test_image_content() {}

use crate::gcp;
use binance::api::*;
use binance::market::*;
use binance::model::DepthOrderBookEvent;
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use image::imageops::rotate180_in_place;
use image::io::Reader as ImageReader;
use image::ImageBuffer;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{debug, error, info};
use merge_hashmap::Merge;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

// TODO:
// 1. rotate bearish so input is bullish;

use crate::dataset::order_book::OrderBookState;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 10;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
static IMAGES_COUNT: AtomicUsize = AtomicUsize::new(0);
const BTC_TRADING_AMOUNT: f64 = 0.02f64;
const DENOISING_QTY_THRESHOLD: f64 = 1.0;
const LEVEL_PRICE_CHANGE_PERCENT: f64 = 0.04;

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
    static ref ORDER_BOOK: Mutex<VecDeque<OrderBookState>> = Mutex::new(VecDeque::new());
}

fn calc_new_state(event: DepthOrderBookEvent) {
    let mut order_book_state_series = ORDER_BOOK.lock().unwrap();
    if order_book_state_series.is_empty() {
        let new_order_book = MARKET
            .get_custom_depth(SYMBOL, 5000)
            .expect("Failed to get initial order book.");
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
        let snapshot = order_book_state_series
            .iter()
            .map(|x| x.to_owned())
            .collect_vec();
        rayon::spawn(move || {
            create_input_image(snapshot);
        });
    }
}

fn calc_change_level(current_price: f64, next_price: f64) -> i32 {
    let shift = next_price - current_price;
    let mut change_level =
        ((shift.abs() * 100.0) / (current_price * LEVEL_PRICE_CHANGE_PERCENT)).floor() as i32;
    if change_level > 4 {
        change_level = 4;
    }
    change_level
}

// we define price change levels by step of LEVEL_PRICE_CHANGE_PERCENT
// al levels that expands more than 4 level become 4 level
// we don't want create images if price change is 0
fn calc_label(bullish: (f64, f64), bearish: (f64, f64)) -> Option<String> {
    let mut bullish_change_level = 0;
    let mut bearish_change_level = 0;
    if bullish.1 > bullish.0 {
        bullish_change_level = calc_change_level(bullish.0, bullish.1);
    }
    if bearish.1 < bearish.0 {
        bearish_change_level = calc_change_level(bearish.0, bearish.1);
    }
    if bullish_change_level == bearish_change_level {
        bullish_change_level = 0;
        bearish_change_level = 0;
    }
    let bullish_label: String = (1..5).fold("".to_string(), |acc: String, i: i32| -> String {
        if i == bullish_change_level {
            format!("{acc}{}", "1")
        } else {
            format!("{acc}{}", "0")
        }
    });
    let bearish_label: String =
        (1..5)
            .rev()
            .fold("".to_string(), |acc: String, i: i32| -> String {
                if i == bearish_change_level {
                    format!("{acc}{}", "1")
                } else {
                    format!("{acc}{}", "0")
                }
            });
    let mut label = "".to_string();
    label.push_str(&bearish_label);
    label.push_str(&bullish_label);
    if label == "00000000" {
        None
    } else {
        Some(label)
    }
}

fn get_max_price(states: &[(Vec<(String, f64)>, Vec<(String, f64)>)]) -> f64 {
    states
        .iter()
        .map(|x| {
            x.0.iter()
                .chain(x.1.iter())
                .max_by_key(|x| x.0.to_owned())
                .unwrap()
                .0
                .to_owned()
        })
        .max()
        .unwrap()
        .parse()
        .unwrap()
}

fn get_min_price(states: &[(Vec<(String, f64)>, Vec<(String, f64)>)]) -> f64 {
    states
        .iter()
        .map(|x| {
            x.0.iter()
                .chain(x.1.iter())
                .min_by_key(|x| x.0.to_owned())
                .unwrap()
                .0
                .to_owned()
        })
        .min()
        .unwrap()
        .parse()
        .unwrap()
}

fn denoise(
    states: &[(Vec<(String, f64)>, Vec<(String, f64)>)],
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
                    *qty > DENOISING_QTY_THRESHOLD
                })
                .map(|x| x.to_owned())
                .collect();
            let filtered_bids = s
                .1
                .iter()
                .filter(|a| -> bool {
                    let level = ((a.0.parse::<f64>().unwrap() - min_price) / shift).round() as u32;
                    let qty = bid_qts[idx].get(&level).unwrap();
                    *qty > DENOISING_QTY_THRESHOLD
                })
                .map(|x| x.to_owned())
                .collect();
            (filtered_asks, filtered_bids)
        })
        .collect();
    let new_max_price = get_max_price(&filtered_states) + 0.000001;
    let new_min_price = get_min_price(&filtered_states);
    if max_price != new_max_price || min_price != new_min_price {
        denoise(&filtered_states, new_max_price, new_min_price)
    } else {
        (ask_qts, bid_qts)
    }
}

fn get_current_buy_price(state: &OrderBookState) -> String {
    state
        .asks
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

fn get_current_sell_price(state: &OrderBookState) -> String {
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
        .max()
        .unwrap()
        .to_owned()
}

fn get_next_sell_price(states: &[OrderBookState]) -> String {
    states
        .iter()
        .map(|x| x.bids.to_owned())
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

fn get_next_buy_price(states: &[OrderBookState]) -> String {
    states
        .iter()
        .map(|x| x.asks.to_owned())
        .concat()
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

fn rotate_image(filename: String, new_filename: String) {
    let mut img = ImageReader::open(format!("./{}", filename))
        .unwrap()
        .decode()
        .unwrap();
    rotate180_in_place(&mut img);
    img.save(format!("./{}", new_filename)).unwrap();
}

fn create_input_image(states: Vec<OrderBookState>) {
    let iterable_states: Vec<(Vec<(String, f64)>, Vec<(String, f64)>)> = states
        .iter()
        .map(|s| {
            (
                s.asks
                    .iter()
                    .map(|a| (a.0.to_owned(), a.1.to_owned()))
                    .collect(),
                s.bids
                    .iter()
                    .map(|b| (b.0.to_owned(), b.1.to_owned()))
                    .collect(),
            )
        })
        .collect();
    let max_price = get_max_price(&iterable_states) + 0.000001;
    let min_price = get_min_price(&iterable_states);
    let (ask_qts, bid_qts) = denoise(&iterable_states, max_price, min_price);

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
        let mut r = 0;
        if let Some(ask_qty) = ask_qts[x as usize].get(&y) {
            r = ((ask_qty - min_qty) / quantity_level_shift).round() as u8;
        }
        let mut g = 0;
        if let Some(bid_qty) = bid_qts[x as usize].get(&y) {
            g = ((bid_qty - min_qty) / quantity_level_shift).round() as u8;
        }
        image::Rgb([r, g, 0])
    });
    let state = states.get(HISTORY_SIZE - 1).unwrap();
    let next_states = states.get(HISTORY_SIZE..ORDER_BOOK_QUEUE_SIZE).unwrap();
    let current_sell_price = get_current_sell_price(state).parse().unwrap();
    let next_buy_price = get_next_buy_price(next_states).parse().unwrap();
    let current_buy_price = get_current_buy_price(state).parse().unwrap();
    let next_sell_price = get_next_sell_price(next_states).parse().unwrap();

    if let Some(label) = calc_label(
        (current_buy_price, next_sell_price),
        (current_sell_price, next_buy_price),
    ) {
        let images_count = IMAGES_COUNT.fetch_add(1, Ordering::SeqCst);
        fs::create_dir_all("./dataset").unwrap();
        let filename = format!("{}_{}.png", label, images_count);
        let filepath = format!("./dataset/{}", filename);
        if let Err(e) = img.save(filepath.clone()) {
            error!("Cannot save dataset image on disk: {}", e);
        };
        if let Err(e) = gcp::create_file(filename.clone(), filepath.clone()) {
            error!("Cannot save dataset file in cloud: {}", e);
        }
        if let Err(e) = fs::remove_file(filepath) {
            error!("Cannot remove dataset file after saving in cloud: {}", e);
        }
    }
}

pub fn from_binance_data() {
    let keep_running = AtomicBool::new(true);
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
        if let WebsocketEvent::DepthOrderBook(event) = event {
            calc_new_state(event);
        }
        Ok(())
    });
    web_socket
        .connect(&format!("{}@depth", SYMBOL.to_lowercase()))
        .expect("Cannot connect to ws streams");
    if let Err(e) = web_socket.event_loop(&keep_running) {
        error!("Error: {:?}", e);
    };
}

#[test]
fn test_image_content() {}

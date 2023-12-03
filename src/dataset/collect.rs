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
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

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
    static ref ORDER_BOOK: Arc<Mutex<VecDeque<OrderBookState>>> =
        Arc::new(Mutex::new(VecDeque::new()));
}

fn create_input(snapshot: &[OrderBookState]) {
    let start = Instant::now();
    let result = create_input_image(snapshot);
    let duration = start.elapsed();
    debug!("create_input_image took: {:?}", duration.as_millis());
    if let Some((filename, filepath)) = result {
        let _ = gcp::create_file(filename.clone(), filepath.clone());
        fs::remove_file(filepath).unwrap();
    }
}

fn calc_new_state(event: DepthOrderBookEvent) {
    let mut order_book_state_series = ORDER_BOOK.lock().unwrap();
    if order_book_state_series.is_empty() {
        let new_order_book = MARKET
            .get_custom_depth(SYMBOL, 5000)
            .expect("Failed to get initial order book.");
        order_book_state_series.push_back(OrderBookState::from(
            new_order_book.last_update_id,
            new_order_book.bids,
            new_order_book.asks,
        ));
    };
    let mut new_order_book = order_book_state_series
        .back()
        .expect("Cannot get last item in states")
        .clone();
    if event.final_update_id > new_order_book.last_update_id {
        new_order_book.merge(OrderBookState::from(
            event.final_update_id,
            event.bids,
            event.asks,
        ));
        order_book_state_series.push_back(new_order_book);
    }
    if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_state_series.pop_front();
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

fn get_max_price(states: &[OrderBookState]) -> f64 {
    states
        .iter()
        .map(|x| {
            x.bids
                .iter()
                .chain(x.asks.iter())
                .max_by_key(|x| x.0.to_owned())
                .expect("Cannot iter through state to get max price")
                .0
                .to_owned()
        })
        .max()
        .expect("Cannot get max price string")
        .parse()
        .expect("Cannot convert max string price into f64")
}

fn get_min_price(states: &[OrderBookState]) -> f64 {
    states
        .iter()
        .map(|x| {
            x.bids
                .iter()
                .chain(x.asks.iter())
                .min_by_key(|x| x.0.to_owned())
                .expect("Cannot iter through state to get min price")
                .0
                .to_owned()
        })
        .min()
        .expect("Cannot get min price string")
        .parse()
        .expect("Cannot convert min string price into f6")
}

fn denoise(
    states: &[OrderBookState],
    max_price: f64,
    min_price: f64,
) -> (Vec<FxHashMap<u32, f64>>, Vec<FxHashMap<u32, f64>>) {
    let shift = (max_price - min_price) / HISTORY_SIZE as f64;
    let bids_qts: Vec<FxHashMap<u32, f64>> = states
        .iter()
        .map(|x| {
            x.bids.iter().fold(
                FxHashMap::default(),
                |mut acc: FxHashMap<u32, f64>, a: (&String, &f64)| -> FxHashMap<u32, f64> {
                    let level = ((a
                        .0
                        .parse::<f64>()
                        .expect("Cannot parse string price in denoise fn for ask_qts")
                        - min_price)
                        / shift)
                        .round() as u32;
                    *acc.entry(level).or_insert(0f64) += a.1;
                    acc
                },
            )
        })
        .collect();
    let asks_qts: Vec<FxHashMap<u32, f64>> = states
        .iter()
        .map(|x| {
            x.asks.iter().fold(
                FxHashMap::default(),
                |mut acc: FxHashMap<u32, f64>, a: (&String, &f64)| -> FxHashMap<u32, f64> {
                    let level = ((a
                        .0
                        .parse::<f64>()
                        .expect("Cannot parse string price in denoise fn for bid_qts")
                        - min_price)
                        / shift)
                        .round() as u32;
                    *acc.entry(level).or_insert(0f64) += a.1;
                    acc
                },
            )
        })
        .collect();
    let filtered_states: Vec<OrderBookState> = states
        .iter()
        .enumerate()
        .map(|(idx, s)| -> OrderBookState {
            let filtered_bids = s
                .bids
                .iter()
                .filter(|b| -> bool {
                    let level = ((b
                        .0
                        .parse::<f64>()
                        .expect("Cannot parse string price in filtered_asks")
                        - min_price)
                        / shift)
                        .round() as u32;
                    let qty = bids_qts[idx]
                        .get(&level)
                        .expect("The level is not present in ask_qts");
                    *qty > DENOISING_QTY_THRESHOLD
                })
                .map(|x| (x.0.to_owned(), x.1.to_owned()))
                .collect();
            let filtered_asks = s
                .asks
                .iter()
                .filter(|a| -> bool {
                    let level = ((a
                        .0
                        .parse::<f64>()
                        .expect("Cannot parse string price in filtered_bids")
                        - min_price)
                        / shift)
                        .round() as u32;
                    let qty = asks_qts[idx]
                        .get(&level)
                        .expect("The level is not present in bid_qts");
                    *qty > DENOISING_QTY_THRESHOLD
                })
                .map(|x| (x.0.to_owned(), x.1.to_owned()))
                .collect();
            OrderBookState {
                bids: filtered_bids,
                asks: filtered_asks,
                last_update_id: 0,
            }
        })
        .collect();
    let new_max_price = get_max_price(&filtered_states) + 0.000001;
    let new_min_price = get_min_price(&filtered_states);

    if format!("{:.6}", max_price) != format!("{:.6}", new_max_price)
        || format!("{:.6}", min_price) != format!("{:.6}", new_min_price)
    {
        denoise(&filtered_states, new_max_price, new_min_price)
    } else {
        (bids_qts.clone(), asks_qts.clone())
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

fn create_input_image(states: &[OrderBookState]) -> Option<(String, String)> {
    let max_price = get_max_price(states) + 0.000001;
    let min_price = get_min_price(states);

    let (ask_qts, bid_qts) = denoise(states, max_price, min_price);

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
        Some((filename, filepath))
    } else {
        None
    }
}

fn run_consumer() {
    thread::spawn(|| loop {
        let order_book_state_series = ORDER_BOOK.lock().unwrap();
        if order_book_state_series.len() == ORDER_BOOK_QUEUE_SIZE {
            let snapshot = order_book_state_series
                .iter()
                .map(|x| x.to_owned())
                .collect_vec();
        }
    });
}

fn run_producer() {
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

pub fn from_binance_data() {
    // run_consumer();
    run_producer();
}

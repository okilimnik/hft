use binance::api::Binance;
use binance::market::Market;
use binance::model::DepthOrderBookEvent;
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::error;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use crate::dataset::order_book::OrderBookState;
use crate::ui;
use crate::utils;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 10;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
const SELL_SHIFT: i64 = 40;
const MIN_BIDS_IN_LINE: usize = 2; // how many states the price greater than price + SELL_SHIFT gets in line
const QUANTITY_THRESHOLD: f64 = 0.01;

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
    static ref ORDER_BOOK: Arc<Mutex<VecDeque<OrderBookState>>> =
        Arc::new(Mutex::new(VecDeque::new()));
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
        // update UI
        ui::set_prices(&new_order_book);
        order_book_state_series.push_back(new_order_book);
    }
    if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_state_series.pop_front();
    }
}

fn to_svm_row(label: i64, order_book: &OrderBook) -> String {
    row.into_iter().fold(format!("{}", label), |acc, e| {
        acc + " " + &e.0 + ":" + &format!("{:.5}", e.1)
    })
}

fn get_price_key(i: usize, price_for_level_10: i64) -> i64 {
    let shift = i - 10;
    price_for_level_10 + (shift * 10) as i64
}

fn create_input() {
    let states = ORDER_BOOK.lock().unwrap();
    let input_series = states.iter().take(HISTORY_SIZE);
    let label_series = states.iter().rev().take(PREDICTION_HEAD).rev();
    let buy_price = input_series
        .last()
        .unwrap()
        .asks
        .iter()
        .min_by_key(|a| a.0)
        .unwrap()
        .0
        .to_owned();
    let future_best_bids =
        label_series.map(|state| state.bids.iter().max_by_key(|bid| bid.0).unwrap());
    let relevant_bids_in_line = future_best_bids.fold(vec![], |mut acc, x| {
        if (x.0 - buy_price) >= SELL_SHIFT {
            acc.push(*x.0);
            acc
        } else {
            vec![]
        }
    });
    // should trade?
    let label = if relevant_bids_in_line.len() >= MIN_BIDS_IN_LINE {
        1
    } else {
        0
    };
    let price_min = input_series
        .map(|state| {
            state
                .asks
                .iter()
                .min_by_key(|x: &(&i64, &f64)| x.0)
                .unwrap()
                .0
        })
        .min()
        .unwrap()
        .to_owned();
    // at what price we would  buy
    let price_for_level_10 = input_series
        .last()
        .unwrap()
        .asks
        .iter()
        .filter(|x| *x.1 > QUANTITY_THRESHOLD)
        .min_by_key(|x| x.0)
        .unwrap()
        .0
        .to_owned();
    let price_max = input_series
        .map(|state| {
            state
                .bids
                .iter()
                .max_by_key(|x: &(&i64, &f64)| x.0)
                .unwrap()
                .0
        })
        .max()
        .unwrap()
        .to_owned();
    // input
    let svm_row = input_series
        .enumerate()
        .fold(label.to_string(), |acc, (i, state)| {
            (1..20).fold(acc, |mut acc: String, i| -> String {
                let price_key = get_price_key(i, price_for_level_10);
                let quantity = *state.bids.entry(price_key).or_insert(0f64)
                    - *state.asks.entry(price_key).or_insert(0f64);
                if quantity.abs() > QUANTITY_THRESHOLD {
                    let input_value = i.to_string() + ":" + &format!("{:.5}", quantity);
                    acc + " " + &input_value
                } else {
                    acc
                }
            })
        });
    utils::to_file("./input.svm", svm_row, true);
}

fn run_producer() {
    thread::spawn(|| {
        let keep_running = AtomicBool::new(true);
        let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
            if let WebsocketEvent::DepthOrderBook(event) = event {
                calc_new_state(event);
                create_input();
            }
            Ok(())
        });
        web_socket
            .connect(&format!("{}@depth", SYMBOL.to_lowercase()))
            .expect("Cannot connect to ws streams");
        if let Err(e) = web_socket.event_loop(&keep_running) {
            error!("Error: {:?}", e);
        };
    });
}

fn calc_label() {}

fn run_consumer() {
    let _ = ui::render();
}

pub fn from_binance_data() {
    run_producer();
    run_consumer();
}

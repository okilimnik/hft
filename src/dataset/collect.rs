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
const CLOSE_ORDER_SHIFT: i64 = 40;
const MIN_STOP_HITS_IN_LINE: usize = 2; // how many states in line reach price we need to close order at
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

fn get_price_by_index(j: usize, price_for_level_5: i64) -> i64 {
    price_for_level_5 + 10 * (j - 5) as i64
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
    let sell_price = input_series
        .last()
        .unwrap()
        .bids
        .iter()
        .max_by_key(|a| a.0)
        .unwrap()
        .0
        .to_owned();
    let future_best_bids = label_series.map(|state| {
        state
            .bids
            .iter()
            .max_by_key(|x: &(&i64, &f64)| x.0)
            .unwrap()
    });
    let future_best_asks = label_series.map(|state| state.asks.iter().min_by_key(|x| x.0).unwrap());
    let relevant_bids_in_line = future_best_bids
        .enumerate()
        .fold(vec![], |mut acc, (i, x)| {
            if (x.0 - buy_price) >= CLOSE_ORDER_SHIFT {
                acc.push((i, *x.0));
                acc
            } else {
                vec![]
            }
        });
    let relevant_asks_in_line =
        future_best_asks
            .enumerate()
            .fold(vec![], |mut acc: Vec<(usize, i64)>, (i, x)| {
                if (sell_price - x.0) >= CLOSE_ORDER_SHIFT {
                    acc.push((i, *x.0));
                    acc
                } else {
                    vec![]
                }
            });
    // default is noise
    let mut label = 0;
    if relevant_bids_in_line.len() >= MIN_STOP_HITS_IN_LINE {
        // buy signal
        label = 1;
    }
    if relevant_asks_in_line.len() >= MIN_STOP_HITS_IN_LINE
        && relevant_asks_in_line.first().unwrap().0 < relevant_bids_in_line.first().unwrap().0
    {
        // sell signal
        label = -1;
    }
    // don't create inputs if it's noise
    if label == 0 {
        return;
    }
    let price_that_matters = input_series
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
    // input
    let svm_row = input_series
        .enumerate()
        .fold(label.to_string(), |acc, (i, state)| -> String {
            (0..10).fold(acc, |acc, j| -> String {
                let quantity = *state
                    .bids
                    .entry(get_price_by_index(j, price_that_matters))
                    .or_insert(0f64)
                    - *state
                        .asks
                        .entry(get_price_by_index(j, price_that_matters))
                        .or_insert(0f64);
                if quantity >= QUANTITY_THRESHOLD {
                    acc + " " + &((i * 10) + j + 1).to_string() + ":" + &format!("{:.4}", quantity)
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

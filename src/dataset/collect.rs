use binance::api::Binance;
use binance::market::Market;
use binance::model::DepthOrderBookEvent;
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use log::error;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::dataset::order_book::OrderBookState;
use crate::gcp;
use crate::ui;
use crate::utils;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 180;
const PREDICTION_HEAD: usize = 60;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
const CLOSE_ORDER_SHIFT: i64 = 30;
const MIN_STOP_HITS_IN_LINE: usize = 2; // how many states in line reach price we need to close order at
const QUANTITY_THRESHOLD: f64 = 0.01;
const DATA_FETCH_INTERVAL: u128 = 3000;

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
}

fn get_price_by_index(j: usize, price_for_level_5: i64) -> i64 {
    price_for_level_5 + 10 * (j as i64 - 5)
}

fn calc_label(input_series: &[OrderBookState], label_series: &[OrderBookState]) -> i64 {
    let last_input = input_series.last().unwrap();
    let buy_price = last_input
        .asks
        .iter()
        .min_by_key(|a| a.0)
        .unwrap()
        .0
        .to_owned();
    let sell_price = last_input
        .bids
        .iter()
        .max_by_key(|a| a.0)
        .unwrap()
        .0
        .to_owned();
    let future_best_bids = label_series
        .iter()
        .map(|state| {
            state
                .bids
                .iter()
                .max_by_key(|x: &(&i64, &f64)| x.0)
                .unwrap()
        })
        .collect_vec();
    let future_best_asks = label_series
        .iter()
        .map(|state| state.asks.iter().min_by_key(|x| x.0).unwrap())
        .collect_vec();
    let relevant_bids_in_line =
        future_best_bids
            .iter()
            .enumerate()
            .fold(vec![], |mut acc, (i, x)| {
                if (x.0 - buy_price) >= CLOSE_ORDER_SHIFT {
                    acc.push((i, *x.0));
                }
                acc
            });
    let relevant_asks_in_line =
        future_best_asks
            .iter()
            .enumerate()
            .fold(vec![], |mut acc: Vec<(usize, i64)>, (i, x)| {
                if (sell_price - x.0) >= CLOSE_ORDER_SHIFT {
                    acc.push((i, *x.0));
                }
                acc
            });
    // default is noise
    let mut label: i64 = 0;
    if relevant_bids_in_line.len() >= MIN_STOP_HITS_IN_LINE {
        // buy signal
        label = 1;
    }
    if relevant_asks_in_line.len() >= MIN_STOP_HITS_IN_LINE
        && (relevant_bids_in_line.is_empty()
            || relevant_asks_in_line.first().unwrap().0 < relevant_bids_in_line.first().unwrap().0)
    {
        // sell signal
        label = -1;
    }
    label
}

fn create_input(input_series: Vec<OrderBookState>, label_series: Vec<OrderBookState>) {
    let label = calc_label(&input_series, &label_series);
    // update UI
    //ui::set_label(label);
    // don't create inputs if it's noise
    if label == 0 {
        return;
    }
    debug!("Label is {}", label);
    let price_that_matters = input_series
        .iter()
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
    let svm_row =
        input_series
            .iter()
            .enumerate()
            .fold(label.to_string(), |acc, (i, state)| -> String {
                (0..10).fold(acc, |acc, j| -> String {
                    let quantity = *state
                        .bids
                        .clone()
                        .entry(get_price_by_index(j, price_that_matters))
                        .or_insert(0f64)
                        - *state
                            .asks
                            .clone()
                            .entry(get_price_by_index(j, price_that_matters))
                            .or_insert(0f64);
                    if quantity >= QUANTITY_THRESHOLD {
                        acc + " "
                            + &((i * 10) + j + 1).to_string()
                            + ":"
                            + &format!("{:.4}", quantity)
                    } else {
                        acc
                    }
                })
            });
    let filename = "input.svm".to_string();
    let filepath = format!("./{}", filename);
    utils::to_file(&filepath, svm_row, true);
    gcp::create_file(filename, filepath);
}

fn run_producer() {
    let mut order_book_state_series = VecDeque::new();
    let mut t: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    loop {
        debug!("t: {}", t);
        let delta: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - t;
        debug!("delta: {}", delta);
        if delta >= DATA_FETCH_INTERVAL {
            debug!("Making binance request");
            let new_order_book = MARKET.get_custom_depth(SYMBOL, 1000).unwrap();
            order_book_state_series.push_back(OrderBookState::from(
                new_order_book.last_update_id,
                new_order_book.bids,
                new_order_book.asks,
            ));
            if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
                order_book_state_series.pop_front();
            }
            if order_book_state_series.len() == ORDER_BOOK_QUEUE_SIZE {
                let input_series = order_book_state_series
                    .iter()
                    .take(HISTORY_SIZE)
                    .cloned()
                    .collect_vec();
                let label_series: Vec<OrderBookState> = order_book_state_series
                    .iter()
                    .rev()
                    .take(PREDICTION_HEAD)
                    .rev()
                    .cloned()
                    .collect_vec();
                create_input(input_series, label_series);
            }
            t = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
        } else {
            thread::sleep(Duration::from_millis(
                (DATA_FETCH_INTERVAL - delta).try_into().unwrap(),
            ));
        }
    }
}

fn run_consumer() {
    // let _ = ui::render();
}

pub fn from_binance_data() {
    run_producer();
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::*;

    #[test]
    fn get_price_by_index_test() {
        let result = get_price_by_index(5, 20000);
        assert_eq!(result, 20000);
        let result = get_price_by_index(1, 20000);
        assert_eq!(result, 19960);
        let result = get_price_by_index(6, 20000);
        assert_eq!(result, 20010);
    }

    #[test]
    fn calc_label_test() {
        let mut input_bids = FxHashMap::default();
        input_bids.insert(20000, 0.5);
        let mut input_asks = FxHashMap::default();
        input_asks.insert(20010, 0.1);
        let input_state = OrderBookState {
            last_update_id: 1,
            bids: input_bids,
            asks: input_asks,
        };

        let mut label_bids1 = FxHashMap::default();
        label_bids1.insert(20050, 0.5);
        let mut label_asks1 = FxHashMap::default();
        label_asks1.insert(20060, 0.1);
        let label_state1 = OrderBookState {
            last_update_id: 2,
            bids: label_bids1,
            asks: label_asks1,
        };

        let mut label_bids2 = FxHashMap::default();
        label_bids2.insert(20050, 0.5);
        let mut label_asks2 = FxHashMap::default();
        label_asks2.insert(20060, 0.1);
        let label_state2 = OrderBookState {
            last_update_id: 2,
            bids: label_bids2,
            asks: label_asks2,
        };

        let result = calc_label(&[input_state], &[label_state1, label_state2]);
        assert_eq!(result, 1);
    }
}

use binance::api::Binance;
use binance::market::Market;
use binance::model::OrderBook;
use itertools::Itertools;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use log::error;
use std::collections::VecDeque;
use std::env;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::dataset::order_book::OrderBookState;
use crate::dataset::utils;
use crate::gcp;
use crate::trade;
use crate::ui;

const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 15;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;
const MIN_STOP_HITS_IN_LINE: usize = 5; // how many states in line reach price we need to close order at
const DATA_FETCH_INTERVAL_MILLIS: u128 = 1000;
const PRICE_PRECISION: i64 = 10;

lazy_static! {
    static ref MARKET: Market = Binance::new(None, None);
}

fn calc_label(input_series: &[OrderBook], label_series: &[OrderBook]) -> i64 {
    let profit_value = env::var("PROFIT").unwrap().parse().unwrap();
    let last_input = input_series.last().unwrap();
    let buy_price = last_input
        .asks
        .iter()
        .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .find_or_first(|x| true)
        .unwrap()
        .price
        .to_owned();
    let sell_price = last_input
        .bids
        .iter()
        .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .find_or_last(|x| true)
        .unwrap()
        .price
        .to_owned();
    let future_best_buy_prices = label_series
        .iter()
        .map(|state| -> f64 {
            state
                .asks
                .iter()
                .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                .find_or_first(|x| true)
                .unwrap()
                .price
                .to_owned()
        })
        .collect_vec();
    let future_best_sell_prices = label_series
        .iter()
        .map(|state| -> f64 {
            state
                .bids
                .iter()
                .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                .find_or_last(|x| true)
                .unwrap()
                .price
                .to_owned()
        })
        .collect_vec();
    let relevant_buy_prices_in_line =
        future_best_buy_prices
            .iter()
            .enumerate()
            .fold(vec![], |mut acc, (i, x)| {
                if (sell_price - x) >= profit_value {
                    acc.push((i, *x));
                }
                acc
            });
    let relevant_sell_prices_in_line =
        future_best_sell_prices
            .iter()
            .enumerate()
            .fold(vec![], |mut acc, (i, x)| {
                if (x - buy_price) >= profit_value {
                    acc.push((i, *x));
                }
                acc
            });

    // default is noise
    let mut label: i64 = 0;
    if relevant_sell_prices_in_line.len() >= MIN_STOP_HITS_IN_LINE {
        // buy signal
        label = 1;
    }
    if relevant_buy_prices_in_line.len() >= MIN_STOP_HITS_IN_LINE
        && (relevant_sell_prices_in_line.is_empty()
            || relevant_buy_prices_in_line.first().unwrap().0
                < relevant_sell_prices_in_line.first().unwrap().0)
    {
        // sell signal
        label = -1;
    }
    label
}

fn create_input(
    input_series_with_precision: Vec<OrderBookState>,
    raw_input_series: Vec<OrderBook>,
    raw_label_series: Vec<OrderBook>,
) {
    let label = calc_label(&raw_input_series, &raw_label_series);
    // update UI
    //ui::set_label(label);
    // don't create inputs if it's noise
    if label == 0 {
        return;
    }
    debug!("Label is {}", label);

    // input
    let svm_row = utils::to_svm(label, input_series_with_precision);
    let filename = "input.svm".to_string();
    let filepath = format!("./{}", filename);
    utils::to_file(&filepath, svm_row, true);
    gcp::create_file(filename, filepath);
}

fn run_producer() {
    let mut series_with_precision = VecDeque::new();
    let mut raw_series = VecDeque::new();
    let mut t: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    loop {
        let delta: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - t;
        if delta >= DATA_FETCH_INTERVAL_MILLIS {
            let new_order_book = MARKET
                .get_custom_depth(env::var("SYMBOL").unwrap(), 1000)
                .unwrap();

            raw_series.push_back(new_order_book.clone());
            if raw_series.len() > ORDER_BOOK_QUEUE_SIZE {
                raw_series.pop_front();
            }

            series_with_precision.push_back(OrderBookState::with_precision(
                new_order_book.last_update_id,
                new_order_book.bids,
                new_order_book.asks,
                PRICE_PRECISION,
            ));
            if series_with_precision.len() > ORDER_BOOK_QUEUE_SIZE {
                series_with_precision.pop_front();
            }

            if series_with_precision.len() == ORDER_BOOK_QUEUE_SIZE {
                if env::var("TRADE_AMOUNT").is_ok() {
                    let trade_series_with_precision: Vec<OrderBookState> = series_with_precision
                        .iter()
                        .rev()
                        .take(HISTORY_SIZE)
                        .rev()
                        .cloned()
                        .collect_vec();
                    let raw_trade_series: Vec<OrderBook> = raw_series
                        .iter()
                        .rev()
                        .take(HISTORY_SIZE)
                        .rev()
                        .cloned()
                        .collect_vec();
                    trade::trade(raw_trade_series, trade_series_with_precision);
                };
                if env::var("COLLECT").is_ok() {
                    let input_series_with_precision = series_with_precision
                        .iter()
                        .take(HISTORY_SIZE)
                        .cloned()
                        .collect_vec();
                    let raw_input_series =
                        raw_series.iter().take(HISTORY_SIZE).cloned().collect_vec();
                    let raw_label_series: Vec<OrderBook> = raw_series
                        .iter()
                        .rev()
                        .take(PREDICTION_HEAD)
                        .rev()
                        .cloned()
                        .collect_vec();
                    create_input(
                        input_series_with_precision,
                        raw_input_series,
                        raw_label_series,
                    );
                };
            }
            t = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
        } else {
            let interval = DATA_FETCH_INTERVAL_MILLIS as i64 - delta as i64;
            if interval > 0 {
                thread::sleep(Duration::from_millis(interval.try_into().unwrap()));
            }
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

    /*  #[test]
    fn get_price_by_index_test() {
        let result = get_price_by_index(5, 20000);
        assert_eq!(result, 20000);
        let result = get_price_by_index(1, 20000);
        assert_eq!(result, 19960);
        let result = get_price_by_index(6, 20000);
        assert_eq!(result, 20010);
    }*/

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

use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use binance::{
    account::{Account, OrderSide, OrderType, TimeInForce},
    api::Binance,
    config::Config,
    model::{OrderBook, Transaction},
};
use image::open;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use std::thread;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::{
    dataset::{order_book::OrderBookState, utils},
    lightgbm,
};

const WAIT_ORDER_FILL: u128 = 10000;

lazy_static! {
    static ref ACCOUNT: Account = Binance::new_with_config(
        env::var("BINANCE_API_KEY").ok(),
        env::var("BINANCE_SECRET").ok(),
        &Config::default().set_rest_api_endpoint("https://api1.binance.com")
    );
    static ref TRADING: AtomicBool = AtomicBool::new(false);
}

fn open_stop_profit_order(
    main_order_id: u64,
    symbol: &str,
    qty: f64,
    price: f64,
    order_side: OrderSide,
) {
    let t: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    loop {
        thread::sleep(Duration::from_millis(1000));
        let delta: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - t;
        let stop_profit_side = if matches!(order_side, OrderSide::Buy) {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if delta >= WAIT_ORDER_FILL {
            match ACCOUNT.cancel_order(symbol, main_order_id) {
                Ok(_) => debug!("Canceled opening an order"),
                Err(_) => {
                    open_stop_profit_order(main_order_id, symbol, qty, price, stop_profit_side)
                }
            }
            break;
        } else {
            match ACCOUNT.order_status(symbol, main_order_id) {
                Ok(order) => {
                    debug!("Order status: {}", order.status);
                    if order.status == "FILLED" {
                        debug!("started trading");
                        match ACCOUNT.custom_order(
                            symbol,
                            qty,
                            price,
                            None,
                            stop_profit_side,
                            OrderType::Limit,
                            TimeInForce::GTC,
                            None,
                        ) {
                            Ok(_) => {
                                debug!("opened a STOP PROFIT order");
                                break;
                            }
                            Err(e) => debug!("opening stop profit error {:?}", e),
                        }
                    }
                }
                Err(_) => debug!("Cannot query order status"),
            }
        }
    }
}

fn open_order(price: f64, stop_profit: f64, order_side: OrderSide) {
    let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
    let symbol: String = env::var("SYMBOL").unwrap();
    let stop_profit_side = if matches!(order_side, OrderSide::Buy) {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    match ACCOUNT.custom_order(
        &symbol,
        trade_amount,
        price,
        None,
        order_side,
        OrderType::Limit,
        TimeInForce::GTC,
        None,
    ) {
        Ok(tx) => open_stop_profit_order(
            tx.order_id,
            &symbol,
            trade_amount,
            stop_profit,
            stop_profit_side,
        ),
        Err(e) => debug!("start trading error {:?}", e),
    };
}

fn buy(last_state: OrderBook) {
    let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
    let best_buy_price = last_state
        .asks
        .iter()
        .filter(|x| x.price >= trade_amount)
        .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .find_or_first(|x| true)
        .unwrap()
        .price
        .to_owned();
    let profit_value: f64 = env::var("PROFIT").unwrap().parse().unwrap();
    let order_side = OrderSide::Buy;
    let stop_profit = best_buy_price + profit_value;
    open_order(best_buy_price, stop_profit, order_side);
}

fn sell(last_state: OrderBook) {
    let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
    let best_sell_price = last_state
        .bids
        .iter()
        .filter(|x| x.qty >= trade_amount)
        .sorted_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .find_or_last(|x| true)
        .unwrap()
        .price
        .to_owned();
    let profit_value: f64 = env::var("PROFIT").unwrap().parse().unwrap();
    let order_side = OrderSide::Sell;
    let stop_profit = best_sell_price - profit_value;
    open_order(best_sell_price, stop_profit, order_side);
}

pub fn trade(raw_series: Vec<OrderBook>, series_with_precision: Vec<OrderBookState>) {
    let symbol: String = env::var("SYMBOL").unwrap();
    if TRADING.load(Ordering::SeqCst) || !ACCOUNT.get_open_orders(symbol).unwrap().is_empty() {
        return;
    }
    TRADING.store(true, Ordering::SeqCst);
    thread::spawn(move || {
        let prediction_threshold: f64 = env::var("PREDICTION_THRESHOLD").unwrap().parse().unwrap();

        let svm_row = utils::to_svm(1, series_with_precision);
        let (sell_prediction, buy_prediction) = lightgbm::predict(svm_row);
        debug!("Prediction for sell is {:.2}", sell_prediction);
        debug!("Prediction for buy is {:.2}", buy_prediction);

        if buy_prediction >= prediction_threshold {
            buy(raw_series.last().unwrap().clone());
        } else if sell_prediction >= prediction_threshold {
            sell(raw_series.last().unwrap().clone());
        }
        TRADING.store(false, Ordering::SeqCst);
    });

    // ACCOUNT.cancel_all_open_orders("WTCETH")
    // account.order_status("WTCETH", order_id)
    // account.get_balance("KNC")
    // account.cancel_order("WTCETH", order_id)
}

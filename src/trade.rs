use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use binance::{
    account::{Account, OrderSide, OrderType, TimeInForce},
    api::Binance,
    model::Transaction,
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

const WAIT_ORDER_FILL: u128 = 15000;

lazy_static! {
    static ref ACCOUNT: Account = Binance::new(
        env::var("BINANCE_API_KEY").ok(),
        env::var("BINANCE_SECRET").ok()
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

fn buy(price: f64) {
    let profit_value: f64 = env::var("PROFIT").unwrap().parse().unwrap();
    let order_side = OrderSide::Buy;
    let stop_profit = price + profit_value;
    open_order(price, stop_profit, order_side);
}

fn sell(price: f64) {
    let profit_value: f64 = env::var("PROFIT").unwrap().parse().unwrap();
    let order_side = OrderSide::Sell;
    let stop_profit = price - profit_value;
    open_order(price, stop_profit, order_side);
}

pub fn trade(data: Vec<OrderBookState>) {
    let symbol: String = env::var("SYMBOL").unwrap();
    if TRADING.load(Ordering::SeqCst) || !ACCOUNT.get_open_orders(symbol).unwrap().is_empty() {
        return;
    }
    TRADING.store(true, Ordering::SeqCst);
    thread::spawn(|| {
        let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
        let prediction_threshold: f64 = env::var("PREDICTION_THRESHOLD").unwrap().parse().unwrap();

        let last_state = data.last().unwrap().clone();
        let svm_row = utils::to_svm(1, data);
        let prediction = lightgbm::predict(svm_row);
        debug!("Prediction is {:.2}", prediction);

        let best_buy_price = last_state
            .asks
            .iter()
            .filter(|x| *x.1 >= trade_amount)
            .min_by_key(|x| x.0)
            .unwrap()
            .0
            .to_owned() as f64;
        let best_sell_price = last_state
            .bids
            .iter()
            .filter(|x| *x.1 >= trade_amount)
            .max_by_key(|x| x.0)
            .unwrap()
            .0
            .to_owned() as f64;
        if prediction >= prediction_threshold {
            buy(best_buy_price);
        }
        if prediction <= 1f64 - prediction_threshold {
            sell(best_sell_price);
        }
        TRADING.store(false, Ordering::SeqCst);
    });

    // ACCOUNT.cancel_all_open_orders("WTCETH")
    // account.order_status("WTCETH", order_id)
    // account.get_balance("KNC")
    // account.cancel_order("WTCETH", order_id)
}

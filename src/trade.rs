use std::env;

use binance::{
    account::{Account, OrderSide, OrderType, TimeInForce},
    api::Binance,
};
use image::open;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;

use crate::{
    dataset::{order_book::OrderBookState, utils},
    lightgbm,
};

lazy_static! {
    static ref ACCOUNT: Account = Binance::new(
        env::var("BINANCE_API_KEY").ok(),
        env::var("BINANCE_SECRET").ok()
    );
}

pub fn trade(data: Vec<OrderBookState>) {
    let last_state = data.last().unwrap().clone();
    let svm_row = utils::to_svm(0, data);
    let prediction = lightgbm::predict(svm_row);
    debug!("Prediction is {:.3}", prediction);

    let symbol = env::var("SYMBOL").unwrap();
    let quantity: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
    let profit_value: f64 = env::var("PROFIT").unwrap().parse().unwrap();
    let opened_orders = ACCOUNT.get_open_orders(&symbol).unwrap();
    if opened_orders.is_empty() {
        let best_buy_price = last_state
            .asks
            .iter()
            .filter(|x| *x.1 >= quantity)
            .min_by_key(|x| x.0)
            .unwrap()
            .0
            .to_owned() as f64;
        let best_sell_price = last_state
            .bids
            .iter()
            .filter(|x| *x.1 >= quantity)
            .max_by_key(|x| x.0)
            .unwrap()
            .0
            .to_owned() as f64;
        ACCOUNT
            .custom_order(
                &symbol,
                quantity,
                best_buy_price,
                None,
                OrderSide::Buy,
                OrderType::Limit,
                TimeInForce::GTC,
                None,
            )
            .unwrap();
        ACCOUNT
            .custom_order(
                &symbol,
                quantity,
                best_buy_price + profit_value,
                None,
                OrderSide::Sell,
                OrderType::Limit,
                TimeInForce::GTC,
                None,
            )
            .unwrap();
    }

    // ACCOUNT.cancel_all_open_orders("WTCETH")
    // account.order_status("WTCETH", order_id)
    // account.get_balance("KNC")
    // account.cancel_order("WTCETH", order_id)
}

fn open_order() {}

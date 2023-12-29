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

fn open_order(price: f64, stop_profit: f64, order_side: OrderSide) {
    let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();
    let symbol: String = env::var("SYMBOL").unwrap();
    let stop_profit_side = if matches!(order_side, OrderSide::Buy) {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    ACCOUNT
        .custom_order(
            &symbol,
            trade_amount,
            price,
            None,
            order_side,
            OrderType::Limit,
            TimeInForce::GTC,
            None,
        )
        .unwrap();
    ACCOUNT
        .custom_order(
            &symbol,
            trade_amount,
            stop_profit,
            None,
            stop_profit_side,
            OrderType::Limit,
            TimeInForce::GTC,
            None,
        )
        .unwrap();
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
    let trade_amount: f64 = env::var("TRADE_AMOUNT").unwrap().parse().unwrap();

    let opened_orders = ACCOUNT.get_open_orders(symbol).unwrap();
    if opened_orders.is_empty() {
        let last_state = data.last().unwrap().clone();
        let svm_row = utils::to_svm(0, data);
        let prediction = lightgbm::predict(svm_row);
        debug!("Prediction is {:.3}", prediction);

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
        if prediction >= 0.9 {
            buy(best_buy_price);
        } else {
            sell(best_sell_price);
        }
    }

    // ACCOUNT.cancel_all_open_orders("WTCETH")
    // account.order_status("WTCETH", order_id)
    // account.get_balance("KNC")
    // account.cancel_order("WTCETH", order_id)
}

use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use slint::{Timer, TimerMode};
use std::sync::{Arc, Mutex};

use crate::dataset::order_book::OrderBookState;
slint::include_modules!();

lazy_static! {
    static ref ASK: Arc<Mutex<Vec<Price>>> = Arc::new(Mutex::new(vec![]));
    static ref BID: Arc<Mutex<Vec<Price>>> = Arc::new(Mutex::new(vec![]));
}

pub fn render() -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    let timer = Timer::default();
    {
        let ui_handler = ui.as_weak();
        timer.start(
            TimerMode::Repeated,
            std::time::Duration::from_millis(500),
            move || {
                let ask = slint::ModelRc::new(slint::VecModel::from(ASK.lock().unwrap().clone()));
                ui_handler.unwrap().set_ask(ask);
                let bid = slint::ModelRc::new(slint::VecModel::from(BID.lock().unwrap().clone()));
                ui_handler.unwrap().set_bid(bid);
            },
        );
    }
    ui.run()
}

pub fn set_prices(order_book: &OrderBookState) {
    let ask = order_book
        .asks
        .iter()
        .sorted_by_key(|a| a.0)
        .take(20)
        .map(|x| Price {
            price: x.0.to_string().into(),
            quantity: format!("{:.5}", x.1).into(),
        })
        .collect_vec();
    let bid = order_book
        .bids
        .iter()
        .sorted_by_key(|a| a.0)
        .rev()
        .take(20)
        .map(|x| Price {
            price: x.0.to_string().into(),
            quantity: format!("{:.5}", x.1).into(),
        })
        .collect_vec();
    set_ask(ask);
    set_bid(bid);
}

fn set_bid(new_bid: Vec<Price>) {
    let mut bid = BID.lock().unwrap();
    *bid = new_bid;
}

fn set_ask(new_ask: Vec<Price>) {
    let mut ask = ASK.lock().unwrap();
    *ask = new_ask;
}

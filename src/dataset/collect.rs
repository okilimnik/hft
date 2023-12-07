use binance::api::*;
use binance::market::*;
use binance::model::DepthOrderBookEvent;
use binance::websockets::WebSockets;
use binance::websockets::WebsocketEvent;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use log::error;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crate::dataset::order_book::OrderBookState;
use crate::ui;

const SYMBOL: &str = "BTCTUSD";
const HISTORY_SIZE: usize = 60;
const PREDICTION_HEAD: usize = 10;
const ORDER_BOOK_QUEUE_SIZE: usize = HISTORY_SIZE + PREDICTION_HEAD;

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
        ui::set_prices(&new_order_book);
        order_book_state_series.push_back(new_order_book);
    }
    if order_book_state_series.len() > ORDER_BOOK_QUEUE_SIZE {
        order_book_state_series.pop_front();
    }
}

fn run_producer() {
    thread::spawn(|| {
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
    });
}

fn run_consumer() {
    let _ = ui::render();
}

pub fn from_binance_data() {
    run_producer();
    run_consumer();
}

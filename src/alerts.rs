use binance::model::Bids;
use binance::model::OrderBook;
use binance::websockets::*;
use binance::api::*;
use binance::market::*;
use std::sync::atomic::AtomicBool;
use std::io::Error;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::collections::HashSet;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;

const SYMBOL: &'static str = "BTCTUSD";

fn play_sound() {
    Command::new("afplay")
        .arg("alert.wav")
        .output()
        .expect("Couldn't play sound");
}

fn to_file(filename: &str, data: String, append: bool) -> Result<(), Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .open(format!("./{}", filename))
        .expect(&format!("Unable to write {}", filename));
    if let Err(e) = writeln!(file, "{}", data) {
        eprintln!("Couldn't write to file: {}", e);
    }
    Ok(())
}

pub fn subscribe() {
   //  let mut interesting_symbols: HashSet<String> = vec!["AKROUSDT".to_string(), "RUNEUSDT".to_string(), "HIFIUSDT".to_string(), "SUPERUSDT".to_string()].into_iter().collect();
   // let mut ignore_symbols: HashSet<String> = vec!["AKROUSDT".to_string(), "RUNEUSDT".to_string(), "HIFIUSDT".to_string(), "SUPERUSDT".to_string()].into_iter().collect();
    let keep_running = AtomicBool::new(true);
    let streams = [//String::from("!ticker_1h@arr"), 
                                format!("{}@depth@100ms", SYMBOL.to_lowercase()), 
                                format!("{}@aggTrade", SYMBOL.to_lowercase())];
    let mut symbols: HashSet<String> = vec![].into_iter().collect();
    let market: Market = Binance::new(None, None);
    let mut order_book = OrderBook {
        last_update_id: 0,
        bids: vec![],
        asks: vec![]
    };
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
	    match event {
            // 1h rolling window ticker statistics for all symbols that changed in an array.
        /*  WebsocketEvent::WindowTickerAll(events) => {
                for event in events {
                    let change: f32 = event.price_change_percent.parse().expect("Cannot parse price_change_percent");
                    let symbol = event.symbol;
                    if change >= 5.0 && !symbols.contains(&symbol) && symbol.ends_with("USDT") {
                        symbols.insert(symbol.clone());
                        play_sound();
                        to_file("alerts.txt", format!("{} - {}", symbol, change), true)?;
                    }
                    if change >= 5.0 && !symbols.contains(&symbol) && symbol.ends_with("USDT") {
                        symbols.insert(symbol.clone());
                        play_sound();
                        to_file("alerts.txt", format!("{} - {}", symbol, change), true)?;
                    }
                }
            },*/
            WebsocketEvent::DepthOrderBook(event) => { 
                if order_book.last_update_id == 0 {
                    match market.get_custom_depth(SYMBOL, 5000) {
                        Ok(answer) => {
                            order_book = answer;
                        },
                        Err(e) => println!("Error: {}", e),
                    }
                } else {
                    if event.final_update_id > order_book.last_update_id {
                        let iter = event.bids.iter();
                        for x in iter {
                            order_book.bids = order_book.bids.iter().map(|y| {
                                if x.price == y.price {
                                    Bids::new(x.price, x.qty)
                                } else {
                                    Bids::new(y.price, y.qty)
                                }
                            }).filter(|x| x.qty > 0f64).collect();
                        }
                    }
                }
                let _ = to_file("order_book.json", 
                    serde_json::to_string(&order_book).expect("Failed to stringify order book."), 
                    false);
            },
            WebsocketEvent::AggrTrades(event) => {

            },
            _ => (),
        };

        Ok(())
    });

    web_socket.connect_multiple_streams(&streams).expect("Cannot connect to ws streams");
    if let Err(e) = web_socket.event_loop(&keep_running) {
        match e {
            err => {
                eprintln!("Error: {}", err);
            }
        }
     }
}
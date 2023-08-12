use binance::websockets::*;
use std::sync::atomic::AtomicBool;

pub fn subscribe() {
    let keep_running = AtomicBool::new(true);
    let rolling_window_stats = format!("!ticker_1h@arr");
    let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
	match event {
        // 1hr rolling window ticker statistics for all symbols that changed in an array.
	    WebsocketEvent::WindowTickerAll(ticker_events) => {
	        for tick_event in ticker_events {
                let change: f32 = tick_event.price_change_percent.parse().unwrap();
                if change.abs() >= 5.0 {
                    println!("{} - {}", tick_event.symbol, change);
                }
		    }
	    },
	    _ => (),
        };

        Ok(())
    });

    web_socket.connect(&rolling_window_stats).unwrap(); // check error
    if let Err(e) = web_socket.event_loop(&keep_running) {
        match e {
            err => {
                println!("Error: {:?}", err);
            }
        }
     }
}
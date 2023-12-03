mod dataset;
mod gcp;
mod utils;
use clap::Parser;
use dotenv::dotenv;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// task name
    #[arg(short, long)]
    task: String,
}

fn main() {
    dotenv().ok();
    env_logger::init();
    let args = Args::parse();

    match args.task.as_str() {
        "collect" => dataset::collect::from_binance_data(),
        _ => print!("No task, exiting..."),
    };
}
/*
slint::slint! {
    export component MainWindow inherits Window {
        Text {
            text: "hello world";
            color: green;
        }
    }
}
*/

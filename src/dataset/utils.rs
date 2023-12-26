use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng};
use std::fs::{read_to_string, OpenOptions};
use std::io::Write;

use super::order_book::OrderBookState;

const QUANTITY_THRESHOLD: f64 = 0.01;

pub fn to_file(filepath: &str, data: String, append: bool) {
    //fs::create_dir_all("./dataset").unwrap();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .open(filepath)
        .unwrap_or_else(|_| panic!("Unable to write {}", filepath));
    if let Err(e) = writeln!(file, "{}", data) {
        eprintln!("Couldn't write to file: {}", e);
    };
}

pub fn split() {
    let mut data: Vec<String> = read_to_string("./svm_input.txt")
        .unwrap() // panic on possible file-reading errors
        .lines() // split the string into an iterator of string slices
        .map(String::from) // make each slice into a string
        .collect_vec();
    data.shuffle(&mut thread_rng());
    let count = data.len() as f64;
    data.iter()
        .take((count * 0.75) as usize)
        .map(|x| {
            to_file("./lgbm.train", x.to_owned(), true);
        })
        .collect_vec();
    data.iter()
        .rev()
        .take((count * 0.25) as usize)
        .map(|x| {
            to_file("./lgbm.test", x.to_owned(), true);
        })
        .collect_vec();
}

fn get_price_by_index(j: usize, price_for_level_5: i64) -> i64 {
    price_for_level_5 + 10 * (j as i64 - 5)
}

pub fn to_svm(label: i64, data: Vec<OrderBookState>) -> String {
    let price_that_matters = data
        .iter()
        .map(|state| {
            state
                .asks
                .iter()
                .min_by_key(|x: &(&i64, &f64)| x.0)
                .unwrap()
                .0
        })
        .min()
        .unwrap()
        .to_owned();
    let label_str = if label == 0 {
        "".to_string()
    } else {
        label.to_string()
    };
    data.iter()
        .enumerate()
        .fold(label_str, |acc, (i, state)| -> String {
            (0..10).fold(acc, |acc, j| -> String {
                let quantity = *state
                    .bids
                    .clone()
                    .entry(get_price_by_index(j, price_that_matters))
                    .or_insert(0f64)
                    - *state
                        .asks
                        .clone()
                        .entry(get_price_by_index(j, price_that_matters))
                        .or_insert(0f64);
                let start_str = if acc.len() == 0 { acc } else { acc + " " };
                if quantity >= QUANTITY_THRESHOLD {
                    start_str + &((i * 10) + j + 1).to_string() + ":" + &format!("{:.4}", quantity)
                } else {
                    start_str
                }
            })
        })
}

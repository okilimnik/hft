use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng};
use std::fs::read_to_string;

use crate::utils;

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
            utils::to_file("./lgbm.train", x.to_owned(), true);
        })
        .collect_vec();
    data.iter()
        .rev()
        .take((count * 0.25) as usize)
        .map(|x| {
            utils::to_file("./lgbm.test", x.to_owned(), true);
        })
        .collect_vec();
}

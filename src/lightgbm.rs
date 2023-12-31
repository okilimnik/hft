use log::debug;

use crate::dataset::utils::to_file;
use std::{
    fs::{self, read_to_string},
    process::Command,
};

pub fn train() {
    let output = Command::new("lightgbm")
        .arg("config=lgbm.train.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

pub fn predict(data: String) -> f64 {
    let _ = fs::remove_file("./lgbm.predict");
    let _ = fs::remove_file("./lgbm.prediction");
    to_file("./lgbm.predict", data, false);
    let output = Command::new("lightgbm")
        .arg("config=lgbm.predict.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    let string_result = read_to_string("./lgbm.prediction")
        .unwrap()
        .trim()
        .to_string();
    string_result.parse().unwrap()
}

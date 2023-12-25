use std::{fs::read_to_string, process::Command};

use crate::utils::to_file;

pub fn train() {
    let output = Command::new("lightgbm")
        .arg("config=lgbm.train.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

pub fn predict(data: String) -> f64 {
    to_file("./lgbm.predict.txt", data, false);
    let output = Command::new("lightgbm")
        .arg("config=lgbm.predict.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    read_to_string("./lgbm.prediction.txt")
        .unwrap()
        .parse()
        .unwrap()
}

use std::process::Command;

pub fn train() {
    let output = Command::new("lightgbm")
        .arg("config=lgbm.train.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

pub fn predict() {
    let output = Command::new("lightgbm")
        .arg("config=lgbm.predict.conf")
        .output()
        .expect("Failed to execute command");
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

// TODO: dasdasdsad

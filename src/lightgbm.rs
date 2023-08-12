use std::process::Command;

pub fn train() {
    let output = Command::new("lightgbm")
        .arg("config=train.conf")
        .output()
        .expect("Failed to execute command");
    println!("{:?}", output.stdout);
}

pub fn predict() {
    let output = Command::new("lightgbm")
        .arg("config=predict.conf")
        .output()
        .expect("Failed to execute command");
    println!("{:?}", output.stdout);
}

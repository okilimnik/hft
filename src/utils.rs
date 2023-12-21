use std::fs::{self, OpenOptions};
use std::io::Write;

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

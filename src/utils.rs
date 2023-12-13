use std::fs::{self, OpenOptions};
use std::io::Write;

pub fn to_file(filename: &str, data: String, append: bool) {
    fs::create_dir_all("./dataset").unwrap();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .open(format!("./{}", filename))
        .unwrap_or_else(|_| panic!("Unable to write {}", filename));
    if let Err(e) = writeln!(file, "{}", data) {
        eprintln!("Couldn't write to file: {}", e);
    };
}

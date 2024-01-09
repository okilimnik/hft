use cloud_storage::Client;
use lazy_static::lazy_static;
use std::{fs::File, io::Read};
use tokio::runtime::Runtime;

lazy_static! {
    static ref STORAGE: Client = Client::default();
    static ref RUNTIME: Runtime = tokio::runtime::Runtime::new().unwrap();
}

pub fn create_file(filename: String, filepath: String) {
    let mut bytes: Vec<u8> = Vec::new();
    for byte in File::open(&filepath).unwrap().bytes() {
        bytes.push(byte.unwrap())
    }
    let object = STORAGE.object();
    let create_object = object.create("neusa-lgbm", bytes, &filename, "text/plain");
    if let Err(e) = RUNTIME.block_on(create_object) {
        eprintln!("Couldn't write to cloud storage: {}", e);
    };
}

use cloud_storage::{Client, Error};
use lazy_static::lazy_static;
use std::{fs::File, io::Read};
use tokio::sync;

lazy_static! {
    static ref STORAGE: sync::Mutex<Client> = sync::Mutex::new(Client::default());
}

pub async fn create_file(filename: String, filepath: String) -> Result<(), Error> {
    let client = STORAGE.lock().await;
    let mut bytes: Vec<u8> = Vec::new();
    for byte in File::open(&filepath)?.bytes() {
        bytes.push(byte?)
    }
    let mut prefixed_path = "order_book_images/".to_string();
    prefixed_path.push_str(&filename);
    let _ = client
        .object()
        .create("neusa-datasets", bytes, &prefixed_path, "image/png");
    Ok(())
}

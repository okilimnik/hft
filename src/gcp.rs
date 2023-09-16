use cloud_storage::{Client, Error};
use lazy_static::lazy_static;
use std::{fs::File, io::Read, sync::Mutex};
use tokio::runtime::Runtime;
use tokio::sync;

lazy_static! {
    static ref STORAGE: sync::Mutex<Client> = sync::Mutex::new(Client::default());
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    );
}

pub fn create_file(filename: String) -> Result<(), Error> {
    let runtime = RUNTIME.lock().unwrap();
    let client = runtime.block_on(STORAGE.lock());
    let mut bytes: Vec<u8> = Vec::new();
    for byte in File::open(&filename)?.bytes() {
        bytes.push(byte?)
    }
    let _ = runtime.block_on(
        client
            .object()
            .create("dataset", bytes, &filename, "image/png"),
    );
    Ok(())
}

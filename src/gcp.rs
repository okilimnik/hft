use cloud_storage::{object::ObjectList, Client, Error, ListRequest, Object};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{collections::HashMap, fs::File, io::Read};
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
        .create("neusa-datasets", bytes, &prefixed_path, "image/png")
        .await
        .unwrap();
    Ok(())
}

pub async fn list_files_by_categories() -> HashMap<String, Vec<String>> {
    STORAGE
        .lock()
        .await
        .object()
        .list("neusa-datasets", ListRequest::default())
        .await
        .unwrap()
        .try_fold(HashMap::new(), |acc, list| async move {
            Ok(list
                .items
                .iter()
                .fold(acc, |mut inner_acc, file| -> HashMap<String, Vec<String>> {
                    let category = file.name.split_once('_').unwrap().0;
                    let new_val = inner_acc.get_mut(category).unwrap();
                    new_val.push(file.name.clone());
                    inner_acc
                }))
        })
        .await
        .unwrap()
}

use cloud_storage::{object::ObjectList, Client, Error, ListRequest, Object};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{collections::HashMap, fs::File, io::Read, sync::Mutex};

lazy_static! {
    static ref STORAGE: Mutex<Client> = Mutex::new(Client::default());
}

pub fn create_file(filename: String, filepath: String) -> Result<(), Error> {
    let client = STORAGE.lock().unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    for byte in File::open(&filepath)?.bytes() {
        bytes.push(byte?)
    }
    let mut prefixed_path = "order_book_images/".to_string();
    prefixed_path.push_str(&filename);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let object = client.object();
    rt.block_on(async {
        let _ = object
            .create("neusa-datasets", bytes, &prefixed_path, "image/png")
            .await
            .unwrap();
    });
    Ok(())
}

/*
pub fn list_files_by_categories() -> HashMap<String, Vec<String>> {
    STORAGE
        .lock()
        .unwrap()
        .object()
        .list("neusa-datasets", ListRequest::default())
        .await
        .unwrap()
        .try_fold(HashMap::new(), |acc, list| async move {
            Ok(list
                .items
                .iter()
                .fold(acc, |mut inner_acc, file| -> HashMap<String, Vec<String>> {
                    let filename = file.name.replace("order_book_images/", "");
                    let category = filename.split_once('_').unwrap().0;
                    let mut new_val: Vec<String> = match inner_acc.get_mut(category) {
                        Some(v) => v.to_owned(),
                        None => vec![],
                    };
                    new_val.push(file.name.clone());
                    inner_acc.insert(category.to_string(), new_val);
                    inner_acc
                }))
        })
        .await
        .unwrap()
}

*/

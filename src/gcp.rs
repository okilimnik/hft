use cloud_storage::{object::ObjectList, Client, Error, ListRequest, Object};
use futures::executor::block_on;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::time::Instant;
use std::{collections::HashMap, fs::File, io::Read};

lazy_static! {
    static ref STORAGE: Client = Client::default();
}

pub fn create_file(filename: String, filepath: String) -> Result<(), Error> {
    let mut bytes: Vec<u8> = Vec::new();
    for byte in File::open(&filepath)?.bytes() {
        bytes.push(byte?)
    }
    let mut prefixed_path = format!("order_book8/{:?}/", Instant::now().elapsed().as_millis());
    prefixed_path.push_str(&filename);
    let object = STORAGE.object();
    let create_object = object.create("neusa-datasets", bytes, &prefixed_path, "image/png");
    let _ = block_on(create_object);
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

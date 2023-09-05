use binance::model::OrderBook;
use firestore::FirestoreDb;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Mutex;

struct Connection {
    db: Vec<FirestoreDb>,
}

impl Connection {
    fn new() -> Connection {
        Connection { db: Vec::new() }
    }
}

const DATASETS_COLLECTION_NAME: &str = "datasets";
lazy_static! {
    static ref FIRESTORE: Mutex<Connection> = Mutex::new(Connection::new());
}

pub async fn init() {
    let db = FirestoreDb::new(env::var("PROJECT_ID").unwrap())
        .await
        .unwrap();
    let mut conn = FIRESTORE
        .lock()
        .map_err(|_| "Failed to acquire MutexGuard on FIRESTORE")
        .unwrap();
    conn.db.push(db);
}

fn get_db() -> FirestoreDb {
    FIRESTORE
        .lock()
        .map_err(|_| "Failed to acquire MutexGuard on FIRESTORE")
        .unwrap()
        .db
        .first()
        .unwrap()
        .to_owned()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Dataset {
    price: f64,
    order_book: OrderBook,
}

pub async fn insert(price: f64, order_book: OrderBook) {
    let _: Dataset = get_db()
        .fluent()
        .insert()
        .into(DATASETS_COLLECTION_NAME)
        .document_id(order_book.last_update_id.to_string())
        .object(&Dataset { price, order_book })
        .execute()
        .await
        .unwrap();
}

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

const DATASETS_COLLECTION_NAME: &str = "OrderBook";
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
    svm: String,
}

#[tokio::main]
pub async fn insert(svm: String) {
    let _: Dataset = get_db()
        .fluent()
        .insert()
        .into(DATASETS_COLLECTION_NAME)
        .generate_document_id()
        .object(&Dataset { svm })
        .execute()
        .await
        .unwrap();
}

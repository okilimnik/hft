use std::fs;

use crate::gcp;

pub async fn balance_categories() {
    let cats = gcp::list_files_by_categories().await;
}

use crate::db;
use actix_files as fs;
use actix_web::{App, HttpServer};

#[actix_web::main]
pub async fn start() -> std::io::Result<()> {
    std::fs::create_dir_all("./datasets").unwrap();
    db::init().await;
    HttpServer::new(|| {
        App::new().service(
            fs::Files::new("/datasets", "./datasets")
                .show_files_listing()
                .use_last_modified(true),
        )
    })
    .bind(("0.0.0.0", 80))?
    .run()
    .await
}

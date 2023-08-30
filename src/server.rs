use actix_files as fs;
use actix_web::{App, HttpServer};

#[actix_web::main]
pub async fn start() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new().service(
            fs::Files::new("/datasets", "./datasets")
                .show_files_listing()
                .use_last_modified(true),
        )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}

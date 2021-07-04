mod log;

use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use log::{Log, LogExtend, Record};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Offset {
    value: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotFound {
    message: String,
}

#[get("/")]
async fn consume(log: web::Data<Log>, req: web::Json<Offset>) -> HttpResponse {
    match log.read(req.0.value) {
        Ok(record) => HttpResponse::Ok().json(record),
        Err(message) => HttpResponse::NotFound().json(NotFound { message }),
    }
}

#[post("/")]
async fn produce(log: web::Data<Log>, req: web::Json<Record>) -> HttpResponse {
    let offset = log.append(req.0);
    HttpResponse::Ok().json(Offset { value: offset })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let records = web::Data::new(Log::create_empty());
    HttpServer::new(move || {
        App::new()
            .app_data(records.clone())
            .service(consume)
            .service(produce)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

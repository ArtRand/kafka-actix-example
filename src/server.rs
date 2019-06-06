#[macro_use]
extern crate log;
extern crate redis;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use actix::Addr;
use actix_web::middleware::Logger;
use actix_web::{web, App, Error as ActixError, HttpRequest, HttpResponse, HttpServer};
use env_logger;
use futures::future::ok;
use futures::Future;

use kafka::IngestConsumer;

use crate::cache::{GetCounts, RedisCache};
use crate::kafka::StartConsuming;
use actix_web::error::ErrorBadRequest;
use actix_web::web::Query;
use std::process::exit;
use std::sync::Arc;

mod cache;
mod kafka;

#[derive(Deserialize, Debug)]
struct CountsQuery {
    topic: String,
    n: Option<i32>,
}

#[derive(Clone)]
struct State {
    cache: Addr<RedisCache>,
}

fn health(_req: HttpRequest) -> impl Future<Item = HttpResponse, Error = ActixError> {
    let res = HttpResponse::Ok().json(json!("healthy"));
    ok(res)
}

type ValidatingResponse = Result<Box<Future<Item = HttpResponse, Error = ActixError>>, ActixError>;

fn counts(req: HttpRequest) -> ValidatingResponse {
    match Query::<CountsQuery>::from_query(req.query_string()) {
        Ok(counts_query) => {
            let state: &Arc<State> = req.app_data().expect("app data");
            let addr = state.cache.clone();
            let msg = GetCounts::new(counts_query.topic.as_str(), counts_query.n);
            let counts = addr
                .send(msg)
                .map_err(|mailbox_error| mailbox_error.into())
                .and_then(|result| match result {
                    Ok(counts) => {
                        let serialized = serde_json::to_value(&counts).unwrap();
                        ok(HttpResponse::Ok().json(serialized))
                    }
                    Err(e) => {
                        let js = json!({"error": e.to_string()});
                        ok(HttpResponse::ServiceUnavailable().json(js))
                    }
                });
            Ok(Box::new(counts))
        }
        Err(query_payload_error) => {
            let msg = format!("failed to parse {:?}", query_payload_error);
            let resp = ErrorBadRequest(msg);
            Err(resp)
        }
    }
}

fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("USAGE wordcount-server <topic-name> <topic_name> ...");
        exit(1);
    }
    let topics = &args[1..];

    let system = actix::System::new("word-count");
    let consumer =
        IngestConsumer::make("127.0.0.1:9092".into(), topics).expect("failed to make consumer");
    let cache = RedisCache::make("redis://127.0.0.1").expect("redis");
    consumer.do_send(StartConsuming {
        storage: cache.clone(),
    });

    let app_state = Arc::new(State { cache });

    HttpServer::new(move || {
        App::new()
            .data(app_state.clone())
            .wrap(Logger::default())
            .route("/health", web::get().to_async(health))
            .route("/counts", web::get().to_async(counts))
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .start();

    info!("starting");
    let _ = system.run();
    info!("exiting");
}

use log::{info, warn};

use actix_web::web::Data;
use actix_web::{middleware::Logger, web, App, HttpRequest, HttpResponse, HttpServer};
use redis::Client as RedisClient;
use serde_json::json;
use std::process::exit;
use std::sync::Arc;

use wordcount::cache::RedisCache;
use wordcount::kafka::{kafka_seed, IngestConsumer};

#[derive(serde::Deserialize, Debug)]
struct CountsQuery {
    topic: String,
    n: Option<i64>,
}

#[derive(Clone)]
struct State {
    cache: RedisCache,
}

async fn health(_req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().json(json!("healthy"))
}

async fn counts(app_state: Data<State>, counts_query: web::Query<CountsQuery>) -> HttpResponse {
    let topic = counts_query.0.topic;
    let top_n = counts_query.0.n.unwrap_or_else(|| -1);

    match app_state.cache.get_counts(&topic, top_n).await {
        Ok(token_counts) => {
            let serialized = serde_json::to_value(token_counts).unwrap();
            HttpResponse::Ok().json(serialized)
        }
        Err(redis_error) => {
            let err = json!({
                "redis-error": redis_error.to_string()
            });
            HttpResponse::InternalServerError().json(err)
        }
    }
}

#[actix_rt::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("USAGE wordcount-server <topic-name> <topic_name> ...");
        exit(1);
    }
    let topics = &args[1..];
    info!("listening on topics {:?}", &topics);

    let redis_host = std::env::var("REDIS_HOST").unwrap_or_else(|_| {
        let redis_host = "redis://localhost:6379".to_string();
        warn!("using default redis connection, {}", redis_host);
        redis_host
    });

    let redis_connection =
        RedisClient::open(redis_host.as_str()).expect("failed to make redis client");
    let connection = Arc::new(redis_connection);
    let redis_cache = RedisCache::new(connection);

    let kafka_host = kafka_seed();
    let ingest_consumer = IngestConsumer::new(kafka_host, topics, redis_cache.clone())
        .expect("failed to make ingest consumer");

    actix_rt::spawn(async move { ingest_consumer.run().await });

    let app_state = State { cache: redis_cache };

    let _ = HttpServer::new(move || {
        App::new()
            .data(app_state.clone())
            .wrap(Logger::default())
            .route("/health", web::get().to(health))
            .route("/counts", web::get().to(counts))
    })
    .workers(2)
    .bind("localhost:8080".to_string())
    .unwrap()
    .run()
    .await;
}

#[macro_use]
extern crate log;

use log::error;
use std::io::{self, BufRead};
use std::process::exit;

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use wordcount::kafka::kafka_seed;

fn kafka_producer(brokers: &str) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
}

async fn publish_line_to_topic(topic: String, line: String, producer: FutureProducer) {
    let rec = FutureRecord::to(topic.as_str())
        .payload(line.as_str())
        .key("");
    match producer.clone().send(rec, 0).await {
        Ok(Ok(_)) => {}
        Ok(Err((kakfa_error, _))) => error!(
            "{}",
            format!("failed to publish to kafka, {}", kakfa_error.to_string())
        ),
        Err(cancelled_err) => error!("{}", format!("cancelled, {}", cancelled_err.to_string())),
    }
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() != 2 {
        eprintln!("USAGE wordcount-client <topic-name>");
        exit(1);
    }

    let topic = &args[1];
    info!("producing to topic {}", topic);

    let kafka_host = kafka_seed();
    let producer = kafka_producer(&kafka_host).expect("failed to create kafka producer");

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(l) => {
                tokio::spawn(publish_line_to_topic(topic.clone(), l, producer.clone()));
            }
            Err(_) => error!("huh?"),
        }
    }
}

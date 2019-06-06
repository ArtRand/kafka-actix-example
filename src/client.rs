#[macro_use]
extern crate log;
use std::io::{self, BufRead};
use std::process::exit;

use futures::future::{ok, Future};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::runtime::Runtime;

fn kafka_producer(brokers: &str) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
}

fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() != 2 {
        eprintln!("USAGE wordcount-client <topic-name>");
        exit(1);
    }

    let topic = &args[1];
    info!("producing to topic {}", topic);

    let mut runtime = Runtime::new().expect("failed to get tokio runtime");
    let producer = kafka_producer("127.0.0.1:9092").expect("failed to create kafka producer");

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(l) => {
                let rec = FutureRecord::to(topic.as_str()).payload(&l).key(""); // could make this a random UUID to get better partitioning behavior
                let produce_task = producer.send(rec, 0).then(move |_| {
                    info!("sent: {}", l);
                    ok(())
                });
                runtime.spawn(produce_task);
            }
            Err(_) => error!("huh?"),
        }
    }
    runtime.shutdown_on_idle().wait().expect("failed");
}

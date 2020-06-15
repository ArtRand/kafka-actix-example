use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;

use crate::cache::RedisCache;
use futures::StreamExt;
use rdkafka::message::{BorrowedMessage, Message as KafkaMessage};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MessagePayload(String);

impl MessagePayload {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// generic way to turn a borrowed message into a (wrapped) string
impl<'a> From<&'a BorrowedMessage<'a>> for MessagePayload {
    fn from(bm: &'a BorrowedMessage) -> Self {
        match bm.payload_view::<str>() {
            Some(Ok(s)) => MessagePayload(String::from(s)),
            Some(Err(e)) => MessagePayload(format!("{:?}", e)),
            None => MessagePayload(String::from("")),
        }
    }
}

pub struct IngestConsumer {
    consumer: StreamConsumer,
    redis_cache: RedisCache,
}

impl IngestConsumer {
    pub fn new(
        kafka_seed: String,
        topics: &[String],
        redis_cache: RedisCache,
    ) -> Result<Self, KafkaError> {
        let consumer = new_consumer(kafka_seed, topics)?;
        Ok(IngestConsumer {
            consumer,
            redis_cache,
        })
    }

    fn process_message(borrowed_message: BorrowedMessage) -> HashMap<String, usize> {
        let message_payload = MessagePayload::from(&borrowed_message);
        let splitted = message_payload
            .as_str()
            .split_whitespace()
            .map(|token| token.to_string())
            .collect::<Vec<String>>();
        let mut counts: HashMap<String, usize> = HashMap::new();
        for token in splitted {
            if let Some(count) = counts.get_mut(token.as_str()) {
                *count += 1;
            } else {
                counts.insert(token, 1);
            }
        }
        counts
    }

    pub async fn run(&self) {
        let mut stream = self.consumer.start_with(Duration::from_millis(50), false);
        loop {
            match stream.next().await {
                Some(Ok(borrowed_message)) => {
                    let topic_name = borrowed_message.topic().to_owned();
                    let counts = Self::process_message(borrowed_message);
                    match self.redis_cache.persist_counts(topic_name, counts).await {
                        Ok(()) => {}
                        Err(redis_err) => error!(
                            "{}",
                            &format!("failed to persist to redis, {}", redis_err.to_string())
                        ),
                    }
                }
                Some(Err(kafka_error)) => match kafka_error {
                    KafkaError::PartitionEOF(partition) => {
                        info!("at end of partition {:?}", partition);
                    }
                    _ => error!(
                        "{}",
                        &format!("errors from kafka, {}", kafka_error.to_string())
                    ),
                },
                None => {}
            }
        }
    }
}

pub fn kafka_seed() -> String {
    std::env::var("KAFKA_SEED").unwrap_or_else(|_| {
        let kafka_seed = "127.0.0.1:9092".to_string();
        warn!("using default kafka seed, {}", kafka_seed);
        kafka_seed
    })
}

fn new_consumer(brokers: String, topics: &[String]) -> Result<StreamConsumer, KafkaError> {
    let msg = topics.join(" ");
    info!("subscribing to topics {}", msg);
    let stream_consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "test-group")
        .set("bootstrap.servers", &brokers)
        .set("auto.offset.reset", "latest")
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    let topics = topics
        .iter()
        .map(|topic| topic.as_str())
        .collect::<Vec<&str>>();
    stream_consumer.subscribe(topics.as_slice())?;
    Ok(stream_consumer)
}

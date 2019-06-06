use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message};
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;

use crate::cache::{Persist, RedisCache};
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

pub struct StartConsuming {
    pub storage: Addr<RedisCache>,
}

impl Message for StartConsuming {
    type Result = ();
}

pub struct IngestConsumer {
    consumer: BaseConsumer,
}

impl IngestConsumer {
    pub fn make(brokers: String, topics: &[String]) -> Result<Addr<Self>, KafkaError> {
        let consumer = new_consumer(brokers, topics)?;
        let this = IngestConsumer { consumer };
        Ok(this.start())
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
}

impl Actor for IngestConsumer {
    type Context = Context<Self>;
}

impl Handler<StartConsuming> for IngestConsumer {
    type Result = ();

    fn handle(
        &mut self,
        msg: StartConsuming,
        ctx: &mut Self::Context,
    ) -> <Self as Handler<StartConsuming>>::Result {
        info!("ingest consumer asked to start");
        let poll_frequency = Duration::from_millis(50);
        ctx.run_interval(poll_frequency, move |this, _this_context| {
            let mut at_end = false;
            while !at_end {
                match this.consumer.poll(Duration::from_secs(0)) {
                    Some(Ok(borrowed_message)) => {
                        let topic_name = borrowed_message.topic().to_owned();
                        let counts = Self::process_message(borrowed_message);
                        let redis = msg.storage.clone();
                        redis.do_send(Persist::make(counts, topic_name))
                    }
                    _ => at_end = true,
                }
            }
        });
        info!("polling started");
    }
}

fn new_consumer(brokers: String, topics: &[String]) -> Result<BaseConsumer, KafkaError> {
    let msg = topics.join(" ");
    info!("subscribing to topics {}", msg);
    let base_consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", "test-group")
        .set("bootstrap.servers", &brokers)
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    let topics = topics
        .iter()
        .map(|topic| topic.as_str())
        .collect::<Vec<&str>>();
    base_consumer.subscribe(topics.as_slice())?;
    Ok(base_consumer)
}

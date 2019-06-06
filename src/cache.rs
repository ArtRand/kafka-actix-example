use std::collections::HashMap;

use actix::{Actor, Addr, Arbiter, Context, Handler, Message, ResponseFuture};
use futures::future::Future;
use futures::future::{err, ok};
use redis::{Client, RedisError};

pub struct RedisCache {
    client: Client,
}

impl RedisCache {
    pub fn make(redis_host: &str) -> Result<Addr<Self>, RedisError> {
        let client = Client::open(redis_host)?;
        let this = RedisCache { client };
        Ok(this.start())
    }

    fn group_counts(counts: Vec<String>) -> HashMap<String, i32> {
        counts
            .chunks(2)
            .map(|chunk| {
                let token = chunk.get(0).unwrap().to_owned();
                let count = {
                    let s = chunk.get(1).unwrap();
                    s.parse::<i32>().unwrap()
                };
                (token, count)
            })
            .collect::<HashMap<String, i32>>()
    }
}

impl Actor for RedisCache {
    type Context = Context<Self>;
}

pub struct Persist {
    topic: String,
    counts: HashMap<String, usize>,
}

impl Persist {
    pub fn make(counts: HashMap<String, usize>, topic: String) -> Self {
        Persist { counts, topic }
    }
}

impl Message for Persist {
    type Result = ();
}

impl Handler<Persist> for RedisCache {
    type Result = ();

    fn handle(&mut self, msg: Persist, _ctx: &mut Self::Context) -> Self::Result {
        for (token, count) in msg.counts {
            debug!("persisting {}/{:?}", &token, &count);
            let inner_topic = msg.topic.clone();
            let task = self
                .client
                .get_async_connection()
                .and_then(move |conn| {
                    redis::cmd("ZINCRBY")
                        .arg(inner_topic)
                        .arg(count as i32)
                        .arg(token)
                        .query_async::<_, i32>(conn)
                })
                .then(|result| match result {
                    Ok((_, count)) => ok(info!("increased to {:?}", count)),
                    Err(redis_error) => err(error!(
                        "there was a problem {:?}",
                        redis_error.to_string()
                    )),
                });
            Arbiter::spawn(task);
        }
    }
}

pub struct GetCounts {
    topic: String,
    n: Option<i32>,
}

impl GetCounts {
    pub fn new(topic_name: &str, top_n: Option<i32>) -> GetCounts {
        let n = top_n.map(|i| i - 1);
        GetCounts {
            topic: topic_name.into(),
            n,
        }
    }
}

impl Message for GetCounts {
    type Result = Result<HashMap<String, i32>, RedisError>;
}

impl Handler<GetCounts> for RedisCache {
    type Result = ResponseFuture<HashMap<String, i32>, RedisError>;

    fn handle(&mut self, msg: GetCounts, _ctx: &mut Self::Context) -> Self::Result {
        Box::new(
            self.client
                .get_async_connection()
                .and_then(|conn| {
                    let top_n = if msg.n.is_none() {
                        "-1".to_string()
                    } else {
                        format!("{:?}", msg.n.unwrap())
                    };
                    redis::cmd("ZREVRANGE")
                        .arg(msg.topic)
                        .arg("0")
                        .arg(top_n.as_str())
                        .arg("withscores")
                        .query_async::<_, Vec<String>>(conn)
                })
                .then(|result| match result {
                    Ok((_, top_n)) => ok(RedisCache::group_counts(top_n)),
                    Err(redis_err) => err(redis_err),
                }),
        )
    }
}

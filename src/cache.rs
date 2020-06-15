use std::collections::HashMap;

use redis::{Client as RedisClient, RedisError};
use std::sync::Arc;

#[derive(Clone)]
pub struct RedisCache {
    client: Arc<RedisClient>,
}

impl RedisCache {
    pub fn new(connection: Arc<RedisClient>) -> Self {
        Self { client: connection }
    }

    pub async fn get_counts(
        &self,
        topic: &str,
        top_n: i64,
    ) -> Result<HashMap<String, i64>, RedisError> {
        let mut conn = self.client.get_async_connection().await?;
        let token_counts = redis::cmd("ZREVRANGE")
            .arg(topic)
            .arg("0")
            .arg(&format!("{:?}", top_n))
            .arg("withscores")
            .query_async::<_, Vec<String>>(&mut conn)
            .await?;
        Ok(Self::group_counts(token_counts))
    }

    pub async fn persist_counts(
        &self,
        topic: String,
        counts: HashMap<String, usize>,
    ) -> Result<(), RedisError> {
        let mut conn = self.client.get_async_connection().await?;
        for (token, count) in counts {
            redis::cmd("ZINCRBY")
                .arg(&topic)
                .arg(count as i32)
                .arg(token)
                .query_async::<_, i64>(&mut conn)
                .await?;
        }
        Ok(())
    }

    pub fn group_counts(counts: Vec<String>) -> HashMap<String, i64> {
        counts
            .chunks(2)
            .map(|chunk| {
                let token = chunk.get(0).unwrap().to_owned();
                let count = {
                    let s = chunk.get(1).unwrap();
                    s.parse::<i64>().unwrap()
                };
                (token, count)
            })
            .collect::<HashMap<String, i64>>()
    }
}

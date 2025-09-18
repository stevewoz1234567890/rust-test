use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
};

type City = String;
type Temperature = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<City, Temperature>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>>;
}

pub struct StreamCache {
    results: Arc<Mutex<HashMap<String, u64>>>,
}

impl StreamCache {
    pub fn new(api: impl Api + Clone) -> Self {
        let instance = Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        };
        instance.update_in_background(api);
        instance
    }

    pub fn get(&self, key: &str) -> Option<u64> {
        let results = self.results.lock().expect("poisoned");
        results.get(key).copied()
    }

    pub fn update_in_background(&self, api: impl Api + Clone) {
        let results1 = Arc::clone(&self.results);
        let results2 = Arc::clone(&self.results);
        let api_clone = api.clone();

        // Spawn a background task to handle the subscription stream first
        // This ensures we get real-time updates immediately
        tokio::spawn(async move {
            let mut stream = api.subscribe().await;
            while let Some(update) = stream.next().await {
                match update {
                    Ok((city, temperature)) => {
                        let mut cache = results1.lock().expect("poisoned");
                        cache.insert(city, temperature);
                    }
                    Err(e) => {
                        eprintln!("Error in subscription stream: {}", e);
                    }
                }
            }
        });

        // Spawn another background task to handle the initial fetch
        // This will only populate cities that haven't been updated by the stream yet
        tokio::spawn(async move {
            match api_clone.fetch().await {
                Ok(initial_data) => {
                    let mut cache = results2.lock().expect("poisoned");
                    // Only insert cities that don't already exist in the cache
                    // This ensures subscribe updates take precedence
                    for (city, temperature) in initial_data {
                        cache.entry(city).or_insert(temperature);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to fetch initial data: {}", e);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time;

    use futures::{future, stream::select, FutureExt, StreamExt};
    use maplit::hashmap;

    use super::*;

    #[derive(Default, Clone)]
    struct TestApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for TestApi {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            // fetch is slow an may get delayed until after we receive the first updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Berlin".to_string() => 29,
                "Paris".to_string() => 31,
            })
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let results = vec![
                Ok(("London".to_string(), 27)),
                Ok(("Paris".to_string(), 32)),
            ];
            select(
                futures::stream::iter(results),
                async {
                    self.signal.notify_one();
                    future::pending().await
                }
                .into_stream(),
            )
            .boxed()
        }
    }
    #[tokio::test]
    async fn works() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(29));
        assert_eq!(cache.get("London"), Some(27));
        assert_eq!(cache.get("Paris"), Some(32));
    }

    #[tokio::test]
    async fn test_subscribe_updates_override_fetch() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        // Paris should have the value from subscribe (32), not fetch (31)
        assert_eq!(cache.get("Paris"), Some(32));
    }

    #[tokio::test]
    async fn test_nonexistent_city() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        // Non-existent city should return None
        assert_eq!(cache.get("Tokyo"), None);
    }

    #[tokio::test]
    async fn test_empty_cache_initially() {
        let cache = StreamCache::new(TestApi::default());

        // Before any updates, cache should be empty
        assert_eq!(cache.get("Berlin"), None);
        assert_eq!(cache.get("London"), None);
        assert_eq!(cache.get("Paris"), None);
    }
}

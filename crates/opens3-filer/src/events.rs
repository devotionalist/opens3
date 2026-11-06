// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize)]
pub struct FilerEvent {
    pub event: String,
    pub path: String,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<FilerEvent>,
    webhooks: Arc<parking_lot::RwLock<Vec<String>>>,
    http: reqwest::Client,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            tx,
            webhooks: Arc::new(parking_lot::RwLock::new(Vec::new())),
            http: reqwest::Client::new(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FilerEvent> {
        self.tx.subscribe()
    }

    pub fn add_webhook(&self, url: String) {
        self.webhooks.write().push(url);
    }

    pub async fn publish(&self, event: FilerEvent) {
        let _ = self.tx.send(event.clone());
        let hooks = self.webhooks.read().clone();
        for url in hooks {
            let http = self.http.clone();
            let ev = event.clone();
            tokio::spawn(async move {
                let _ = http.post(&url).json(&ev).send().await;
            });
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

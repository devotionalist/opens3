// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Filer 事件总线：本地 broadcast + 可选 webhook 推送。

use serde::Serialize;
use std::sync::Arc;
use tokio::sync::broadcast;

/// 单条 Filer 变更事件（序列化后可推送给订阅者 / webhook）
#[derive(Debug, Clone, Serialize)]
pub struct FilerEvent {
    pub event: String, // 事件类型名，如 put / delete
    pub path: String,  // 对象路径
    pub size: u64,     // 对象大小（字节）
    pub etag: String,  // 内容 ETag
}

/// 内存事件总线：broadcast 通道 + webhook URL 列表
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<FilerEvent>, // 本地订阅通道（容量见 new）
    webhooks: Arc<parking_lot::RwLock<Vec<String>>>, // 外部 webhook URL
    http: reqwest::Client,             // 异步 POST 客户端
}

impl EventBus {
    /// 创建事件总线；broadcast 缓冲 1024 条
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            tx,
            webhooks: Arc::new(parking_lot::RwLock::new(Vec::new())),
            http: reqwest::Client::new(),
        }
    }

    /// 订阅本地事件流
    pub fn subscribe(&self) -> broadcast::Receiver<FilerEvent> {
        self.tx.subscribe()
    }

    /// 注册一个 webhook URL（失败发送在 publish 时静默忽略）
    pub fn add_webhook(&self, url: String) {
        self.webhooks.write().push(url);
    }

    /// 发布事件：先本地 broadcast，再异步 POST 各 webhook
    pub async fn publish(&self, event: FilerEvent) {
        let _ = self.tx.send(event.clone()); // 无订阅者时忽略
        let hooks = self.webhooks.read().clone();
        for url in hooks {
            let http = self.http.clone();
            let ev = event.clone();
            // 每个 webhook 独立 task，避免阻塞调用方
            tokio::spawn(async move {
                let _ = http.post(&url).json(&ev).send().await;
            });
        }
    }
}

impl Default for EventBus {
    /// 同 [`EventBus::new`]
    fn default() -> Self {
        Self::new()
    }
}

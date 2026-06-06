use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::field::{Field, Visit};
use tracing::Event;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::time::iso_now;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" => Some(Self::Warn),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

impl From<&tracing::Level> for LogLevel {
    fn from(l: &tracing::Level) -> Self {
        match *l {
            tracing::Level::ERROR => Self::Error,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::INFO => Self::Info,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::TRACE => Self::Debug,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub ts: String,
    pub level: LogLevel,
    pub source: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "correlationId")]
    pub correlation_id: Option<String>,
}

pub struct LogBuffer {
    capacity: usize,
    state: Mutex<State>,
    tx: broadcast::Sender<LogEntry>,
}

struct State {
    entries: VecDeque<LogEntry>,
    total_appended: u64,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(256);
        Arc::new(Self {
            capacity,
            state: Mutex::new(State {
                entries: VecDeque::with_capacity(capacity),
                total_appended: 0,
            }),
            tx,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn append(&self, entry: LogEntry) {
        {
            let mut s = self.state.lock();
            s.entries.push_back(entry.clone());
            s.total_appended = s.total_appended.saturating_add(1);
            while s.entries.len() > self.capacity {
                s.entries.pop_front();
            }
        }
        let _ = self.tx.send(entry);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.tx.subscribe()
    }

    pub fn snapshot(&self, since: Option<&str>, level: Option<LogLevel>, limit: usize) -> Vec<LogEntry> {
        let s = self.state.lock();
        let mut iter: Box<dyn Iterator<Item = &LogEntry>> = Box::new(s.entries.iter());
        if let Some(since) = since {
            iter = Box::new(iter.filter(move |e| e.ts.as_str() > since));
        }
        if let Some(lvl) = level {
            iter = Box::new(iter.filter(move |e| e.level >= lvl));
        }
        let collected: Vec<LogEntry> = iter.cloned().collect();
        let take_from = collected.len().saturating_sub(limit);
        collected.into_iter().skip(take_from).collect()
    }

    pub fn stats(&self) -> (usize, usize, u64) {
        let s = self.state.lock();
        (self.capacity, s.entries.len(), s.total_appended)
    }
}

pub struct RingBufferLayer {
    buffer: Arc<LogBuffer>,
}

impl RingBufferLayer {
    pub fn new(buffer: Arc<LogBuffer>) -> Self {
        Self { buffer }
    }
}

struct MessageVisitor {
    message: String,
    correlation_id: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            self.message.push_str(value);
        } else if field.name() == "correlation_id" || field.name() == "cid" {
            self.correlation_id = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{:?}", value);
        }
    }
}

impl<S> Layer<S> for RingBufferLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut visitor = MessageVisitor {
            message: String::new(),
            correlation_id: None,
        };
        event.record(&mut visitor);

        let entry = LogEntry {
            ts: iso_now(),
            level: meta.level().into(),
            source: meta.target().to_string(),
            message: visitor.message,
            correlation_id: visitor.correlation_id,
        };
        self.buffer.append(entry);
    }
}

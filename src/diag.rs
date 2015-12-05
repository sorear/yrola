// a simple library for logs which are not necessarily process-global
// inspired by [json_logger][https://github.com/rsolomo/json_logger]

extern crate serde_json as json;
use json;
use std::sync::Arc;

pub trait LogSink {
    fn accept(&self, record: json::Value);
}

pub type LogHandle = Arc<LogSync>;

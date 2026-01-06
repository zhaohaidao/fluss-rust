// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_server: Option<String>,

    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub request_max_size: i32,

    #[arg(long, default_value_t = String::from("all"))]
    pub writer_acks: String,

    #[arg(long, default_value_t = i32::MAX)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub writer_batch_size: i32,

    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub scanner_log_fetch_max_bytes: i32,

    #[arg(long, default_value_t = 1024 * 1024)]
    pub scanner_log_fetch_max_bytes_for_bucket: i32,

    #[arg(long, default_value_t = 1)]
    pub scanner_log_fetch_min_bytes: i32,

    #[arg(long, default_value_t = 500)]
    pub scanner_log_fetch_wait_max_ms: i32,

    #[arg(long, default_value_t = 500)]
    pub scanner_log_max_poll_records: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_server: None,
            request_max_size: 10 * 1024 * 1024,
            writer_acks: String::from("all"),
            writer_retries: i32::MAX,
            writer_batch_size: 2 * 1024 * 1024,
            scanner_log_fetch_max_bytes: 16 * 1024 * 1024,
            scanner_log_fetch_max_bytes_for_bucket: 1024 * 1024,
            scanner_log_fetch_min_bytes: 1,
            scanner_log_fetch_wait_max_ms: 500,
            scanner_log_max_poll_records: 500,
        }
    }
}

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

const DEFAULT_BOOTSTRAP_SERVER: &str = "127.0.0.1:9123";
const DEFAULT_REQUEST_MAX_SIZE: i32 = 10 * 1024 * 1024;
const DEFAULT_WRITER_BATCH_SIZE: i32 = 2 * 1024 * 1024;
const DEFAULT_RETRIES: i32 = i32::MAX;
const DEFAULT_PREFETCH_NUM: usize = 4;
const DEFAULT_DOWNLOAD_THREADS: usize = 3;
const DEFAULT_DECODE_THREADS: usize = 0;
const DEFAULT_DECODE_QUEUE_CAPACITY: usize = 256;
const DEFAULT_DECODE_INFLIGHT_PER_FETCH: usize = 4;

const DEFAULT_ACKS: &str = "all";

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value_t = String::from(DEFAULT_BOOTSTRAP_SERVER))]
    pub bootstrap_servers: String,

    #[arg(long, default_value_t = DEFAULT_REQUEST_MAX_SIZE)]
    pub writer_request_max_size: i32,

    #[arg(long, default_value_t = String::from(DEFAULT_ACKS))]
    pub writer_acks: String,

    #[arg(long, default_value_t = DEFAULT_RETRIES)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = DEFAULT_WRITER_BATCH_SIZE)]
    pub writer_batch_size: i32,

    /// Maximum number of remote log segments to prefetch
    /// Default: 4 (matching Java CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM)
    #[arg(long, default_value_t = DEFAULT_PREFETCH_NUM)]
    pub scanner_remote_log_prefetch_num: usize,

    /// Maximum concurrent remote log downloads
    /// Default: 3 (matching Java REMOTE_FILE_DOWNLOAD_THREAD_NUM)
    #[arg(long, default_value_t = DEFAULT_DOWNLOAD_THREADS)]
    pub remote_file_download_thread_num: usize,

    /// Parallel Arrow decode threads for RecordBatch scanning (0 disables)
    #[arg(long, default_value_t = DEFAULT_DECODE_THREADS)]
    pub scanner_decode_threads: usize,

    /// Decode task queue capacity
    #[arg(long, default_value_t = DEFAULT_DECODE_QUEUE_CAPACITY)]
    pub scanner_decode_queue_capacity: usize,

    /// Max in-flight decode tasks per fetch
    #[arg(long, default_value_t = DEFAULT_DECODE_INFLIGHT_PER_FETCH)]
    pub scanner_decode_inflight_per_fetch: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from(DEFAULT_BOOTSTRAP_SERVER),
            writer_request_max_size: DEFAULT_REQUEST_MAX_SIZE,
            writer_acks: String::from(DEFAULT_ACKS),
            writer_retries: i32::MAX,
            writer_batch_size: DEFAULT_WRITER_BATCH_SIZE,
            scanner_remote_log_prefetch_num: DEFAULT_PREFETCH_NUM,
            remote_file_download_thread_num: DEFAULT_DOWNLOAD_THREADS,
            scanner_decode_threads: DEFAULT_DECODE_THREADS,
            scanner_decode_queue_capacity: DEFAULT_DECODE_QUEUE_CAPACITY,
            scanner_decode_inflight_per_fetch: DEFAULT_DECODE_INFLIGHT_PER_FETCH,
        }
    }
}

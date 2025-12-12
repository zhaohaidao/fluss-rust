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

use crate::error::Result;
use opendal::services::S3Config;
use opendal::Configurator;
use opendal::Operator;
use std::collections::HashMap;

pub(crate) fn s3_config_build(props: &HashMap<String, String>) -> Result<Operator> {
    let config = S3Config::from_iter(props.clone())?;
    Ok(Operator::from_config(config)?.finish())
}

pub(crate) fn parse_s3_path(path: &str) -> (&str, &str) {
    let path = path
        .strip_prefix("s3a://")
        .or_else(|| path.strip_prefix("s3://"))
        .unwrap_or(path);

    match path.find('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => (path, ""),
    }
}


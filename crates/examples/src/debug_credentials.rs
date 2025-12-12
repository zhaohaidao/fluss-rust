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

use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::rpc::message::GetSecurityTokenRequest;

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Debug: Fetching security token from Fluss server...\n");

    let mut config = Config::default();
    config.bootstrap_server = Some("10.147.136.86:9123".to_string());

    println!("1) Connecting to Fluss...");
    let conn = FlussConnection::new(config).await?;
    println!("   Connected successfully!");

    // Get the RPC client and metadata
    let rpc_client = conn.get_connections();
    
    // Get a server node to send request to
    let cluster = conn.get_metadata().get_cluster();
    let server_node = cluster
        .get_coordinator_server()
        .or_else(|| Some(cluster.get_one_available_server()))
        .expect("no available server");
    
    println!("\n2) Fetching security token from server: {:?}", server_node);
    
    let connection = rpc_client.get_connection(server_node).await?;
    let request = GetSecurityTokenRequest::new();
    let response = connection.request(request).await?;
    
    println!("\n3) Security Token Response:");
    println!("   Token (raw bytes): {} bytes", response.token.len());
    
    // Try to parse as JSON
    if let Ok(token_str) = std::str::from_utf8(&response.token) {
        println!("   Token (as string): {}", token_str);
    }
    
    println!("\n4) Addition Info (key-value pairs from server):");
    if response.addition_info.is_empty() {
        println!("   (empty)");
    } else {
        for kv in &response.addition_info {
            println!("   {} = {}", kv.key, kv.value);
        }
    }
    
    println!("\n5) Current key mapping in Rust client (credentials.rs):");
    println!("   - fs.s3a.endpoint -> endpoint");
    println!("   - fs.s3a.endpoint.region -> region");
    println!("   - fs.s3a.path.style.access -> enable_virtual_host_style");
    println!("\n   NOTE: fs.red-s3.* keys are NOT supported!");
    
    println!("\nDone!");
    Ok(())
}

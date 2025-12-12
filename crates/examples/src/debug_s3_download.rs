// Debug S3 download to see what props are being used

use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::io::{FileIO, Storage};
use fluss::metadata::TablePath;
use fluss::rpc::message::GetSecurityTokenRequest;
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Credentials {
    access_key_id: String,
    access_key_secret: String,
    security_token: Option<String>,
}

/// Returns (opendal_key, needs_inversion)
fn convert_hadoop_key_to_opendal(hadoop_key: &str) -> Option<(String, bool)> {
    match hadoop_key {
        "fs.s3a.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.s3a.endpoint.region" => Some(("region".to_string(), false)),
        "fs.s3a.path.style.access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.s3a.connection.ssl.enabled" => None,
        "fs.red-s3.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.red-s3.region" => Some(("region".to_string(), false)),
        "fs.red-s3.path-style-access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.red-s3.connection.ssl.enabled" => None,
        _ => None,
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Debug: Testing S3 download configuration...\n");

    let mut config = Config::default();
    config.bootstrap_server = Some("10.147.136.86:9123".to_string());

    let conn = FlussConnection::new(config).await?;
    let rpc_client = conn.get_connections();
    let cluster = conn.get_metadata().get_cluster();
    let server_node = cluster.get_coordinator_server()
        .or_else(|| Some(cluster.get_one_available_server()))
        .expect("no available server");
    
    let connection = rpc_client.get_connection(server_node).await?;
    let request = GetSecurityTokenRequest::new();
    let response = connection.request(request).await?;
    
    // Parse credentials
    let credentials: Credentials = serde_json::from_slice(&response.token)
        .map_err(|e| fluss::error::Error::JsonSerdeError(e.to_string()))?;
    
    // Build props like the real code does
    let mut props = HashMap::new();
    props.insert("access_key_id".to_string(), credentials.access_key_id.clone());
    props.insert("secret_access_key".to_string(), credentials.access_key_secret.clone());
    if let Some(token) = &credentials.security_token {
        props.insert("security_token".to_string(), token.clone());
    }
    
    for kv in &response.addition_info {
        if let Some((opendal_key, needs_inversion)) = convert_hadoop_key_to_opendal(&kv.key) {
            let final_value = if needs_inversion {
                if kv.value == "true" { "false".to_string() } else { "true".to_string() }
            } else {
                kv.value.clone()
            };
            println!("Mapping: {} = {} -> {} = {}{}", 
                kv.key, kv.value, opendal_key, final_value,
                if needs_inversion { " (inverted)" } else { "" });
            props.insert(opendal_key, final_value);
        } else {
            println!("Skipping: {} = {}", kv.key, kv.value);
        }
    }
    
    println!("\n--- Final OpenDAL S3 Props ---");
    for (k, v) in &props {
        // Mask secrets
        if k.contains("key") || k.contains("secret") {
            println!("  {} = {}...", k, &v[..std::cmp::min(8, v.len())]);
        } else {
            println!("  {} = {}", k, v);
        }
    }
    
    // Try to create the S3 operator
    println!("\n--- Testing S3 Operator Creation ---");
    
    // Add bucket
    let test_bucket = "lsh-oss-fluss-internal";
    props.insert("bucket".to_string(), test_bucket.to_string());
    
    println!("Using bucket: {}", test_bucket);
    
    // Check for issues
    println!("\n--- Configuration Check ---");
    
    if let Some(vhost_style) = props.get("enable_virtual_host_style") {
        println!("enable_virtual_host_style = {}", vhost_style);
        println!("  (Note: This value should be the INVERSE of path_style_access)");
    }
    
    if !props.contains_key("endpoint") {
        println!("  ERROR: No endpoint configured!");
    }
    
    if !props.contains_key("region") {
        println!("  ERROR: No region configured!");
    }
    
    // Now try to actually create operator and stat a file
    println!("\n--- Testing S3 Connection ---");
    
    use opendal::services::S3Config;
    use opendal::Configurator;
    use opendal::Operator;
    
    let config = S3Config::from_iter(props.clone())?;
    let op = Operator::from_config(config)?.finish();
    
    println!("S3 Operator created successfully!");
    
    // Try a simple operation with timeout
    let test_path = "fluss_alsh_poc-2/remote-storage/";
    println!("Trying to list: {}", test_path);
    
    let list_future = op.list(test_path);
    let timeout = std::time::Duration::from_secs(10);
    
    match tokio::time::timeout(timeout, list_future).await {
        Ok(Ok(entries)) => {
            println!("List succeeded! Found {} entries", entries.len());
            for entry in entries.iter().take(5) {
                println!("  - {}", entry.path());
            }
            if entries.len() > 5 {
                println!("  ... and {} more", entries.len() - 5);
            }
        }
        Ok(Err(e)) => {
            println!("List failed with error: {}", e);
        }
        Err(_) => {
            println!("List timed out after {:?}", timeout);
        }
    }
    
    println!("\nDone!");
    Ok(())
}

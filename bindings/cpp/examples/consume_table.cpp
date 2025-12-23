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

#include "fluss.hpp"

#include <iostream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <csignal>
#include <atomic>
#include <thread>
#include <iomanip>

// 全局标志，用于优雅退出
std::atomic<bool> g_running{true};

// 信号处理函数
void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
    g_running = false;
}

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code
                  << " msg=" << r.error_message << std::endl;
        std::exit(1);
    }
}

// 将 DataType 转换为字符串
static const char* DataTypeToString(fluss::DataType type) {
    switch (type) {
        case fluss::DataType::Boolean: return "BOOLEAN";
        case fluss::DataType::TinyInt: return "TINYINT";
        case fluss::DataType::SmallInt: return "SMALLINT";
        case fluss::DataType::Int: return "INT";
        case fluss::DataType::BigInt: return "BIGINT";
        case fluss::DataType::Float: return "FLOAT";
        case fluss::DataType::Double: return "DOUBLE";
        case fluss::DataType::String: return "STRING";
        case fluss::DataType::Bytes: return "BYTES";
        case fluss::DataType::Date: return "DATE";
        case fluss::DataType::Time: return "TIME";
        case fluss::DataType::Timestamp: return "TIMESTAMP";
        case fluss::DataType::TimestampLtz: return "TIMESTAMP_LTZ";
        default: return "UNKNOWN";
    }
}

// 打印表 Schema
static void printTableSchema(const fluss::TableInfo& table_info) {
    std::cout << "\n=== Table Schema ===" << std::endl;
    std::cout << "Table: " << table_info.table_path.ToString() << std::endl;
    std::cout << "Table ID: " << table_info.table_id << std::endl;
    std::cout << "Schema ID: " << table_info.schema_id << std::endl;
    std::cout << "Number of buckets: " << table_info.num_buckets << std::endl;

    if (!table_info.comment.empty()) {
        std::cout << "Comment: " << table_info.comment << std::endl;
    }

    const auto& schema = table_info.schema;
    std::cout << "\nColumns (" << schema.columns.size() << "):" << std::endl;
    std::cout << "  Index | Name      | Type      | Comment" << std::endl;
    std::cout << "  ------|-----------|-----------|--------" << std::endl;

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& col = schema.columns[i];
        std::cout << "  " << std::setw(5) << i << " | "
                  << std::setw(9) << col.name << " | "
                  << std::setw(9) << DataTypeToString(col.data_type) << " | ";
        if (!col.comment.empty()) {
            std::cout << col.comment;
        }
        std::cout << std::endl;
    }

    if (!schema.primary_keys.empty()) {
        std::cout << "\nPrimary Keys: ";
        for (size_t i = 0; i < schema.primary_keys.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << schema.primary_keys[i];
        }
        std::cout << std::endl;
    }

    if (!table_info.bucket_keys.empty()) {
        std::cout << "Bucket Keys: ";
        for (size_t i = 0; i < table_info.bucket_keys.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << table_info.bucket_keys[i];
        }
        std::cout << std::endl;
    }

    if (!table_info.partition_keys.empty()) {
        std::cout << "Partition Keys: ";
        for (size_t i = 0; i < table_info.partition_keys.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << table_info.partition_keys[i];
        }
        std::cout << std::endl;
    }

    std::cout << "===================" << std::endl;
}

// 创建连接
static fluss::Connection setupConnection(const std::string& server_address) {
    std::cout << "Step 1: Creating client connection..." << std::endl;
    fluss::Connection conn;
    check("connect", fluss::Connection::Connect(server_address, conn));
    std::cout << "  ✓ Connected to Fluss server" << std::endl;
    return conn;
}

// 获取 Admin 和 Table，并打印 Schema
static std::pair<fluss::Admin, fluss::Table> setupTable(
    fluss::Connection& conn,
    const fluss::TablePath& table_path) {
    std::cout << "\nStep 2: Getting admin and table..." << std::endl;
    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    auto table_info = table.GetTableInfo();
    int num_buckets = table_info.num_buckets;
    std::cout << "  ✓ Table obtained, has " << num_buckets << " buckets" << std::endl;

    // 打印表 Schema
    printTableSchema(table_info);

    return {std::move(admin), std::move(table)};
}

// 获取所有 bucket IDs
static std::vector<int32_t> getAllBucketIds(int num_buckets) {
    std::vector<int32_t> bucket_ids;
    for (int b = 0; b < num_buckets; ++b) {
        bucket_ids.push_back(b);
    }
    return bucket_ids;
}

// 获取 earliest offsets
static std::unordered_map<int32_t, int64_t> getEarliestOffsets(
    fluss::Admin& admin,
    const fluss::TablePath& table_path,
    const std::vector<int32_t>& bucket_ids) {
    std::unordered_map<int32_t, int64_t> earliest_offsets;
    check("list_earliest_offsets",
          admin.ListOffsets(table_path, bucket_ids,
                           fluss::OffsetQuery::Earliest(),
                           earliest_offsets));

    std::cout << "  ✓ Earliest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        std::cout << "    Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    return earliest_offsets;
}

// 获取 timestamp offsets
static std::unordered_map<int32_t, int64_t> getTimestampOffsets(
    fluss::Admin& admin,
    const fluss::TablePath& table_path,
    const std::vector<int32_t>& bucket_ids,
    int64_t timestamp_ms) {
    std::unordered_map<int32_t, int64_t> timestamp_offsets;
    check("list_timestamp_offsets",
          admin.ListOffsets(table_path, bucket_ids,
                           fluss::OffsetQuery::FromTimestamp(timestamp_ms),
                           timestamp_offsets));

    std::cout << "  ✓ Timestamp offsets:" << std::endl;
    if (timestamp_offsets.empty()) {
        std::cerr << "  ERROR: No timestamp offsets found. Cannot proceed with consumption." << std::endl;
        std::exit(1);
    }
    for (const auto& [bucket_id, offset] : timestamp_offsets) {
        std::cout << "    Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    return timestamp_offsets;
}

// 处理单条记录
static void processRecord(size_t index, const fluss::ScanRecord& rec) {
    const auto& row = rec.row;

    // 验证字段数量
    if (row.fields.size() != 2) {
        std::cerr << "  WARNING: Record " << index << " has " << row.fields.size()
                  << " fields, expected 2" << std::endl;
        return;
    }

    // 解析 id (BIGINT -> Int64)
    int64_t id_val = 0;
    if (row.fields[0].type == fluss::DatumType::Int64) {
        id_val = row.fields[0].i64_val;
    } else if (row.fields[0].type == fluss::DatumType::Int32) {
        id_val = row.fields[0].i32_val;
    } else {
        std::cerr << "  WARNING: Record " << index << " field 0 type is not Int64/Int32" << std::endl;
        return;
    }

    // 解析 value (STRING)
    std::string value_val;
    if (row.fields[1].type == fluss::DatumType::String) {
        value_val = row.fields[1].string_val;
    } else {
        std::cerr << "  WARNING: Record " << index << " field 1 type is not String" << std::endl;
        return;
    }

    // 打印记录信息
    std::cout << "    Record " << index << ": bucket_id=" << rec.bucket_id
              << ", offset=" << rec.offset
              << ", timestamp=" << rec.timestamp
              << ", id=" << id_val
              << ", value=" << value_val << std::endl;
}

// 持续消费数据
// target_timestamp_ms: 如果 > 0，则只消费 timestamp >= target_timestamp_ms 的记录；如果 <= 0，则消费所有记录
static void consumeRecords(fluss::LogScanner& scanner, int64_t target_timestamp_ms = 0) {
    if (target_timestamp_ms > 0) {
        std::cout << "\nStep 6: Starting continuous consumption from timestamp..." << std::endl;
        std::cout << "  Target timestamp: " << target_timestamp_ms << " (filtering records before this)" << std::endl;
    } else {
        std::cout << "\nStep 6: Starting continuous consumption from earliest offsets..." << std::endl;
    }
    std::cout << "  (Press Ctrl+C to stop)" << std::endl;
    std::cout << std::endl;

    int64_t total_records = 0;
    int64_t filtered_records = 0;  // 被过滤的记录数（仅在 timestamp 模式下使用）
    int64_t poll_count = 0;
    int consecutive_arrow_errors = 0;
    const int MAX_CONSECUTIVE_ARROW_ERRORS = 5;  // 最多允许 5 次连续的 Arrow 错误
    auto start_consume_time = std::chrono::steady_clock::now();
    bool use_timestamp_filter = (target_timestamp_ms > 0);

    while (g_running) {
        fluss::ScanRecords records;
        auto poll_result = scanner.Poll(5000, records);  // 5 秒超时

        if (!poll_result.Ok()) {
            // 检查是否是 Arrow 解析错误
            bool is_arrow_error = (poll_result.error_message.find("Invalid continuation marker") != std::string::npos ||
                                  poll_result.error_message.find("Arrow error") != std::string::npos ||
                                  poll_result.error_message.find("Parser error") != std::string::npos);

            if (is_arrow_error) {
                consecutive_arrow_errors++;
                std::cerr << "Poll failed (Arrow error #" << consecutive_arrow_errors << "): code=" << poll_result.error_code
                          << " msg=" << poll_result.error_message << std::endl;

                if (consecutive_arrow_errors >= MAX_CONSECUTIVE_ARROW_ERRORS) {
                    std::cerr << "\nERROR: Encountered " << consecutive_arrow_errors
                              << " consecutive Arrow parse errors." << std::endl;
                    std::cerr << "The starting offset is NOT at a message boundary and cannot be automatically fixed." << std::endl;
                    std::cerr << "Exiting program..." << std::endl;
                    g_running = false;
                    break;
                }
            } else {
                // 非 Arrow 错误，重置计数
                consecutive_arrow_errors = 0;
                std::cerr << "Poll failed: code=" << poll_result.error_code
                          << " msg=" << poll_result.error_message << std::endl;
            }

            // 继续重试
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }

        // 成功 Poll，重置 Arrow 错误计数
        if (consecutive_arrow_errors > 0) {
            std::cout << "  ✓ Successfully recovered from Arrow errors. Consumption resumed." << std::endl;
            consecutive_arrow_errors = 0;
        }

        poll_count++;
        size_t record_count = records.Size();
        int64_t valid_records = 0;  // 有效的记录数（通过过滤的）

        if (record_count > 0) {
            // 处理每条记录
            for (size_t i = 0; i < record_count; ++i) {
                const auto& rec = records[i];

                // 如果使用 timestamp 过滤，只处理 timestamp >= target_timestamp_ms 的记录
                if (use_timestamp_filter) {
                    if (rec.timestamp >= target_timestamp_ms) {
                        valid_records++;
                        total_records++;

                        // 只打印前 10 条有效记录
                        if (valid_records <= 10) {
                            processRecord(valid_records - 1, rec);
                        }
                    } else {
                        filtered_records++;
                    }
                } else {
                    // 不使用过滤，处理所有记录
                    total_records++;
                    valid_records++;

                    // 只打印前 10 条记录
                    if (total_records <= 10) {
                        processRecord(total_records - 1, rec);
                    }
                }
            }

            if (use_timestamp_filter) {
                if (valid_records > 0) {
                    std::cout << "[Poll #" << poll_count << "] Received " << record_count
                              << " records, " << valid_records << " valid (after timestamp), "
                              << filtered_records << " filtered (before timestamp)" << std::endl;
                    std::cout << "  Total valid records: " << total_records << std::endl;

                    if (valid_records > 10) {
                        std::cout << "    ... and " << (valid_records - 10) << " more valid records" << std::endl;
                    }
                } else if (filtered_records > 0) {
                    // 所有记录都被过滤了
                    if (poll_count % 10 == 0) {
                        std::cout << "[Poll #" << poll_count << "] Received " << record_count
                                  << " records, all filtered (before timestamp). Total filtered: "
                                  << filtered_records << std::endl;
                    }
                }
            } else {
                std::cout << "[Poll #" << poll_count << "] Received " << record_count
                          << " records. Total records: " << total_records << std::endl;

                if (total_records > 10) {
                    std::cout << "    ... and " << (total_records - 10) << " more records" << std::endl;
                }
            }
        } else {
            // 没有新数据时，定期输出状态
            if (poll_count % 10 == 0) {
                auto elapsed = std::chrono::steady_clock::now() - start_consume_time;
                auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
                if (use_timestamp_filter) {
                    std::cout << "[Status] Poll #" << poll_count
                              << ", Total valid records: " << total_records
                              << ", Filtered records: " << filtered_records
                              << ", Elapsed: " << elapsed_sec << "s" << std::endl;
                } else {
                    std::cout << "[Status] Poll #" << poll_count
                              << ", Total records: " << total_records
                              << ", Elapsed: " << elapsed_sec << "s" << std::endl;
                }
            }
        }
    }

    // 输出最终统计
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_consume_time).count();

    std::cout << "\n=== Consumption Summary ===" << std::endl;
    std::cout << "Total polls: " << poll_count << std::endl;
    if (use_timestamp_filter) {
        std::cout << "Total valid records consumed: " << total_records << std::endl;
        std::cout << "Total filtered records (before timestamp): " << filtered_records << std::endl;
    } else {
        std::cout << "Total records consumed: " << total_records << std::endl;
    }
    std::cout << "Elapsed time: " << elapsed << " seconds" << std::endl;
    if (elapsed > 0 && total_records > 0) {
        double rate = static_cast<double>(total_records) / static_cast<double>(elapsed);
        std::cout << "Average rate: " << std::fixed << std::setprecision(2) << rate << " records/second" << std::endl;
    } else if (elapsed > 0) {
        std::cout << "Average rate: 0.00 records/second" << std::endl;
    }
    std::cout << "Consumer stopped gracefully." << std::endl;
}

int main(int argc, char* argv[]) {
    // 注册信号处理
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "=== Fluss Table Consumer Example ===" << std::endl;
    std::cout << "Table: cayde_test_fluss_1222_v1" << std::endl;
    std::cout << "Projected columns: id (BIGINT), value (STRING)" << std::endl;
    std::cout << std::endl;

    // 配置参数
    const std::string server_address = "10.147.136.86:9123";
    const fluss::TablePath table_path("fluss", "cayde_test_fluss_1222_v2");
    const std::vector<size_t> projected_columns = {0, 2};  // id 和 value 列

    // 消费模式控制：true = 从 timestamp 消费，false = 从 earliest offset 消费
    const bool USE_TIMESTAMP_MODE = false;  // 设置为 true 启用 timestamp 模式
    const int minutes_ago = 10;  // 从多少分钟前开始消费（仅在 timestamp 模式下使用）

    // 1) 创建连接
    fluss::Connection conn = setupConnection(server_address);

    // 2) 获取 Admin 和 Table
    auto [admin, table] = setupTable(conn, table_path);
    auto table_info = table.GetTableInfo();
    int num_buckets = table_info.num_buckets;
    std::vector<int32_t> bucket_ids = getAllBucketIds(num_buckets);

    // 3) 根据模式选择 offsets
    std::unordered_map<int32_t, int64_t> target_offsets;
    int64_t target_timestamp_ms = 0;

    if (USE_TIMESTAMP_MODE) {
        // 从 timestamp 消费
        std::cout << "\nMode: Consuming from timestamp" << std::endl;
        auto now = std::chrono::system_clock::now();
        auto start_time = now - std::chrono::minutes(minutes_ago);
        target_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            start_time.time_since_epoch()).count();

        std::cout << "Step 3: Getting offsets for timestamp " << target_timestamp_ms
                  << " (" << minutes_ago << " minutes ago)..." << std::endl;
        target_offsets = getTimestampOffsets(admin, table_path, bucket_ids, target_timestamp_ms);
    } else {
        // 从 earliest offset 消费
        std::cout << "\nMode: Consuming from earliest offsets" << std::endl;
        std::cout << "Step 3: Getting earliest offsets for all buckets..." << std::endl;
        target_offsets = getEarliestOffsets(admin, table_path, bucket_ids);
    }


    // 4) 创建 scanner 并订阅
    std::cout << "\nStep 4: Creating log scanner with projection (columns: id, value)..." << std::endl;
    fluss::LogScanner scanner;
    check("new_log_scanner_with_projection",
          table.NewLogScannerWithProjection(projected_columns, scanner));
    std::cout << "  ✓ Projected scanner created" << std::endl;

    std::cout << "\nStep 5: Subscribing to all buckets..." << std::endl;
    std::vector<fluss::BucketSubscription> subscriptions;
    for (const auto& [bucket_id, offset] : target_offsets) {
        subscriptions.push_back({bucket_id, offset});
        std::cout << "  Subscribing bucket " << bucket_id << " from offset " << offset << std::endl;
    }

    if (subscriptions.empty()) {
        std::cerr << "  ERROR: No subscriptions to create. Cannot proceed with consumption." << std::endl;
        std::exit(1);
    }

    check("subscribe_batch", scanner.Subscribe(subscriptions));
    std::cout << "  ✓ Subscribed to " << subscriptions.size() << " buckets" << std::endl;

    // 6) 持续消费数据
    consumeRecords(scanner, target_timestamp_ms);

    return 0;
}


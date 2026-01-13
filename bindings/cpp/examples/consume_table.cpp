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
#include <limits>

// Global flag for graceful shutdown.
std::atomic<bool> g_running{true};

// Signal handler.
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

// Convert DataType to string.
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

// Print table schema.
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

// Create connection.
static fluss::Connection setupConnection(const std::string& server_address) {
    std::cout << "Step 1: Creating client connection..." << std::endl;
    fluss::Connection conn;
    check("connect", fluss::Connection::Connect(server_address, conn));
    std::cout << "  OK: Connected to Fluss server" << std::endl;
    return conn;
}

// Get admin and table, then print schema.
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
    std::cout << "  OK: Table obtained, has " << num_buckets << " buckets" << std::endl;

    // Print table schema.
    printTableSchema(table_info);

    return {std::move(admin), std::move(table)};
}

// Get all bucket IDs.
static std::vector<int32_t> getAllBucketIds(int num_buckets) {
    std::vector<int32_t> bucket_ids;
    for (int b = 0; b < num_buckets; ++b) {
        bucket_ids.push_back(b);
    }
    return bucket_ids;
}

// Get earliest offsets.
static std::unordered_map<int32_t, int64_t> getEarliestOffsets(
    fluss::Admin& admin,
    const fluss::TablePath& table_path,
    const std::vector<int32_t>& bucket_ids) {
    std::unordered_map<int32_t, int64_t> earliest_offsets;
    check("list_earliest_offsets",
          admin.ListOffsets(table_path, bucket_ids,
                           fluss::OffsetQuery::Earliest(),
                           earliest_offsets));

    std::cout << "  OK: Earliest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        std::cout << "    Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    return earliest_offsets;
}

// Get timestamp offsets.
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

    std::cout << "  OK: Timestamp offsets:" << std::endl;
    if (timestamp_offsets.empty()) {
        std::cerr << "  ERROR: No timestamp offsets found. Cannot proceed with consumption."
                  << std::endl;
        std::exit(1);
    }
    for (const auto& [bucket_id, offset] : timestamp_offsets) {
        std::cout << "    Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    return timestamp_offsets;
}

static int64_t computeTimestampMsFromNow(int64_t window_ms) {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    return now_ms - window_ms;
}

// Try to get latest (end) offsets for each bucket.
static bool tryGetLatestOffsets(
    fluss::Admin& admin,
    const fluss::TablePath& table_path,
    const std::vector<int32_t>& bucket_ids,
    std::unordered_map<int32_t, int64_t>& latest_offsets) {
    latest_offsets.clear();
    auto result = admin.ListOffsets(table_path, bucket_ids,
                                    fluss::OffsetQuery::Latest(),
                                    latest_offsets);
    if (!result.Ok()) {
        std::cerr << "list_latest_offsets failed: code=" << result.error_code
                  << " msg=" << result.error_message << std::endl;
        return false;
    }
    return true;
}

static fluss::LogScanner createScannerWithProjectionAndSubscribe(
    fluss::Table& table,
    const std::vector<size_t>& projected_columns,
    const std::unordered_map<int32_t, int64_t>& offsets,
    bool verbose) {
    fluss::LogScanner scanner;
    check("new_log_scanner_with_projection",
          table.NewLogScannerWithProjection(projected_columns, scanner));

    if (verbose) {
        std::cout << "  OK: Projected scanner created" << std::endl;
    }

    if (verbose) {
        std::cout << "\nStep 5: Subscribing to all buckets..." << std::endl;
    }

    std::vector<fluss::BucketSubscription> subscriptions;
    subscriptions.reserve(offsets.size());
    for (const auto& [bucket_id, offset] : offsets) {
        subscriptions.push_back({bucket_id, offset});
        if (verbose) {
            std::cout << "  Subscribing bucket " << bucket_id << " from offset " << offset << std::endl;
        }
    }

    if (subscriptions.empty()) {
        std::cerr << "  ERROR: No subscriptions to create. Cannot proceed with consumption." << std::endl;
        std::exit(1);
    }

    check("subscribe_batch", scanner.Subscribe(subscriptions));
    if (verbose) {
        std::cout << "  OK: Subscribed to " << subscriptions.size() << " buckets" << std::endl;
    }
    return scanner;
}

// Process a single record.
static void processRecord(size_t index, const fluss::ScanRecord& rec) {
    const auto& row = rec.row;

    // Validate field count.
    if (row.fields.size() != 2) {
        std::cerr << "  WARNING: Record " << index << " has " << row.fields.size()
                  << " fields, expected 2" << std::endl;
        return;
    }

    // Parse id (BIGINT -> Int64).
    int64_t id_val = 0;
    if (row.fields[0].type == fluss::DatumType::Int64) {
        id_val = row.fields[0].i64_val;
    } else if (row.fields[0].type == fluss::DatumType::Int32) {
        id_val = row.fields[0].i32_val;
    } else {
        std::cerr << "  WARNING: Record " << index << " field 0 type is not Int64/Int32" << std::endl;
        return;
    }

    // Parse value (STRING).
    std::string value_val;
    if (row.fields[1].type == fluss::DatumType::String) {
        value_val = row.fields[1].string_val;
    } else {
        std::cerr << "  WARNING: Record " << index << " field 1 type is not String" << std::endl;
        return;
    }

    // Print record data.
    std::cout << "    Record " << index << ": bucket_id=" << rec.bucket_id
              << ", offset=" << rec.offset
              << ", timestamp=" << rec.timestamp
              << ", id=" << id_val
              << ", value=" << value_val << std::endl;
}

// Continuous consumption.
// target_timestamp_ms: when > 0, only consume records with timestamp >= target_timestamp_ms.
static void consumeRecords(
    fluss::Admin& admin,
    fluss::Table& table,
    const fluss::TablePath& table_path,
    const std::vector<int32_t>& bucket_ids,
    const std::vector<size_t>& projected_columns,
    std::unordered_map<int32_t, int64_t> start_offsets,
    int64_t target_timestamp_ms = 0,
    int64_t timestamp_window_ms = 0) {
    std::cout << "\nStep 4: Creating log scanner with projection (columns: id, value)..." << std::endl;
    fluss::LogScanner scanner = createScannerWithProjectionAndSubscribe(
        table, projected_columns, start_offsets, true);

    if (target_timestamp_ms > 0) {
        std::cout << "\nStep 6: Starting continuous consumption from timestamp..." << std::endl;
        std::cout << "  Target timestamp: " << target_timestamp_ms << " (filtering records before this)" << std::endl;
        if (timestamp_window_ms > 0) {
            double window_hours = static_cast<double>(timestamp_window_ms) / 3600000.0;
            std::cout << "  Sliding window: now - " << std::fixed << std::setprecision(2)
                      << window_hours << " hours"
                      << std::defaultfloat << std::endl;
        }
    } else {
        std::cout << "\nStep 6: Starting continuous consumption from earliest offsets..." << std::endl;
    }
    std::cout << "  (Press Ctrl+C to stop)" << std::endl;
    std::cout << std::endl;

    int64_t total_records = 0;
    int64_t filtered_records = 0;  // Count of filtered records in timestamp mode.
    int64_t poll_count = 0;
    int consecutive_arrow_errors = 0;
    const int MAX_CONSECUTIVE_ARROW_ERRORS = 5;  // Max consecutive Arrow errors.
    const int64_t RESTART_LAG_THRESHOLD = 500;
    const auto END_OFFSET_CHECK_INTERVAL = std::chrono::seconds(10);
    auto start_consume_time = std::chrono::steady_clock::now();
    auto last_progress_time = start_consume_time;
    int64_t last_progress_records = 0;
    auto next_end_offset_check = start_consume_time + END_OFFSET_CHECK_INTERVAL;
    bool use_timestamp_filter = (target_timestamp_ms > 0);
    bool has_consumed_since_restart = false;
    int64_t restart_count = 0;
    std::unordered_map<int32_t, int64_t> consumed_offsets = std::move(start_offsets);
    const int LOG_INTERVAL = 10000;

    while (g_running) {
        fluss::ScanRecords records;
        auto poll_result = scanner.Poll(5000, records);  // 5 seconds timeout.

        if (!poll_result.Ok()) {
            // Check whether this is an Arrow parse error.
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
                // Reset the counter for non-Arrow errors.
                consecutive_arrow_errors = 0;
                std::cerr << "Poll failed: code=" << poll_result.error_code
                          << " msg=" << poll_result.error_message << std::endl;
            }

            // Retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }

        // Reset Arrow error counter after a successful poll.
        if (consecutive_arrow_errors > 0) {
            std::cout << "  OK: Successfully recovered from Arrow errors. Consumption resumed."
                      << std::endl;
            consecutive_arrow_errors = 0;
        }

        poll_count++;
        size_t record_count = records.Size();
        int64_t valid_records = 0;  // Valid records after timestamp filter.
        bool should_log = (poll_count % LOG_INTERVAL == 0);
        size_t printed_records = 0;

        if (record_count > 0) {
            // Process each record.
            for (size_t i = 0; i < record_count; ++i) {
                const auto& rec = records[i];
                auto& current_offset = consumed_offsets[rec.bucket_id];
                if (rec.offset > current_offset) {
                    current_offset = rec.offset;
                }
                has_consumed_since_restart = true;

                // In timestamp mode, only process records >= target_timestamp_ms.
                if (use_timestamp_filter) {
                    if (rec.timestamp >= target_timestamp_ms) {
                        valid_records++;
                        total_records++;

                        // Only print sample records when logging this poll.
                        if (should_log && printed_records < 10) {
                            processRecord(printed_records, rec);
                            printed_records++;
                        }
                    } else {
                        filtered_records++;
                    }
                } else {
                    // Process all records.
                    total_records++;
                    valid_records++;

                    // Only print the first 10 records.
                    if (total_records <= 10) {
                        processRecord(total_records - 1, rec);
                    }
                }
            }

            if (use_timestamp_filter) {
                if (should_log && valid_records > 0) {
                    std::cout << "[Poll #" << poll_count << "] Received " << record_count
                              << " records, " << valid_records << " valid (after timestamp), "
                              << filtered_records << " filtered (before timestamp)" << std::endl;
                    std::cout << "  Total valid records: " << total_records << std::endl;

                    if (valid_records > 10) {
                        std::cout << "    ... and " << (valid_records - 10) << " more valid records" << std::endl;
                    }
                } else if (should_log && filtered_records > 0) {
                    // All records were filtered.
                    std::cout << "[Poll #" << poll_count << "] Received " << record_count
                              << " records, all filtered (before timestamp). Total filtered: "
                              << filtered_records << std::endl;
                }
            } else {
                // std::cout << "[Poll #" << poll_count << "] Received " << record_count
                //        << " records. Total records: " << total_records << std::endl;

                // if (total_records > 10000000) {
                //     std::cout << "    ... and " << (total_records - 10) << " more records" << std::endl;
                // }
            }
        } else {
            // Periodically report status when no new data arrives.
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

        auto now = std::chrono::steady_clock::now();
        if (now >= next_end_offset_check) {
            next_end_offset_check = now + END_OFFSET_CHECK_INTERVAL;
            std::unordered_map<int32_t, int64_t> latest_offsets;
            if (tryGetLatestOffsets(admin, table_path, bucket_ids, latest_offsets)) {
                bool all_close = has_consumed_since_restart;
                int64_t max_lag = 0;
                int64_t min_lag = std::numeric_limits<int64_t>::max();
                int64_t min_consumed = std::numeric_limits<int64_t>::max();
                int64_t max_consumed = std::numeric_limits<int64_t>::min();
                int64_t min_latest = std::numeric_limits<int64_t>::max();
                int64_t max_latest = std::numeric_limits<int64_t>::min();
                size_t valid_buckets = 0;
                for (int32_t bucket_id : bucket_ids) {
                    auto consumed_it = consumed_offsets.find(bucket_id);
                    auto latest_it = latest_offsets.find(bucket_id);
                    if (consumed_it == consumed_offsets.end() ||
                        latest_it == latest_offsets.end()) {
                        all_close = false;
                        continue;
                    }
                    valid_buckets++;
                    int64_t consumed = consumed_it->second;
                    int64_t latest = latest_it->second;
                    if (consumed < min_consumed) {
                        min_consumed = consumed;
                    }
                    if (consumed > max_consumed) {
                        max_consumed = consumed;
                    }
                    if (latest < min_latest) {
                        min_latest = latest;
                    }
                    if (latest > max_latest) {
                        max_latest = latest;
                    }
                    int64_t lag = latest_it->second - consumed_it->second;
                    if (lag < 0) {
                        lag = 0;
                    }
                    if (lag > max_lag) {
                        max_lag = lag;
                    }
                    if (lag < min_lag) {
                        min_lag = lag;
                    }
                    if (lag >= RESTART_LAG_THRESHOLD) {
                        all_close = false;
                    }
                }

                int64_t interval_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_progress_time).count();
                int64_t delta_records = total_records - last_progress_records;
                double throughput = 0.0;
                if (interval_ms > 0) {
                    throughput = (static_cast<double>(delta_records) * 1000.0) /
                                 static_cast<double>(interval_ms);
                }
                last_progress_time = now;
                last_progress_records = total_records;

                const char* phase = "REMOTE_READ";
                if (!has_consumed_since_restart) {
                    phase = "SEEKING";
                } else if (max_lag < RESTART_LAG_THRESHOLD) {
                    phase = "STREAMING";
                }

                if (valid_buckets == 0) {
                    std::cout << "[Progress] phase=" << phase
                              << ", no bucket offsets available"
                              << ", throughput=" << std::fixed << std::setprecision(2)
                              << throughput << " r/s"
                              << ", interval_ms=" << interval_ms
                              << ", records=" << total_records
                              << std::defaultfloat << std::endl;
                } else {
                    std::cout << "[Progress] phase=" << phase
                              << ", max_lag=" << max_lag
                              << ", min_lag=" << min_lag
                              << ", consumed=[" << min_consumed << "," << max_consumed << "]"
                              << ", latest=[" << min_latest << "," << max_latest << "]"
                              << ", buckets=" << valid_buckets
                              << ", throughput=" << std::fixed << std::setprecision(2)
                              << throughput << " r/s"
                              << ", interval_ms=" << interval_ms
                              << ", records=" << total_records
                              << std::defaultfloat << std::endl;
                }

                if (all_close) {
                    restart_count++;
                    if (use_timestamp_filter) {
                        double window_hours = static_cast<double>(timestamp_window_ms) / 3600000.0;
                        std::cout << "\n[Rewind] Reached tail (max lag " << max_lag
                                  << " < " << RESTART_LAG_THRESHOLD
                                  << "), restarting from now - " << std::fixed << std::setprecision(2)
                                  << window_hours << " hours. Cycle: "
                                  << restart_count
                                  << std::defaultfloat << std::endl;

                        target_timestamp_ms = computeTimestampMsFromNow(timestamp_window_ms);
                        auto timestamp_offsets = getTimestampOffsets(
                            admin, table_path, bucket_ids, target_timestamp_ms);
                        scanner = createScannerWithProjectionAndSubscribe(
                            table, projected_columns, timestamp_offsets, false);
                        consumed_offsets = std::move(timestamp_offsets);
                        has_consumed_since_restart = false;
                        consecutive_arrow_errors = 0;
                    } else {
                        std::cout << "\n[Rewind] Reached tail (max lag " << max_lag
                                  << " < " << RESTART_LAG_THRESHOLD
                                  << "), restarting from earliest offsets. Cycle: "
                                  << restart_count << std::endl;

                        auto earliest_offsets = getEarliestOffsets(admin, table_path, bucket_ids);
                        scanner = createScannerWithProjectionAndSubscribe(
                            table, projected_columns, earliest_offsets, false);
                        consumed_offsets = std::move(earliest_offsets);
                        has_consumed_since_restart = false;
                        consecutive_arrow_errors = 0;
                    }
                }
            }
        }
    }

    // Final summary.
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
    std::cout << "Total rewinds: " << restart_count << std::endl;
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
    // Register signal handlers.
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "=== Fluss Table Consumer Example ===" << std::endl;
    std::cout << "Table: cayde_test_fluss_1222_v1" << std::endl;
    std::cout << "Projected columns: id (BIGINT), value (STRING)" << std::endl;
    std::cout << std::endl;

    // Configuration.
    const std::string server_address = "10.147.136.86:9123";
    const fluss::TablePath table_path("fluss", "cayde_test_fluss_0107_v1");
    const std::vector<size_t> projected_columns = {0, 2};  // id and value columns.

    // Consumption mode: true = timestamp, false = earliest offset.
    const bool USE_TIMESTAMP_MODE = true;  // Enable timestamp mode by default.
    const int hours_ago = 2;  // Start consumption from this many hours ago.
    const int64_t timestamp_window_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::hours(hours_ago)).count();

    // 1) Create connection.
    fluss::Connection conn = setupConnection(server_address);

    // 2) Get admin and table.
    auto [admin, table] = setupTable(conn, table_path);
    auto table_info = table.GetTableInfo();
    int num_buckets = table_info.num_buckets;
    std::vector<int32_t> bucket_ids = getAllBucketIds(num_buckets);

    // 3) Select offsets based on the mode.
    std::unordered_map<int32_t, int64_t> target_offsets;
    int64_t target_timestamp_ms = 0;

    if (USE_TIMESTAMP_MODE) {
        // Consume from a timestamp.
        std::cout << "\nMode: Consuming from timestamp" << std::endl;
        target_timestamp_ms = computeTimestampMsFromNow(timestamp_window_ms);

        std::cout << "Step 3: Getting offsets for timestamp " << target_timestamp_ms
                  << " (" << hours_ago << " hours ago)..." << std::endl;
        target_offsets = getTimestampOffsets(admin, table_path, bucket_ids, target_timestamp_ms);
    } else {
        // Consume from earliest offsets.
        std::cout << "\nMode: Consuming from earliest offsets" << std::endl;
        std::cout << "Step 3: Getting earliest offsets for all buckets..." << std::endl;
        target_offsets = getEarliestOffsets(admin, table_path, bucket_ids);
    }


    // 6) Continuous consumption.
    consumeRecords(admin, table, table_path, bucket_ids, projected_columns,
                   target_offsets, target_timestamp_ms, timestamp_window_ms);

    return 0;
}

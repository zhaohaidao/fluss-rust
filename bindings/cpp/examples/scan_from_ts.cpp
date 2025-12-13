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
#include <chrono>
#include <ctime>

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code
                  << " msg=" << r.error_message << std::endl;
        std::exit(1);
    }
}

int main() {
    std::cout << "Starting C++ scan from timestamp example..." << std::endl;

    std::cout << "1) Connecting to Fluss..." << std::endl;
    fluss::Connection conn;
    check("connect", fluss::Connection::Connect("10.147.136.86:9123", conn));
    std::cout << "   Connected successfully!" << std::endl;

    fluss::TablePath table_path("fluss", "mahong_log_table_cpp_test_1212");
    std::cout << "2) Getting admin..." << std::endl;
    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));
    std::cout << "   Admin obtained successfully!" << std::endl;

    std::cout << "3) Getting table: " << table_path.ToString() << std::endl;
    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));
    std::cout << "   Table obtained successfully!" << std::endl;

    auto table_info = table.GetTableInfo();
    int num_buckets = table_info.num_buckets;
    std::cout << "   Table has " << num_buckets << " buckets" << std::endl;

    auto now = std::chrono::system_clock::now();
    auto twenty_minutes_ago = now - std::chrono::minutes(20);
    auto timestamp_sec = std::chrono::duration_cast<std::chrono::seconds>(
        twenty_minutes_ago.time_since_epoch()).count();
    std::cout << "   Calculated timestamp (seconds): " << timestamp_sec << std::endl;

    std::cout << "\n7) Creating log scanner with projection (columns 0, 2)..." << std::endl;
    std::vector<size_t> projected_columns = {0, 2};
    fluss::LogScanner projected_scanner;
    check("new_log_scanner_with_projection", 
          table.NewLogScannerWithProjection(projected_columns, projected_scanner));
    std::cout << "   Projected scanner created successfully!" << std::endl;

    std::cout << "8) Subscribing projected scanner to all buckets from offset 278807821..." << std::endl;
    for (int bucket_id = 0; bucket_id < num_buckets; ++bucket_id) {
        check("subscribe", projected_scanner.Subscribe(bucket_id, 278807821));
        std::cout << "   Subscribed to bucket " << bucket_id << " from offset 278807821" << std::endl;
    }

    while (true) {
        std::cout << "9) Polling projected records (timeout: 10 seconds)..." << std::endl;
        fluss::ScanRecords projected_records;
        check("poll_projected", projected_scanner.Poll(10000, projected_records));

        size_t projected_count = projected_records.Size();
        std::cout << "Projected records: " << projected_count << std::endl;

        bool projection_verified = true;
        for (size_t i = 0; i < projected_records.Size(); ++i) {
            const auto& rec = projected_records[i];
            const auto& row = rec.row;
            size_t field_count = row.fields.size();

            if (field_count != 2) {
                std::cerr << "ERROR: Record " << i << " has " << field_count 
                          << " fields, expected 2" << std::endl;
                projection_verified = false;
                continue;
            }

            if (i < 10) {
                int64_t id_val = 0;
                std::string name_val;
                
                if (row.fields[0].type == fluss::DatumType::Int32) {
                    id_val = row.fields[0].i32_val;
                } else if (row.fields[0].type == fluss::DatumType::Int64) {
                    id_val = row.fields[0].i64_val;
                }
                
                if (row.fields[1].type == fluss::DatumType::String) {
                    name_val = row.fields[1].string_val;
                }
                
                std::cout << "  Record " << i << ": id=" << id_val 
                          << ", name=" << name_val << std::endl;
            }
        }

        if (projected_count > 10) {
            std::cout << "  ... and " << (projected_count - 10) << " more records" << std::endl;
        }

        if (projection_verified) {
            std::cout << "\nColumn pruning verification passed!" << std::endl;
        } else {
            std::cerr << "\nColumn pruning verification failed!" << std::endl;
            std::exit(1);
        }
    }

    return 0;
}

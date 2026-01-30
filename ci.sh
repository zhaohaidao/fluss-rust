#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -xe

ROOT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
CPP_DIR="$ROOT_DIR/bindings/cpp"

# Set Bazel output base to bindings/cpp/bazel-build/.bazel-output-base
BAZEL_OUTPUT_BASE="$CPP_DIR/bazel-build/.bazel-output-base"

# Create output base directory if it doesn't exist
mkdir -p "$BAZEL_OUTPUT_BASE"

# Wrapper function to run bazel from bindings/cpp with a fixed output base
bazel() {
    (cd "$CPP_DIR" && command bazel --output_base="$BAZEL_OUTPUT_BASE" "$@")
}

compile() {
    bazel build //:fluss_cpp
}

build_example() {
    bazel build //:consume_table
}

run_example() {
    build_example
    # Forward all args after 'run' to consume_table
    shift
    bazel run //:consume_table --action_env=RUST_LOG="${RUST_LOG:-warn}" -- "$@"
}

clean() {
    bazel clean
    # Remove bazel-* symlinks (Bazel automatically creates these)
    rm -f "$CPP_DIR"/bazel-*
    # Also remove the bazel-build directory if it exists
    if [ -d "$CPP_DIR/bazel-build" ]; then
        rm -rf "$CPP_DIR/bazel-build"
    fi
    echo "Cleaned all Bazel outputs and symlinks"
}

show_outputs() {
    echo "=== Library outputs ==="
    bazel cquery //:fluss_cpp --output=files 2>/dev/null || echo "Run 'bazel build //:fluss_cpp' first"
    echo ""
    echo "=== Example binary outputs ==="
    bazel cquery //:fluss_cpp_example --output=files 2>/dev/null || echo "Run 'bazel build //:fluss_cpp_example' first"
    echo ""
    echo "=== To run the example ==="
    echo "  bazel run //:fluss_cpp_example"
    echo ""
    echo "=== To find outputs manually ==="
    echo "  bazel info bazel-bin"
}

case $1 in
    compile )
        compile
        ;;
    example )
        build_example
        ;;
    run )
        run_example "$@"
        ;;
    outputs )
        show_outputs
        ;;
    clean )
        clean
        ;;
    * )
        echo "Usage: $0 {compile|example|run|outputs|clean}"
        echo ""
        echo "Commands:"
        echo "  compile  - Build the fluss_cpp library"
        echo "  example  - Build the example binary"
        echo "  run      - Build and run the example binary"
        echo "  outputs  - Show the location of build outputs"
        echo "  clean    - Clean all Bazel outputs"
        exit 1
        ;;
esac

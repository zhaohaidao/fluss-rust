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

ROOT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"
CPP_DIR="$ROOT_DIR/bindings/cpp"
BAZEL_WORK_DIR="$ROOT_DIR"
# Set Bazel output base outside bindings/cpp to avoid cargo scanning sandbox dirs.
BAZEL_OUTPUT_BASE="$ROOT_DIR/bazel-build/.bazel-output-base"
BAZEL_BUILD_FLAGS="${BAZEL_BUILD_FLAGS:--c opt}"
BAZEL_PROXY_URL="${BAZEL_PROXY_URL:-http://10.7.4.2:3128}"
BAZEL_NO_PROXY_DEFAULT="${BAZEL_NO_PROXY_DEFAULT:-127.0.0.1,localhost,bazel-build.devops.xiaohongshu.com,.devops.xiaohongshu.com}"
BAZEL_RELEASE_OUT_DIR="${BAZEL_RELEASE_OUT_DIR:-$ROOT_DIR/dist/bzlmod-release}"
BAZEL_RELEASE_REF="${BAZEL_RELEASE_REF:-HEAD}"

append_csv() {
    local base="$1"
    local extra="$2"
    if [ -z "$base" ]; then
        echo "$extra"
    elif [ -z "$extra" ]; then
        echo "$base"
    else
        echo "$base,$extra"
    fi
}

setup_bazel_proxy() {
    # Allow opt-out in special environments by setting BAZEL_USE_PROXY=0
    if [ "${BAZEL_USE_PROXY:-1}" = "0" ]; then
        return
    fi
    export http_proxy="${http_proxy:-$BAZEL_PROXY_URL}"
    export https_proxy="${https_proxy:-$BAZEL_PROXY_URL}"
    export HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
    export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"

    local no_proxy_extra="${BAZEL_NO_PROXY:-$BAZEL_NO_PROXY_DEFAULT}"
    export no_proxy="$(append_csv "${no_proxy:-}" "$no_proxy_extra")"
    export NO_PROXY="$(append_csv "${NO_PROXY:-}" "$no_proxy_extra")"
}

# Create output base directory if it doesn't exist
mkdir -p "$BAZEL_OUTPUT_BASE"
setup_bazel_proxy

# Wrapper function to run bazel from workspace root with a fixed output base
bazel() {
    local startup_args=(--output_base="$BAZEL_OUTPUT_BASE")
    local cmd="$1"
    shift || true
    local command_args=()
    if [ -n "${http_proxy:-}" ]; then
        command_args+=(
            --repo_env=http_proxy="$http_proxy"
            --repo_env=https_proxy="${https_proxy:-$http_proxy}"
            --repo_env=HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
            --repo_env=HTTPS_PROXY="${HTTPS_PROXY:-${https_proxy:-$http_proxy}}"
            --repo_env=no_proxy="${no_proxy:-}"
            --repo_env=NO_PROXY="${NO_PROXY:-}"
        )
        case "$cmd" in
            build|run|test|coverage)
                command_args+=(
                    --action_env=http_proxy="$http_proxy"
                    --action_env=https_proxy="${https_proxy:-$http_proxy}"
                    --action_env=HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
                    --action_env=HTTPS_PROXY="${HTTPS_PROXY:-${https_proxy:-$http_proxy}}"
                    --action_env=no_proxy="${no_proxy:-}"
                    --action_env=NO_PROXY="${NO_PROXY:-}"
                )
                ;;
        esac
    fi
    (cd "$BAZEL_WORK_DIR" && command bazel "${startup_args[@]}" "$cmd" "${command_args[@]}" "$@")
}

check_protoc() {
    if [ "${FLUSS_STRICT_PROTOC_CHECK:-}" = "1" ]; then
        if [ -n "${PROTOC:-}" ] && [ ! -x "$PROTOC" ]; then
            echo "ERROR: PROTOC is set but not executable: $PROTOC" >&2
            exit 1
        fi
        if [ -z "${PROTOC:-}" ] && ! command -v protoc >/dev/null 2>&1; then
            echo "ERROR: protoc not found. Install protobuf-compiler or set PROTOC." >&2
            exit 1
        fi
    fi

    if [ -n "${PROTOC:-}" ]; then
        if [ -x "$PROTOC" ]; then
            export PROTOC
        else
            echo "WARN: PROTOC is set but not executable: $PROTOC. Ignoring." >&2
            unset PROTOC
        fi
        return 0
    fi

    if command -v protoc >/dev/null 2>&1; then
        PROTOC="$(command -v protoc)"
        export PROTOC
        return 0
    fi

    echo "WARN: protoc not found. Bazel will use @com_google_protobuf//:protoc." >&2
}

search_file() {
    local pattern="$1"
    local file="$2"
    grep -nE "$pattern" "$file"
}

print_file_section() {
    local file="$1"
    local start="${2:-1}"
    local end="${3:-200}"
    if [ -f "$file" ]; then
        sed -n "${start},${end}p" "$file"
    else
        echo "Missing file: $file"
    fi
}

debug_env() {
    echo "=== Debug context ==="
    echo "ROOT_DIR=$ROOT_DIR"
    echo "CPP_DIR=$CPP_DIR"
    echo "PWD=$(pwd -P)"
    echo "uname=$(uname -a)"
    echo "whoami=$(whoami)"
    echo "PATH=$PATH"
    echo "RUST_LOG=${RUST_LOG:-}"
    echo "FLUSS_LZ4_MODE=${FLUSS_LZ4_MODE:-}"
    echo "http_proxy=${http_proxy:-}"
    echo "https_proxy=${https_proxy:-}"
    echo "no_proxy=${no_proxy:-}"
    echo "NO_PROXY=${NO_PROXY:-}"
    echo "BAZEL_PROXY_URL=${BAZEL_PROXY_URL:-}"
    echo "BAZEL_NO_PROXY=${BAZEL_NO_PROXY:-}"
    echo ""
    echo "=== Top-level files ==="
    ls -la "$ROOT_DIR" || true
    echo ""
    echo "=== Workspace Cargo.toml (patch) ==="
    if [ -f "$ROOT_DIR/Cargo.toml" ]; then
        search_file "patch.crates-io|arrow-ipc|arrow" "$ROOT_DIR/Cargo.toml" || true
        echo "--- Cargo.toml head ---"
        print_file_section "$ROOT_DIR/Cargo.toml" 1 200
    else
        echo "Cargo.toml missing at $ROOT_DIR"
    fi
    echo ""
    echo "=== Cargo.lock arrow-ipc ==="
    if [ -f "$ROOT_DIR/Cargo.lock" ]; then
        grep -nE "name = \"arrow-ipc\"|source = " "$ROOT_DIR/Cargo.lock" || true
    else
        echo "Cargo.lock missing at $ROOT_DIR"
    fi
    echo ""
    echo "=== third_party/arrow-ipc ==="
    if [ -d "$ROOT_DIR/third_party/arrow-ipc" ]; then
        ls -la "$ROOT_DIR/third_party/arrow-ipc" || true
    else
        echo "third_party/arrow-ipc missing at $ROOT_DIR/third_party/arrow-ipc"
    fi
    echo ""
    # echo "=== cargo metadata (arrow-ipc) ==="
    # if command -v cargo >/dev/null 2>&1; then
    #     (cd "$ROOT_DIR" && cargo metadata --format-version=1 2>/dev/null | grep -nE "arrow-ipc|third_party/arrow-ipc|source") || true
    # else
    #     echo "cargo not found"
    # fi
    echo "=== Debug context end ==="
}

check_arrow_ipc_patch() {
    # Temporarily skip arrow-ipc metadata/patch checks.
    # Do not run debug probes in normal CI path.
    echo "WARN: skip check_arrow_ipc_patch temporarily"
    return 0

    # if [ ! -d "$ROOT_DIR/third_party/arrow-ipc" ]; then
    #     echo "ERROR: third_party/arrow-ipc is missing. Patch cannot be applied." >&2
    #     exit 1
    # fi
    # if ! search_file "patch.crates-io" "$ROOT_DIR/Cargo.toml" >/dev/null 2>&1; then
    #     echo "ERROR: Cargo.toml missing [patch.crates-io] section." >&2
    #     exit 1
    # fi
    # if ! search_file "arrow-ipc.*third_party/arrow-ipc" "$ROOT_DIR/Cargo.toml" >/dev/null 2>&1; then
    #     echo "ERROR: Cargo.toml missing arrow-ipc patch to third_party/arrow-ipc." >&2
    #     exit 1
    # fi
}

compile() {
    check_arrow_ipc_patch
    check_protoc
    bazel build ${BAZEL_BUILD_FLAGS} //:fluss_cpp
}

resolve_module_name() {
    local module_file="$ROOT_DIR/MODULE.bazel"
    if [ ! -f "$module_file" ]; then
        echo "ERROR: MODULE.bazel not found at $module_file" >&2
        exit 1
    fi

    local module_name
    module_name="$(awk -F'"' '/^[[:space:]]*name[[:space:]]*=/ {print $2; exit}' "$module_file")"
    if [ -z "$module_name" ]; then
        echo "ERROR: Failed to parse module name from $module_file" >&2
        exit 1
    fi
    echo "$module_name"
}

release_pkg() {
    if [ -z "${BAZEL_RELEASE_VERSION:-}" ]; then
        echo "ERROR: BAZEL_RELEASE_VERSION is required for release_pkg." >&2
        echo "Example: BAZEL_RELEASE_VERSION=0.1.0 sh ci.sh release_pkg" >&2
        exit 1
    fi

    local script="$ROOT_DIR/tools/bazel/prepare_bzlmod_release.sh"
    if [ ! -x "$script" ]; then
        echo "ERROR: Missing executable release helper: $script" >&2
        exit 1
    fi

    local args=(
        --version "$BAZEL_RELEASE_VERSION"
        --ref "${BAZEL_RELEASE_REF}"
        --output-dir "$BAZEL_RELEASE_OUT_DIR"
    )
    if [ -n "${BAZEL_RELEASE_ARCHIVE_URL:-}" ]; then
        args+=(--archive-url "$BAZEL_RELEASE_ARCHIVE_URL")
    fi

    "$script" "${args[@]}"
}

upload_pkg() {
    release_pkg

    if [ -z "${BAZEL_RELEASE_UPLOAD_BASE_URL:-}" ]; then
        echo "ERROR: BAZEL_RELEASE_UPLOAD_BASE_URL is required for upload_pkg." >&2
        echo "Example: BAZEL_RELEASE_UPLOAD_BASE_URL=https://artifact.example.com/fluss-rust BAZEL_RELEASE_VERSION=0.1.0 sh ci.sh upload_pkg" >&2
        exit 1
    fi

    local module_name
    module_name="$(resolve_module_name)"
    local archive_name="${module_name}-${BAZEL_RELEASE_VERSION}.tar.gz"
    local archive_path="$BAZEL_RELEASE_OUT_DIR/archives/$archive_name"
    if [ ! -f "$archive_path" ]; then
        echo "ERROR: Release archive not found at $archive_path" >&2
        exit 1
    fi

    local upload_url="${BAZEL_RELEASE_UPLOAD_BASE_URL%/}/$archive_name"
    local upload_method="${BAZEL_RELEASE_UPLOAD_METHOD:-PUT}"
    local curl_args=(
        -fL
        --retry 3
        --retry-all-errors
        -X "$upload_method"
        -T "$archive_path"
    )

    if [ -n "${BAZEL_RELEASE_UPLOAD_TOKEN:-}" ]; then
        curl_args+=(-H "Authorization: Bearer ${BAZEL_RELEASE_UPLOAD_TOKEN}")
    fi
    if [ -n "${BAZEL_RELEASE_AUTH_HEADER:-}" ]; then
        curl_args+=(-H "${BAZEL_RELEASE_AUTH_HEADER}")
    fi

    # Avoid leaking auth headers via xtrace when credentials are provided.
    if [ -n "${BAZEL_RELEASE_UPLOAD_TOKEN:-}" ] || [ -n "${BAZEL_RELEASE_AUTH_HEADER:-}" ]; then
        set +x
        curl "${curl_args[@]}" "$upload_url"
        set -x
    else
        curl "${curl_args[@]}" "$upload_url"
    fi

    # Regenerate source.json with final uploaded URL.
    BAZEL_RELEASE_ARCHIVE_URL="$upload_url" release_pkg
    echo "Uploaded archive: $upload_url"
}

build_example() {
    check_arrow_ipc_patch
    check_protoc
    bazel build ${BAZEL_BUILD_FLAGS} //:consume_table
}

run_example() {
    build_example
    # Pass all arguments after 'run' to consume_table.
    shift
    # Propagate RUST_LOG to the runtime environment.
    bazel run ${BAZEL_BUILD_FLAGS} //:consume_table --action_env=RUST_LOG="${RUST_LOG:-warn}" -- "$@"
}

clean() {
    bazel clean
    # Remove bazel-* symlinks (Bazel automatically creates these)
    rm -rf "$ROOT_DIR"/bazel-*
    rm -rf "$CPP_DIR"/bazel-*
    # Also remove the bazel-build directory if it exists
    if [ -d "$ROOT_DIR/bazel-build" ]; then
        rm -rf "$ROOT_DIR/bazel-build"
    fi
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
    release_pkg )
        release_pkg
        ;;
    upload_pkg )
        upload_pkg
        ;;
    clean )
        clean
        ;;
    * )
        echo "Usage: $0 {compile|example|run|outputs|release_pkg|upload_pkg|clean}"
        echo ""
        echo "Commands:"
        echo "  compile  - Build the fluss_cpp library"
        echo "  example  - Build the example binary"
        echo "  run      - Build and run the example binary"
        echo "  outputs  - Show the location of build outputs"
        echo "  release_pkg - Generate bzlmod release assets (archive + registry files)"
        echo "  upload_pkg  - Upload archive then regenerate source.json with uploaded URL"
        echo "  clean    - Clean all Bazel outputs"
        echo ""
        echo "Release env vars:"
        echo "  BAZEL_RELEASE_VERSION       - Required for release_pkg/upload_pkg"
        echo "  BAZEL_RELEASE_REF           - Git ref for source archive (default: HEAD)"
        echo "  BAZEL_RELEASE_OUT_DIR       - Output directory (default: dist/bzlmod-release)"
        echo "  BAZEL_RELEASE_ARCHIVE_URL   - Final archive URL for source.json"
        echo "  BAZEL_RELEASE_UPLOAD_BASE_URL - Base URL used by upload_pkg"
        echo "  BAZEL_RELEASE_UPLOAD_METHOD - Upload HTTP method (default: PUT)"
        echo "  BAZEL_RELEASE_UPLOAD_TOKEN  - Optional bearer token for upload_pkg"
        echo "  BAZEL_RELEASE_AUTH_HEADER   - Optional custom auth header for upload_pkg"
        exit 1
        ;;
esac

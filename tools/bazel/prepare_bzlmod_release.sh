#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MODULE_FILE="$ROOT_DIR/MODULE.bazel"
META_FILE="$ROOT_DIR/bazel_meta.json"

VERSION=""
REF="HEAD"
ARCHIVE_URL=""
OUTPUT_DIR="$ROOT_DIR/dist/bzlmod-release"

usage() {
    cat <<'EOF'
Usage:
  prepare_bzlmod_release.sh --version <version> [options]

Options:
  --version <version>        Release version to publish (required).
  --ref <git-ref>            Source git ref for archive. Default: HEAD.
  --archive-url <url>        Public URL for the uploaded source archive.
  --output-dir <dir>         Output root directory. Default: dist/bzlmod-release.
  --help                     Show this help text.

Outputs:
  <output-dir>/archives/<module>-<version>.tar.gz
  <output-dir>/modules/<module>/metadata.json
  <output-dir>/modules/<module>/<version>/MODULE.bazel
  <output-dir>/modules/<module>/<version>/source.json
  <output-dir>/modules/<module>/<version>/checksums.txt
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            VERSION="${2:-}"
            shift 2
            ;;
        --ref)
            REF="${2:-}"
            shift 2
            ;;
        --archive-url)
            ARCHIVE_URL="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="${2:-}"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    echo "ERROR: --version is required." >&2
    usage
    exit 1
fi

if [[ ! -f "$MODULE_FILE" ]]; then
    echo "ERROR: MODULE.bazel not found at $MODULE_FILE" >&2
    exit 1
fi

MODULE_NAME="$(awk -F'"' '/^[[:space:]]*name[[:space:]]*=/ {print $2; exit}' "$MODULE_FILE")"
if [[ -z "$MODULE_NAME" ]]; then
    echo "ERROR: Failed to parse module name from $MODULE_FILE" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR/archives"
MODULE_ROOT="$OUTPUT_DIR/modules/$MODULE_NAME"
VERSION_DIR="$MODULE_ROOT/$VERSION"
mkdir -p "$VERSION_DIR"

ARCHIVE_NAME="${MODULE_NAME}-${VERSION}.tar.gz"
ARCHIVE_PATH="$OUTPUT_DIR/archives/$ARCHIVE_NAME"

echo "Preparing source archive: $ARCHIVE_PATH"
git -C "$ROOT_DIR" archive --format=tar.gz --prefix="${MODULE_NAME}-${VERSION}/" -o "$ARCHIVE_PATH" "$REF"

if command -v sha256sum >/dev/null 2>&1; then
    SHA256_HEX="$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
    SHA256_HEX="$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
else
    echo "ERROR: Neither sha256sum nor shasum is available." >&2
    exit 1
fi

if command -v openssl >/dev/null 2>&1; then
    SHA256_B64="$(openssl dgst -sha256 -binary "$ARCHIVE_PATH" | openssl base64 -A)"
elif command -v xxd >/dev/null 2>&1 && command -v base64 >/dev/null 2>&1; then
    SHA256_B64="$(printf '%s' "$SHA256_HEX" | xxd -r -p | base64 | tr -d '\n')"
else
    echo "ERROR: Missing tool to build SRI integrity (openssl or xxd+base64)." >&2
    exit 1
fi
INTEGRITY="sha256-${SHA256_B64}"

echo "Generating registry MODULE.bazel for version $VERSION"
if ! awk -v new_version="$VERSION" '
BEGIN { replaced = 0 }
/^[[:space:]]*version[[:space:]]*=/ && replaced == 0 {
    sub(/"[^"]*"/, "\"" new_version "\"")
    replaced = 1
}
{ print }
END {
    if (replaced == 0) {
        exit 42
    }
}
' "$MODULE_FILE" > "$VERSION_DIR/MODULE.bazel"; then
    echo "ERROR: Failed to inject version into MODULE.bazel." >&2
    exit 1
fi

SOURCE_URL="$ARCHIVE_URL"
if [[ -z "$SOURCE_URL" ]]; then
    SOURCE_URL="__REPLACE_WITH_ARCHIVE_URL__/$ARCHIVE_NAME"
fi

cat > "$VERSION_DIR/source.json" <<EOF
{
  "url": "$SOURCE_URL",
  "integrity": "$INTEGRITY",
  "strip_prefix": "${MODULE_NAME}-${VERSION}"
}
EOF

cat > "$VERSION_DIR/checksums.txt" <<EOF
sha256_hex=$SHA256_HEX
sha256_integrity=$INTEGRITY
archive_name=$ARCHIVE_NAME
archive_path=$ARCHIVE_PATH
EOF

HOMEPAGE=""
MAINTAINERS_JSON="[]"
if [[ -f "$META_FILE" ]]; then
    if command -v jq >/dev/null 2>&1; then
        HOMEPAGE="$(jq -r '.homepage // empty' "$META_FILE")"
        MAINTAINERS_JSON="$(jq -c '.maintainers // []' "$META_FILE")"
    else
        HOMEPAGE="$(awk -F'"' '/"homepage"/ {print $4; exit}' "$META_FILE")"
    fi
fi

cat > "$MODULE_ROOT/metadata.json" <<EOF
{
  "homepage": "$HOMEPAGE",
  "maintainers": $MAINTAINERS_JSON,
  "versions": ["$VERSION"]
}
EOF

cat > "$OUTPUT_DIR/README.txt" <<EOF
Bzlmod release assets are ready.

Archive:
  $ARCHIVE_PATH

Registry module root:
  $MODULE_ROOT

Next steps:
  1) Upload archive to artifact storage.
  2) Replace source.json url placeholder if needed:
     $VERSION_DIR/source.json
  3) Commit the generated module files to your Bazel registry repository.
EOF

echo "Done."
echo "  module:   $MODULE_NAME"
echo "  version:  $VERSION"
echo "  archive:  $ARCHIVE_PATH"
echo "  sri:      $INTEGRITY"
echo "  registry: $VERSION_DIR"

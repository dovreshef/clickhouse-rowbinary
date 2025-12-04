#!/usr/bin/env bash
set -euo pipefail

TEST_FLAGS=()

usage() {
  echo "Usage: $0 [-r|--release]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--release)
      TEST_FLAGS=(--release)
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

echo "Running format"
cargo +nightly fmt --all

echo "Running debug build"
RUSTFLAGS="-D warnings" cargo build

echo "Running Clippy lints"
cargo clippy --all-targets --all-features

echo "Running tests"
CLICKHOUSE_DSN="${CLICKHOUSE_DSN:-http://user:pass@127.0.0.1:8123}"
TEST_THREADS="${TEST_THREADS:-1}"
env "CLICKHOUSE_DSN=$CLICKHOUSE_DSN" cargo test "${TEST_FLAGS[@]}" -- --test-threads "$TEST_THREADS"

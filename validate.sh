#!/usr/bin/env bash
set -euo pipefail

TEST_FLAGS=()
SKIP_PYTHON=false

usage() {
  echo "Usage: $0 [-r|--release] [--skip-python]"
  echo "  -r, --release    Run tests in release mode"
  echo "  --skip-python    Skip Python tests and linting"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--release)
      TEST_FLAGS=(--release)
      shift
      ;;
    --skip-python)
      SKIP_PYTHON=true
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

echo "=== Running Rust format ==="
cargo +nightly fmt --all

echo "=== Running Rust debug build (core library) ==="
RUSTFLAGS="-D warnings" cargo build -p clickhouse_rowbinary

echo "=== Running Clippy lints (core library) ==="
cargo clippy -p clickhouse_rowbinary --all-targets --all-features

echo "=== Running Rust tests (core library) ==="
CLICKHOUSE_DSN="${CLICKHOUSE_DSN:-http://user:pass@127.0.0.1:8123}"
TEST_THREADS="${TEST_THREADS:-16}"
env "CLICKHOUSE_DSN=$CLICKHOUSE_DSN" cargo test -p clickhouse_rowbinary "${TEST_FLAGS[@]}" -- --test-threads "$TEST_THREADS"

if [ "$SKIP_PYTHON" = false ]; then
  echo "=== Building Python package ==="
  RUSTFLAGS="-D warnings" uv run maturin develop

  echo "=== Running Clippy lints (Python bindings) ==="
  cargo clippy -p clickhouse_rowbinary_py --all-targets --all-features

  echo "=== Running Python linting (ruff) ==="
  uv run ruff check python/ tests/python/

  echo "=== Running Python formatting check (ruff) ==="
  uv run ruff format --check python/ tests/python/

  echo "=== Running Python type checking (pyright) ==="
  uv run pyright python/ tests/python/ || echo "Warning: pyright check failed (optional)"

  echo "=== Running Python tests ==="
  uv run pytest tests/python/ -v
fi

echo "=== All checks passed! ==="

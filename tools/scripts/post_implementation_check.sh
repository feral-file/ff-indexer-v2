#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

DEFAULT_BASE_REF="main"
if git rev-parse --verify --quiet "origin/main" >/dev/null; then
  DEFAULT_BASE_REF="origin/main"
fi

BASE_REF="${BASE_REF:-$DEFAULT_BASE_REF}"
if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
  echo "Base ref '$BASE_REF' was not found. Set BASE_REF to an existing branch or remote ref." >&2
  exit 1
fi

MERGE_BASE="$(git merge-base HEAD "$BASE_REF")"

run_golangci_lint() {
  if command -v golangci-lint >/dev/null 2>&1; then
    golangci-lint "$@"
    return
  fi

  GOFLAGS='' go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5.0 "$@"
}

read_lines_into_array() {
  local array_name="$1"
  local line
  eval "$array_name=()"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    eval "$array_name+=(\"\$line\")"
  done
}

echo "==> Diff base: $BASE_REF ($MERGE_BASE)"

read_lines_into_array changed_go_files < <(
  {
    git diff --name-only --diff-filter=ACMR "$MERGE_BASE" -- '*.go'
    git ls-files --others --exclude-standard -- '*.go'
  } | LC_ALL=C sort -u
)

if ((${#changed_go_files[@]} == 0)); then
  echo "==> No changed Go files relative to $BASE_REF; skipping changed-file lint."
else
  echo "==> Checking formatting for changed Go files"
  read_lines_into_array unformatted_files < <(gofmt -s -l "${changed_go_files[@]}")
  if ((${#unformatted_files[@]} > 0)); then
    echo "Run 'gofmt -s -w' on these files before re-running:" >&2
    printf ' - %s\n' "${unformatted_files[@]}" >&2
    exit 1
  fi

  # echo "==> Running strict whole-file lint on changed Go files"
  # run_golangci_lint run \
  #   --new-from-rev "$MERGE_BASE" \
  #   --whole-files \
  #   --path-mode abs \
  #   "${changed_go_files[@]}"
fi

export TEST_DB_HOST="${TEST_DB_HOST:-localhost}"
export TEST_DB_PORT="${TEST_DB_PORT:-5432}"
export TEST_DB_USER="${TEST_DB_USER:-postgres}"
export TEST_DB_PASSWORD="${TEST_DB_PASSWORD:-postgres}"
export TEST_DB_NAME="${TEST_DB_NAME:-test_db}"

if command -v pg_isready >/dev/null 2>&1; then
  if ! pg_isready -h "$TEST_DB_HOST" -p "$TEST_DB_PORT" -U "$TEST_DB_USER" >/dev/null 2>&1; then
    echo "PostgreSQL is not reachable at ${TEST_DB_HOST}:${TEST_DB_PORT}." >&2
    echo "Start the local integration dependencies first, for example with 'make dev'." >&2
    exit 1
  fi
elif command -v nc >/dev/null 2>&1; then
  if ! nc -z "$TEST_DB_HOST" "$TEST_DB_PORT" >/dev/null 2>&1; then
    echo "PostgreSQL is not reachable at ${TEST_DB_HOST}:${TEST_DB_PORT}." >&2
    echo "Start the local integration dependencies first, for example with 'make dev'." >&2
    exit 1
  fi
fi

read_lines_into_array test_packages < <(go list ./... | grep -vE '/internal/mocks|/internal/adapter|cmd|internal/store/schema|tools')
if ((${#test_packages[@]} == 0)); then
  echo "No Go packages matched the CI test set." >&2
  exit 1
fi

echo "==> Running CI-aligned Go test suite with coverage"
go test -v -coverprofile=coverage.out -covermode=atomic "${test_packages[@]}"

if [[ -f coverage.out ]]; then
  grep -v 'generated.go' coverage.out > coverage.filtered.out || true
  mv coverage.filtered.out coverage.out

  echo "==> Coverage summary"
  go tool cover -func=coverage.out | tail -n 1
fi

echo "==> Post-implementation checks passed"

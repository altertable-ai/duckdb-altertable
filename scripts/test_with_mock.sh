#!/usr/bin/env bash
# Run `make test` with the altertable-mock Docker container.
#
# In CI, the mock is assumed to already be running as a service container on
# localhost:15002, so no container is started. Locally, this script pulls and
# starts the container, exports the required ALTERTABLE_TEST_* env vars, runs
# the tests, and stops the container on exit.

set -euo pipefail

cd "$(dirname "$0")/.."

# Use the exact mock image CI uses, so local runs match CI. The digest lives in
# the workflow's service definition; bump it there (and only there). To try
# another image, set ALTERTABLE_MOCK_IMAGE, e.g.
#   ALTERTABLE_MOCK_IMAGE=ghcr.io/altertable-ai/altertable-mock:latest make test-mock
CI_WORKFLOW=".github/workflows/MainDistributionPipeline.yml"
if [[ -n "${ALTERTABLE_MOCK_IMAGE:-}" ]]; then
  MOCK_IMAGE="${ALTERTABLE_MOCK_IMAGE}"
else
  MOCK_IMAGE=$(grep -oE 'ghcr\.io/altertable-ai/altertable-mock@sha256:[0-9a-f]{64}' "${CI_WORKFLOW}" | head -n 1 || true)
  if [[ -z "${MOCK_IMAGE}" ]]; then
    echo "ERROR: could not find the pinned altertable-mock image in ${CI_WORKFLOW}." >&2
    exit 1
  fi
fi
MOCK_USER="${ALTERTABLE_MOCK_USER:-testuser}"
MOCK_PASS="${ALTERTABLE_MOCK_PASS:-testpass}"
MOCK_PORT=15002
CONTAINER_NAME="altertable-mock-test"

# ---------------------------------------------------------------------------
# Determine host/port depending on whether we're in CI or running locally
# ---------------------------------------------------------------------------
if [[ "${CI:-}" == "true" ]]; then
  HOST="localhost"
  PORT="${MOCK_PORT}"
else
  # Ensure Docker is available
  if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker is not installed or not on PATH." >&2
    exit 1
  fi

  # Pull the mock image
  echo "==> Pulling ${MOCK_IMAGE} ..."
  docker pull "${MOCK_IMAGE}"

  # Remove any leftover container from a previous run
  docker rm -f "${CONTAINER_NAME}" &>/dev/null || true

  # Start the container; let Docker pick a free host port
  echo "==> Starting altertable-mock container ..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "127.0.0.1::${MOCK_PORT}" \
    -e "ALTERTABLE_MOCK_USERS=${MOCK_USER}:${MOCK_PASS}" \
    "${MOCK_IMAGE}"

  # Stop & remove the container when this script exits (for any reason)
  cleanup() {
    echo "==> Stopping altertable-mock container ..."
    docker rm -f "${CONTAINER_NAME}" &>/dev/null || true
  }
  trap cleanup EXIT

  echo "==> Waiting for altertable-mock to accept connections ..."
  PORT=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "15002/tcp") 0).HostPort}}' "${CONTAINER_NAME}")
  ready_checks=0
  for _ in {1..30}; do
    if (echo >"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then
      ready_checks=$((ready_checks + 1))
      if [[ "${ready_checks}" -ge 5 ]]; then
        break
      fi
    else
      ready_checks=0
    fi
    sleep 1
  done
  if ! (echo >"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then
    echo "ERROR: altertable-mock did not accept connections on port ${PORT}." >&2
    exit 1
  fi
  echo "==> altertable-mock is ready on port ${PORT}."
  HOST="127.0.0.1"
fi

# ---------------------------------------------------------------------------
# Export env vars consumed by sqllogictest require-env directives
# ---------------------------------------------------------------------------
export ALTERTABLE_TEST_HOST="${HOST}"
export ALTERTABLE_TEST_PORT="${PORT}"
export ALTERTABLE_TEST_USER="${MOCK_USER}"
export ALTERTABLE_TEST_PASSWORD="${MOCK_PASS}"
export ALTERTABLE_TEST_SSL="false"

# ---------------------------------------------------------------------------
# Run the tests (pass through any extra arguments, e.g. a single test file)
# ---------------------------------------------------------------------------
echo "==> Running tests (host=${ALTERTABLE_TEST_HOST} port=${ALTERTABLE_TEST_PORT}) ..."
if [[ $# -gt 0 ]]; then
  # Run a single test file directly via the unittest binary
  ./build/release/test/unittest "$@"
else
  # One test worker: the mock (like hosted Altertable) stores tables in DuckLake,
  # and concurrent writes from parallel test workers exhaust DuckLake's commit
  # retries ("Exceeded the maximum retry count ... ducklake_max_retry_count").
  make test TEST_RUNNER="python3 ./duckdb/scripts/ci/run_tests.py --workers 1"
fi

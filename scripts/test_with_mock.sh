#!/usr/bin/env bash
# Run `make test` with the altertable-mock Docker container.
#
# In CI, the mock is assumed to already be running as a service container on
# localhost:15002, so no container is started. Locally, this script pulls and
# starts the container, exports the required ALTERTABLE_TEST_* env vars, runs
# the tests, and stops the container on exit.

set -euo pipefail

MOCK_IMAGE="ghcr.io/altertable-ai/altertable-mock:latest"
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

  # Pull the latest image
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

  echo "==> Waiting for altertable-mock to be ready ..."
  PORT=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "15002/tcp") 0).HostPort}}' "${CONTAINER_NAME}")
  sleep 5 # TODO: ping Arrow Flight SQL
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
  make test
fi

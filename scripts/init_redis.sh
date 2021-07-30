#!/usr/bin/env bash
set -x
set -eo pipefail

# Check if a custom port has been set, otherwise default to '6379'
PORT="${REDIS_PORT:=6379}"

docker run \
  -p "${PORT}":6379 \
  -d redis:5.0

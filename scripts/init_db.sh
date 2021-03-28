#!/usr/bin/env bash

set -x
set -eo pipefail

DB_PORT="${MONGO_PORT:=27017}"

if [[ -z "${SKIP_DOCKER}" ]]
then
docker run \
    -p "${DB_PORT}":27017 \
    -d \
    mongo
fi

export DATABASE_URL=mongodb://localhost:${DB_PORT}

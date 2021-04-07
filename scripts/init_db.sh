#!/usr/bin/env bash

set -x
set -eo pipefail

DB_USER=${MONGO_USER:=mongo}
DB_PASSWORD="${MONGO_PASSWORD:=password}"
DB_NAME="${MONGO_DB:=ordermanagerdb}"
DB_PORT="${MONGO_PORT:=27017}"

if [[ -z "${SKIP_DOCKER}" ]]
then
docker run \
    -e MONGO_INITDB_ROOT_USERNAME="${DB_USER}" \
    -e MONGO_INITDB_ROOT_PASSWORD="${DB_PASSWORD}" \
    -p "${DB_PORT}":27017 \
    -d \
    mongo
fi

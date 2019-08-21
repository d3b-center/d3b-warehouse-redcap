#!/usr/bin/env bash
set -e

cmd="$1"

until psql -h "$PG_HOST" -U "$PG_USER" "$PG_NAME" -c '\q'; do
    >&2 echo "Postgres is unavailable - sleeping"
    sleep 1
done

>&2 echo "Postgres is up - executing command"
exec $cmd

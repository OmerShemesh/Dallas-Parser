#!/usr/bin/env bash
if ! [ -x "$(command -V dropdb)" ]; then
    echo "Please ensure Postgresql is installed (dropdb command not found)"
    exit 1
elif [ -z "$1" ]; then
    echo "Please provide a path for the PostgreSQL dump file!"
    exit 1
elif [ -z "$2" ]; then
    echo "Please provide the engine's FQDN!"
    exit 1
else
    DUMP_PATH="$1"
    FQDN="$2"
    dropdb -h localhost -U omer $FQDN
    createdb -h localhost -U omer $FQDN
    pg_restore --host localhost --username omer --dbname $FQDN --verbose "$DUMP_PATH"

    python3 db_parser.py $FQDN omer $FQDN
fi


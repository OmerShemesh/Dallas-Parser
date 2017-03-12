#!/usr/bin/env bash
if [ -z "$1" ]; then
    echo "Please provide a path for the PostgreSQL dump file!"
    exit 1
elif [ -z "$2" ]; then
    echo "Please provide the engine's FQDN!"
    exit 1
else
    DUMP_PATH="$1"
    FQDN="$2"
    dropdb -h localhost -U omer dallas
    createdb -h localhost -U omer dallas
    pg_restore --host localhost --username omer --dbname dallas --verbose "$DUMP_PATH"

    python3 db_parser.py dallas omer $FQDN
fi


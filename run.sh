#!/usr/bin/env bash
if [[ $# -eq 0 ]]; then
    echo 'Please provide a path for the PostgreSQL dump file!'
    exit 1
else
    createdb -h localhost -U omer lago
    pg_restore --host localhost --username omer --dbname lago --verbose $1

    python3 db_parser.py lago omer
fi


#!/usr/bin/env bash
# Creates the read-only role the monitor service connects as.
# Mounted next to init.sql so the role gets added upon creating the database
# Run by hand using: docker compose exec db bash /docker-entrypoint-initdb.d/monitor-role.sh
set -euo pipefail

: "${MONITOR_DB_USER:?MONITOR_DB_USER must be set in .env}"
: "${MONITOR_DB_PASSWORD:?MONITOR_DB_PASSWORD must be set in .env}"

psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -v monitor_user="$MONITOR_DB_USER" \
    -v monitor_password="$MONITOR_DB_PASSWORD" \
    -v db_name="$POSTGRES_DB" \
    -v owner="$POSTGRES_USER" \
    <<'EOSQL'
CREATE ROLE :"monitor_user" LOGIN PASSWORD :'monitor_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 4;

GRANT CONNECT ON DATABASE :"db_name" TO :"monitor_user";
GRANT USAGE ON SCHEMA public TO :"monitor_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"monitor_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
    GRANT SELECT ON TABLES TO :"monitor_user";
ALTER ROLE :"monitor_user" SET default_transaction_read_only = on;
EOSQL

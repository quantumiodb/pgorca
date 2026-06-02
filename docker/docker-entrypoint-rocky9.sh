#!/usr/bin/env bash
# Minimal first-boot entrypoint for the Rocky 9 pg_orca image.
#
# Mirrors the surface area of docker-library/postgres that pg_orca's bootstrap
# SQL depends on: honors POSTGRES_PASSWORD / POSTGRES_USER / POSTGRES_DB, runs
# initdb on first start, processes /docker-entrypoint-initdb.d/*.{sql,sh} once,
# and then execs `postgres` in the foreground.  Re-running on an existing PGDATA
# is idempotent — the init steps are skipped.

set -Eeuo pipefail

: "${PGDATA:=/var/lib/pgsql/18/data}"
: "${POSTGRES_USER:=postgres}"
: "${POSTGRES_DB:=${POSTGRES_USER}}"

PG_BIN=/usr/pgsql-18/bin

# Running as root: chown the (possibly bind-mounted / volume) PGDATA so the
# postgres user can write, then re-exec ourselves under that user.  Rocky's
# postgresql18-server package creates the postgres account with home
# /var/lib/pgsql.
if [ "$(id -u)" = '0' ]; then
    mkdir -p "$PGDATA"
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    exec runuser -u postgres -- "${BASH_SOURCE[0]}" "$@"
fi

initialize_pgdata() {
    mkdir -p "$PGDATA"
    chmod 700 "$PGDATA"

    local pwfile=""
    local initdb_args=( -D "$PGDATA" --encoding=UTF8 --locale=en_US.utf8 -U "$POSTGRES_USER" )
    # Local socket stays trust so the entrypoint can run bootstrap SQL without
    # juggling PGPASSWORD; remote (host) is scram-sha-256 when a password is
    # provided, mirroring docker-library/postgres behaviour.
    if [ -n "${POSTGRES_PASSWORD:-}" ]; then
        pwfile=$(mktemp)
        printf '%s' "$POSTGRES_PASSWORD" > "$pwfile"
        initdb_args+=( --auth-local=trust --auth-host=scram-sha-256 --pwfile="$pwfile" )
    else
        if [ "${POSTGRES_HOST_AUTH_METHOD:-}" != "trust" ]; then
            echo "ERROR: set POSTGRES_PASSWORD, or POSTGRES_HOST_AUTH_METHOD=trust to allow passwordless logins." >&2
            exit 1
        fi
        initdb_args+=( --auth-local=trust --auth-host=trust )
    fi

    "$PG_BIN/initdb" "${initdb_args[@]}"

    [ -n "$pwfile" ] && rm -f "$pwfile"

    # Listen on all interfaces inside the container.
    cat >> "$PGDATA/postgresql.conf" <<'EOF'

# pg_orca container defaults
listen_addresses = '*'
EOF

    # Allow remote (TCP) connections with the chosen auth.
    local hba_auth="scram-sha-256"
    [ "${POSTGRES_HOST_AUTH_METHOD:-}" = "trust" ] && hba_auth="trust"
    cat >> "$PGDATA/pg_hba.conf" <<EOF

# pg_orca container: allow TCP from anywhere
host all all 0.0.0.0/0 ${hba_auth}
host all all ::/0      ${hba_auth}
EOF
}

create_target_db() {
    # `postgres` DB always exists; only create a custom one.
    if [ "$POSTGRES_DB" != "postgres" ]; then
        "$PG_BIN/psql" -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c \
            "CREATE DATABASE \"$POSTGRES_DB\";"
    fi
}

run_initdb_d() {
    local f
    for f in /docker-entrypoint-initdb.d/*; do
        [ -e "$f" ] || continue
        case "$f" in
            *.sh)
                echo "$0: running $f"
                # shellcheck disable=SC1090
                . "$f"
                ;;
            *.sql)
                echo "$0: applying $f"
                "$PG_BIN/psql" -v ON_ERROR_STOP=1 \
                    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
                    -f "$f"
                ;;
            *.sql.gz)
                echo "$0: applying $f"
                gunzip -c "$f" | "$PG_BIN/psql" -v ON_ERROR_STOP=1 \
                    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"
                ;;
            *)
                echo "$0: ignoring $f"
                ;;
        esac
    done
}

# First-boot init: $PGDATA exists but is empty.
if [ -z "$(ls -A "$PGDATA" 2>/dev/null || true)" ]; then
    initialize_pgdata

    # Start a private postgres just for bootstrap (local-socket only).
    "$PG_BIN/pg_ctl" -D "$PGDATA" \
        -o "-c listen_addresses='' -c unix_socket_directories=/tmp" \
        -w start

    export PGHOST=/tmp PGUSER="$POSTGRES_USER"
    create_target_db
    run_initdb_d

    "$PG_BIN/pg_ctl" -D "$PGDATA" -m fast -w stop
    unset PGHOST PGUSER

    echo
    echo "pg_orca: PostgreSQL init complete; ready to accept connections."
    echo
fi

# Hand off to postgres (or whatever CMD was passed).
if [ "${1:-}" = "postgres" ]; then
    exec "$PG_BIN/postgres" -D "$PGDATA"
fi

exec "$@"

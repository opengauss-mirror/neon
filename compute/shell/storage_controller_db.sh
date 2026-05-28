#!/usr/bin/env bash
set -eux

export OG_VERSION=${OG_VERSION:-V702}
export GAUSSHOME="/usr/local/${OG_VERSION}"
export PATH="/usr/local/${OG_VERSION}/bin:${PATH}"
SYSTEM_LD_LIBRARY_PATH=${SYSTEM_LD_LIBRARY_PATH:-/usr/lib64}
export LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}:/usr/local/${OG_VERSION}/lib:${LD_LIBRARY_PATH:-}"

readonly PGDATA=${PGDATA:-/var/db/omm/storage_controller_db}
readonly DB_USER=${DB_USER:-omm}
readonly DB_NAME=${DB_NAME:-storage_controller}

mkdir -p "${PGDATA}"

if [[ -f "${PGDATA}/postmaster.pid" ]]; then
  old_pid=$(head -n 1 "${PGDATA}/postmaster.pid" || true)
  if [[ -z "${old_pid}" ]] || ! kill -0 "${old_pid}" 2>/dev/null; then
    rm -f "${PGDATA}/postmaster.pid" "${PGDATA}/postmaster.pid.lock" "${PGDATA}/pg_ctl.lock"
  fi
fi

chmod 0700 "${PGDATA}"

if [[ ! -f "${PGDATA}/postgresql.conf" ]]; then
  gs_initdb -D "${PGDATA}" --nodename=storage_controller_db -U "${DB_USER}" -w "${DB_PASSWORD:-StorageController123!}" --auth-host=trust --auth-local=trust
  {
    echo "listen_addresses='*'"
    echo "port=5432"
  } >> "${PGDATA}/postgresql.conf"
  echo "host all all 0.0.0.0/0 trust" >> "${PGDATA}/pg_hba.conf"
fi

gs_ctl start -D "${PGDATA}" -w -t 60 -l /tmp/storage_controller_db.log

if ! gsql -h 127.0.0.1 -p 5432 -U "${DB_USER}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  gsql -h 127.0.0.1 -p 5432 -U "${DB_USER}" -d postgres -c "CREATE DATABASE ${DB_NAME}"
fi

trap 'gs_ctl stop -D "${PGDATA}" -m fast' TERM INT
tail -F /tmp/storage_controller_db.log &
wait $!

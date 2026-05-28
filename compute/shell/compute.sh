#!/usr/bin/env bash
set -eux

generate_id() {
    local -n resvar=${1}
    printf -v resvar '%08x%08x%08x%08x' ${SRANDOM} ${SRANDOM} ${SRANDOM} ${SRANDOM}
}

export OG_VERSION=${OG_VERSION:-V702}
PAGE_SERVER_PG_VERSION=${PAGE_SERVER_PG_VERSION:-14}
SYSTEM_LD_LIBRARY_PATH=${SYSTEM_LD_LIBRARY_PATH:-/usr/lib64}
export GAUSSHOME="/usr/local/${OG_VERSION}"
export PATH="/usr/local/${OG_VERSION}/bin:${PATH}"
export LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}:/usr/local/${OG_VERSION}/lib:${LD_LIBRARY_PATH:-}"
readonly GSQL="/usr/local/${OG_VERSION}/bin/gsql"

readonly CONFIG_FILE_ORG=/var/db/gaussdb/configs/config.json
readonly CONFIG_FILE=/tmp/config.json

echo "Waiting pageserver become ready."
while ! nc -z pageserver 6400; do
     sleep 1
done
echo "Page server is ready."

cp "${CONFIG_FILE_ORG}" "${CONFIG_FILE}"

if [[ -n "${TENANT_ID:-}" && -n "${TIMELINE_ID:-}" ]]; then
   tenant_id=${TENANT_ID}
   timeline_id=${TIMELINE_ID}
else
  echo "Check if a tenant present"
  PARAMS=(
       -X GET
       -H "Content-Type: application/json"
       "http://pageserver:9898/v1/tenant"
  )
  tenant_id=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}" | jq -r .[0].id)
  if [[ -z "${tenant_id}" || "${tenant_id}" = null ]]; then
    echo "Create a tenant"
    generate_id tenant_id
    PARAMS=(
         -X PUT
         -H "Content-Type: application/json"
         -d "{\"mode\": \"AttachedSingle\", \"generation\": 1, \"tenant_conf\": {}}"
        "http://pageserver:9898/v1/tenant/${tenant_id}/location_config"
    )
    result=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}")
    printf '%s\n' "${result}" | jq .
  fi

  if [[ "${RUN_PARALLEL:-false}" != "true" ]]; then
    echo "Check if a timeline present"
    PARAMS=(
         -X GET
         -H "Content-Type: application/json"
        "http://pageserver:9898/v1/tenant/${tenant_id}/timeline"
    )
    timeline_id=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}" | jq -r .[0].timeline_id)
  fi
  if [[ -z "${timeline_id:-}" || "${timeline_id:-}" = null ]]; then
    generate_id timeline_id
    PARAMS=(
        -sbf
        -X POST
        -H "Content-Type: application/json"
        -d "{\"new_timeline_id\": \"${timeline_id}\", \"pg_version\": ${PAGE_SERVER_PG_VERSION}}"
        "http://pageserver:9898/v1/tenant/${tenant_id}/timeline/"
    )
    result=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}")
    printf '%s\n' "${result}" | jq .
    if printf "%s\n" "${result}" | jq -e ".msg? // empty" >/dev/null; then
      echo "Create timeline failed: ${result}" >&2
      exit 1
    fi
  fi
fi

echo "Overwrite tenant id and timeline id in spec file"
sed -i "s|TENANT_ID|${tenant_id}|" ${CONFIG_FILE}
sed -i "s|TIMELINE_ID|${timeline_id}|" ${CONFIG_FILE}

cat ${CONFIG_FILE}

echo "Start compute node"
set +e
/usr/local/bin/compute_ctl --pgdata /var/db/gaussdb/compute \
     -C "postgresql://cloud_admin@localhost:55433/postgres"  \
     -b "/usr/local/${OG_VERSION}/bin/gaussdb"                \
     --compute-id "compute-${RANDOM}"                          \
     --privileged-role-name "cloud_admin"                         \
     --config "${CONFIG_FILE}"                                 \
     --dev &
compute_ctl_pid=$!
set -e

PG_HBA=/var/db/gaussdb/compute/pg_hba.conf
for _ in $(seq 1 120); do
  if [[ -f "${PG_HBA}" ]] && "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -c 'SELECT 1' >/dev/null 2>&1; then
    if ! grep -q '^host[[:space:]]\+all[[:space:]]\+all[[:space:]]\+0\.0\.0\.0/0[[:space:]]\+trust' "${PG_HBA}"; then
      sed -i '/^host[[:space:]]\+all[[:space:]]\+all[[:space:]]\+all[[:space:]]\+md5/i host all all 0.0.0.0/0 trust' "${PG_HBA}"
      "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -c 'SELECT pg_reload_conf()'
    fi

    current_lsn=$("${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -tAc 'SELECT pg_current_xlog_location()' | tr -d '[:space:]')
    if [[ -n "${current_lsn}" ]]; then
      wait_lsn_body=$(jq -cn --arg timeline_id "${timeline_id}" --arg lsn "${current_lsn}" '{($timeline_id): $lsn, timeout: {secs: 60, nanos: 0}}')
      LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sS -f -X POST -H 'Content-Type: application/json' \
        -d "${wait_lsn_body}" \
        "http://pageserver:9898/v1/tenant/${tenant_id}/wait_lsn" >/dev/null
    fi
    break
  fi
  if ! kill -0 "${compute_ctl_pid}" 2>/dev/null; then
    wait "${compute_ctl_pid}"
    exit $?
  fi
  sleep 1
done

wait "${compute_ctl_pid}"

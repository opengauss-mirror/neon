#!/usr/bin/env bash
set -eux

generate_id() {
    local -n resvar=${1}
    printf -v resvar '%08x%08x%08x%08x' ${SRANDOM} ${SRANDOM} ${SRANDOM} ${SRANDOM}
}

set_config_setting() {
    local name=${1}
    local value=${2}
    local vartype=${3:-string}
    local tmp
    tmp=$(mktemp)
    jq \
      --arg name "${name}" \
      --arg value "${value}" \
      --arg vartype "${vartype}" \
      '.spec.cluster.settings = ((.spec.cluster.settings // [])
        | map(select(.name != $name))
        + [{"name": $name, "value": $value, "vartype": $vartype}])' \
      "${CONFIG_FILE}" > "${tmp}"
    mv "${tmp}" "${CONFIG_FILE}"
}

set_config_include() {
    local path=${1}
    local tmp
    tmp=$(mktemp)
    jq \
      --arg path "${path}" \
      '.spec.cluster.settings = ((.spec.cluster.settings // [])
        | map(select(.name != "include_if_exists" or .value != $path))
        + [{"name": "include_if_exists", "value": $path, "vartype": "string"}])' \
      "${CONFIG_FILE}" > "${tmp}"
    mv "${tmp}" "${CONFIG_FILE}"
}

write_endpoint_record() {
    local status=${1}
    if [[ -z "${ENDPOINT_STATUS_DIR:-}" ]]; then
      return
    fi

    mkdir -p "${ENDPOINT_STATUS_DIR}"
    cp "${CONFIG_FILE}" "${ENDPOINT_STATUS_DIR}/config.json"
    chmod 0644 "${ENDPOINT_STATUS_DIR}/config.json"
    local endpoint_json="${ENDPOINT_STATUS_DIR}/endpoint.json"
    local base_json
    local tmp_json
    base_json=$(mktemp)
    tmp_json=$(mktemp)
    if [[ -f "${endpoint_json}" ]]; then
      cp "${endpoint_json}" "${base_json}"
    else
      printf '{}\n' > "${base_json}"
    fi
    jq \
      --arg endpoint_id "${ENDPOINT_ID:-compute_main}" \
      --arg tenant_id "${tenant_id}" \
      --arg timeline_id "${timeline_id}" \
      --arg service_name "${ENDPOINT_SERVICE_NAME:-${ENDPOINT_ID:-compute_main}}" \
      --arg compute_http "http://${ENDPOINT_SERVICE_NAME:-${ENDPOINT_ID:-compute_main}}:3080" \
      --arg compute_pg "${ENDPOINT_SERVICE_NAME:-${ENDPOINT_ID:-compute_main}}:55433" \
      --arg data_dir "/var/db/gaussdb/compute" \
      --arg config_path "${ENDPOINT_STATUS_DIR}/config.json" \
      --arg status "${status}" \
      '. + {
        endpoint_id: (.endpoint_id // $endpoint_id),
        tenant_id: $tenant_id,
        timeline_id: $timeline_id,
        branch_name: (.branch_name // "main"),
        mode: (.mode // "Primary"),
        pg_version: (.pg_version // 140000),
        service_name: (.service_name // $service_name),
        compute_http: (.compute_http // $compute_http),
        compute_pg: (.compute_pg // $compute_pg),
        host_pg_port: (.host_pg_port // 55433),
        host_http_port: (.host_http_port // 3080),
        data_dir: (.data_dir // $data_dir),
        config_path: $config_path,
        grpc: (.grpc // false),
        skip_pg_catalog_updates: (.skip_pg_catalog_updates // false),
        privileged_role_name: (.privileged_role_name // "cloud_admin"),
        status: $status
      }' "${base_json}" > "${tmp_json}"
    mv "${tmp_json}" "${endpoint_json}"
    rm -f "${base_json}"
    chmod 0644 "${endpoint_json}"
}

timeline_exists() {
    local tenant_id=${1}
    local timeline_id=${2}
    LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf \
      "${STORAGE_CONTROLLER_HTTP}/control/v1/tenant/${tenant_id}/timeline/${timeline_id}" >/dev/null
}

timeline_last_record_lsn() {
    local tenant_id=${1}
    local timeline_id=${2}
    LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf \
      "${STORAGE_CONTROLLER_HTTP}/control/v1/tenant/${tenant_id}/timeline/${timeline_id}" \
      | jq -r '.shards[0].last_record_lsn'
}

locate_tenant() {
    local tenant_id=${1}
    LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf \
      "${STORAGE_CONTROLLER_HTTP}/debug/v1/tenant/${tenant_id}/locate"
}

set_pageserver_from_controller() {
    local tenant_id=${1}
    local locate
    locate=$(locate_tenant "${tenant_id}")
    pageserver_pg_host=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_pg_addr')
    pageserver_pg_port=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_pg_port')
    pageserver_http_host=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_http_addr')
    pageserver_http_port=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_http_port')
    pageserver_http="http://${pageserver_http_host}:${pageserver_http_port}"
    pageserver_connstring="host=${pageserver_pg_host} port=${pageserver_pg_port}"
}

wait_lsn_on_pageservers() {
    local tenant_id=${1}
    local timeline_id=${2}
    local lsn=${3}
    local locate
    local wait_lsn_body

    locate=$(locate_tenant "${tenant_id}")
    wait_lsn_body=$(jq -cn --arg timeline_id "${timeline_id}" --arg lsn "${lsn}" '{($timeline_id): $lsn, timeout: {secs: 60, nanos: 0}}')
    while IFS= read -r shard; do
      local shard_id
      local http_host
      local http_port
      shard_id=$(printf '%s' "${shard}" | jq -r '.shard_id')
      http_host=$(printf '%s' "${shard}" | jq -r '.listen_http_addr')
      http_port=$(printf '%s' "${shard}" | jq -r '.listen_http_port')
      LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sS -f -X POST -H 'Content-Type: application/json' \
        -d "${wait_lsn_body}" \
        "http://${http_host}:${http_port}/v1/tenant/${shard_id}/wait_lsn" >/dev/null
    done < <(printf '%s' "${locate}" | jq -c '.shards[]')
}

first_tenant_id() {
    LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf \
      "${STORAGE_CONTROLLER_HTTP}/control/v1/tenant?limit=1" \
      | jq -r '.[0].tenant_id // empty'
}

first_timeline_id() {
    local tenant_id=${1}
    local locate
    locate=$(locate_tenant "${tenant_id}")
    local http_host http_port
    http_host=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_http_addr')
    http_port=$(printf '%s' "${locate}" | jq -r '.shards[0].listen_http_port')
    LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf \
      "http://${http_host}:${http_port}/v1/tenant/${tenant_id}/timeline" \
      | jq -r '.[0].timeline_id // empty'
}

sql_literal() {
    local value=${1//\'/\'\'}
    printf "'%s'" "${value}"
}

ensure_branch_merge_user() {
    if [[ "${BRANCH_MERGE_USER_ENABLED:-true}" == "false" ]]; then
      return
    fi

    local merge_user=${BRANCH_MERGE_USER:-branch_merge}
    local merge_password=${BRANCH_MERGE_PASSWORD:-Branch_merge@123}

    if ! [[ "${merge_user}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      echo "BRANCH_MERGE_USER must be a simple SQL identifier: ${merge_user}" >&2
      exit 1
    fi
    if [[ -z "${merge_password}" ]]; then
      echo "BRANCH_MERGE_PASSWORD must not be empty when BRANCH_MERGE_USER_ENABLED=true" >&2
      exit 1
    fi

    local password_literal
    password_literal=$(sql_literal "${merge_password}")

    set +x
    if "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -tAc \
      "SELECT 1 FROM pg_roles WHERE rolname = '${merge_user}'" | grep -q 1; then
      "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 \
        -c "ALTER USER ${merge_user} WITH SYSADMIN PASSWORD ${password_literal};"
    else
      "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 \
        -c "CREATE USER ${merge_user} WITH SYSADMIN PASSWORD ${password_literal};"
    fi

    if [[ -n "${OGGIT_FDW_USERMAPPING_KEY:-}" ]]; then
      "${GAUSSHOME}/bin/gs_guc" generate \
        -S "${OGGIT_FDW_USERMAPPING_KEY}" \
        -D "${GAUSSHOME}/bin" \
        -o usermapping
    fi
    set -x
}

wait_for_compute_running() {
    local status_json
    local status

    for _ in $(seq 1 120); do
      status_json=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf http://127.0.0.1:3080/status || true)
      status=$(printf '%s' "${status_json}" | jq -r '.status // empty' 2>/dev/null || true)
      case "${status}" in
        running)
          return 0
          ;;
        failed|terminated|termination_pending_fast|termination_pending_immediate)
          echo "compute_ctl reported ${status}: ${status_json}" >&2
          return 1
          ;;
      esac

      if ! kill -0 "${compute_ctl_pid}" 2>/dev/null; then
        wait "${compute_ctl_pid}"
        return $?
      fi
      sleep 1
    done

    echo "timed out waiting for compute_ctl /status to become running" >&2
    return 1
}

export OG_VERSION=${OG_VERSION:-V702}
PAGE_SERVER_PG_VERSION=${PAGE_SERVER_PG_VERSION:-14}
SYSTEM_LD_LIBRARY_PATH=${SYSTEM_LD_LIBRARY_PATH:-/usr/lib64}
STORAGE_CONTROLLER_HTTP=${STORAGE_CONTROLLER_HTTP:-http://storage_controller:1234}
export GAUSSHOME="/usr/local/${OG_VERSION}"
export PATH="/usr/local/${OG_VERSION}/bin:${PATH}"
export LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}:/usr/local/${OG_VERSION}/lib:${LD_LIBRARY_PATH:-}"
readonly GSQL="/usr/local/${OG_VERSION}/bin/gsql"

readonly CONFIG_FILE_ORG=${COMPUTE_CONFIG_FILE:-/var/db/gaussdb/configs/config.json}
readonly CONFIG_FILE=/tmp/config.json
readonly POSTGRESQL_EXTEND_CONF=${POSTGRESQL_EXTEND_CONF:-/var/db/gaussdb/postgresql_extend.conf}

echo "Waiting storage controller become ready."
while ! LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf "${STORAGE_CONTROLLER_HTTP}/ready" >/dev/null; do
     sleep 1
done
echo "Storage controller is ready."

echo "Waiting for at least one active pageserver."
while true; do
    nodes=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl -sf "${STORAGE_CONTROLLER_HTTP}/control/v1/node" || true)
    if printf '%s' "${nodes}" | jq -e 'map(select(.availability == "Active")) | length > 0' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
echo "A pageserver is active."

cp "${CONFIG_FILE_ORG}" "${CONFIG_FILE}"
touch "${POSTGRESQL_EXTEND_CONF}"
chmod 0644 "${POSTGRESQL_EXTEND_CONF}"

if [[ "${DOCKER_CONTROL_PLANE_MODE:-false}" == "true" ]]; then
  tenant_id=$(
    jq -r '.spec.tenant_id // (.spec.cluster.settings[]? | select(.name == "neon.tenant_id") | .value) // empty' \
      "${CONFIG_FILE}"
  )
  timeline_id=$(
    jq -r '.spec.timeline_id // (.spec.cluster.settings[]? | select(.name == "neon.timeline_id") | .value) // empty' \
      "${CONFIG_FILE}"
  )
  if [[ -z "${tenant_id}" || -z "${timeline_id}" ]]; then
    echo "Control-plane config must contain tenant and timeline ids: ${CONFIG_FILE_ORG}" >&2
    exit 1
  fi
  export TENANT_ID="${tenant_id}"
  export TIMELINE_ID="${timeline_id}"
fi

if [[ -n "${TENANT_ID:-}" && -n "${TIMELINE_ID:-}" ]]; then
   tenant_id=${TENANT_ID}
   timeline_id=${TIMELINE_ID}
   if [[ -n "${ANCESTOR_TIMELINE_ID:-}" ]] && ! timeline_exists "${tenant_id}" "${timeline_id}"; then
      ancestor_start_lsn=${ANCESTOR_START_LSN:-$(timeline_last_record_lsn "${tenant_id}" "${ANCESTOR_TIMELINE_ID}")}
      echo "Create branch timeline ${timeline_id} from ${ANCESTOR_TIMELINE_ID} at ${ancestor_start_lsn}"
      PARAMS=(
          -sbf
          -X POST
          -H "Content-Type: application/json"
          -d "{\"new_timeline_id\": \"${timeline_id}\", \"ancestor_timeline_id\": \"${ANCESTOR_TIMELINE_ID}\", \"ancestor_start_lsn\": \"${ancestor_start_lsn}\"}"
          "${STORAGE_CONTROLLER_HTTP}/v1/tenant/${tenant_id}/timeline"
      )
      result=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}")
      printf '%s\n' "${result}" | jq .
      if printf "%s\n" "${result}" | jq -e ".msg? // empty" >/dev/null; then
        echo "Create branch timeline failed: ${result}" >&2
        exit 1
      fi
   fi
else
  echo "Check if a tenant present"
  tenant_id=$(first_tenant_id)
  if [[ -z "${tenant_id}" || "${tenant_id}" = null ]]; then
    echo "Create a tenant"
    generate_id tenant_id
    PARAMS=(
         -sbf
         -X POST
         -H "Content-Type: application/json"
         -d "{\"new_tenant_id\": \"${tenant_id}\", \"placement_policy\": {\"Attached\": 0}}"
         "${STORAGE_CONTROLLER_HTTP}/v1/tenant"
    )
    result=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}")
    printf '%s\n' "${result}" | jq .
  fi

  if [[ "${RUN_PARALLEL:-false}" != "true" ]]; then
    echo "Check if a timeline present"
    timeline_id=$(first_timeline_id "${tenant_id}")
  fi
  if [[ -z "${timeline_id:-}" || "${timeline_id:-}" = null ]]; then
    generate_id timeline_id
    PARAMS=(
        -sbf
        -X POST
        -H "Content-Type: application/json"
        -d "{\"new_timeline_id\": \"${timeline_id}\", \"pg_version\": ${PAGE_SERVER_PG_VERSION}}"
        "${STORAGE_CONTROLLER_HTTP}/v1/tenant/${tenant_id}/timeline"
    )
    result=$(LD_LIBRARY_PATH="${SYSTEM_LD_LIBRARY_PATH}" curl "${PARAMS[@]}")
    printf '%s\n' "${result}" | jq .
    if printf "%s\n" "${result}" | jq -e ".msg? // empty" >/dev/null; then
      echo "Create timeline failed: ${result}" >&2
      exit 1
    fi
  fi
fi

if [[ -z "${ancestor_start_lsn:-}" ]]; then
  if [[ -n "${ANCESTOR_TIMELINE_ID:-}" ]]; then
    ancestor_start_lsn=${ANCESTOR_START_LSN:-$(timeline_last_record_lsn "${tenant_id}" "${ANCESTOR_TIMELINE_ID}")}
  else
    ancestor_start_lsn=${OGGIT_BRANCH_START_LSN:-0/0}
  fi
fi

echo "Overwrite tenant id and timeline id in spec file"
sed -i "s|TENANT_ID|${tenant_id}|" ${CONFIG_FILE}
sed -i "s|TIMELINE_ID|${timeline_id}|" ${CONFIG_FILE}
set_pageserver_from_controller "${tenant_id}"
if [[ "${DOCKER_CONTROL_PLANE_MODE:-false}" != "true" ]]; then
  set_config_setting "neon.pageserver_connstring" "${pageserver_connstring}" "string"

  if [[ "${OGGIT_ENABLED:-false}" == "true" ]]; then
    set_config_setting "enable_subscription" "on" "bool"
    set_config_setting "neon.oggit_enabled" "on" "bool"
    set_config_setting "neon.oggit_tenant_id" "${tenant_id}" "string"
    set_config_setting "neon.oggit_timeline_id" "${timeline_id}" "string"
    set_config_setting "neon.oggit_ancestor_timeline_id" "${ANCESTOR_TIMELINE_ID:-}" "string"
    set_config_setting "neon.oggit_branch_start_lsn" "${ancestor_start_lsn}" "string"
    set_config_setting "neon.oggit_safekeeper_http_urls" "${OGGIT_SAFEKEEPER_HTTP_URLS:-http://safekeeper1:7676,http://safekeeper2:7676,http://safekeeper3:7676}" "string"
  fi
fi

set_config_include "${POSTGRESQL_EXTEND_CONF}"

cat ${CONFIG_FILE}
write_endpoint_record "Starting"

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
    if [[ -n "${COMPUTE_REMOTE_HBA_CIDR:-}" ]] && ! awk -v cidr="${COMPUTE_REMOTE_HBA_CIDR}" '$1 == "host" && $2 == "all" && $3 == "all" && $4 == cidr && $5 == "sha256" { found = 1 } END { exit !found }' "${PG_HBA}"; then
      hba_tmp=$(mktemp)
      awk -v cidr="${COMPUTE_REMOTE_HBA_CIDR}" '
        !inserted && $1 == "host" && $2 == "all" && $3 == "all" && $4 == "all" && $5 == "md5" {
          print "host all all " cidr " sha256"
          inserted = 1
        }
        { print }
        END {
          if (!inserted) {
            print "host all all " cidr " sha256"
          }
        }
      ' "${PG_HBA}" > "${hba_tmp}"
      mv "${hba_tmp}" "${PG_HBA}"
      "${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -c 'SELECT pg_reload_conf()'
    fi

    if ! wait_for_compute_running; then
      write_endpoint_record "Failed"
      exit 1
    fi

    ensure_branch_merge_user
    write_endpoint_record "Running"

    current_lsn=$("${GSQL}" -d postgres -U cloud_admin -p 55433 -h 127.0.0.1 -tAc 'SELECT pg_current_xlog_location()' | tr -d '[:space:]')
    if [[ -n "${current_lsn}" ]]; then
      wait_lsn_on_pageservers "${tenant_id}" "${timeline_id}" "${current_lsn}"
    fi
    break
  fi
  if ! kill -0 "${compute_ctl_pid}" 2>/dev/null; then
    wait "${compute_ctl_pid}"
    status=$?
    write_endpoint_record "Failed"
    exit "${status}"
  fi
  sleep 1
done

wait "${compute_ctl_pid}"
status=$?
if [[ "${status}" -ne 0 ]]; then
  write_endpoint_record "Failed"
fi
exit "${status}"

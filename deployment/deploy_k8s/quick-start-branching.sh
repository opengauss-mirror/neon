#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="$ROOT_DIR/templates"
GENERATED_DIR="$ROOT_DIR/generated"
ARGS_CACHE_FILE="$ROOT_DIR/quick-start-branching.env"
if [[ -d "$ROOT_DIR/../compute/shell" ]]; then
  COMPUTE_SHELL_DIR="$(cd "$ROOT_DIR/../compute/shell" && pwd)"
elif [[ -d "$ROOT_DIR/../../compute/shell" ]]; then
  COMPUTE_SHELL_DIR="$(cd "$ROOT_DIR/../../compute/shell" && pwd)"
else
  echo "Compute scripts directory not found relative to $ROOT_DIR" >&2
  exit 1
fi

usage() {
  cat <<'EOF'
Usage:
  ./quick-start-branching.sh [COMPONENT=all|yaml|admin|storage|compute|pool-client|pool] KEY=VALUE ...

Defaults match the current branch deployment:
  NAMESPACE=branch
  DATA_ROOT=.neon
  STORAGE_IMAGE=og_storage:k8s
  COMPUTE_IMAGE=og_compute:k8s
  OG_VERSION=V702
  NODE_NAME=<通过 kubectl get nodes 查看并填写>
  COMPOSE_PROJECT=branch
  NUM_SAFEKEEPERS=1

Examples:
  ./quick-start-branching.sh COMPONENT=yaml
  ./quick-start-branching.sh COMPONENT=all

Options:
  WAIT=true|false       Wait for applied resources to become ready. Default: true.
  KEEP_JOBS=true|false  Keep completed one-shot Job pods. Default: false.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

set_arg() {
  local pair="$1"
  local key

  if [[ "$pair" != *=* ]]; then
    echo "Invalid argument: $pair" >&2
    usage >&2
    exit 1
  fi

  key="${pair%%=*}"
  if [[ -z "${arg_seen[$key]+x}" ]]; then
    arg_keys+=("$key")
  fi
  arg_seen["$key"]=1
  arg_values["$key"]="${pair#*=}"
}

set_default_arg() {
  local key="$1"
  local value="$2"

  if [[ -z "${arg_seen[$key]+x}" ]]; then
    set_arg "${key}=${value}"
  fi
}

load_cached_args() {
  local line

  if [[ ! -f "$ARGS_CACHE_FILE" ]]; then
    return
  fi

  while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    set_arg "$line"
  done < "$ARGS_CACHE_FILE"
}

merged_args() {
  local key

  for key in "${arg_keys[@]}"; do
    printf '%s=%s\n' "$key" "${arg_values[$key]}"
  done
}

save_args_cache() {
  local tmp_file

  tmp_file="$(mktemp "$ROOT_DIR/quick-start-branching.env.XXXXXX")"
  merged_args > "$tmp_file"
  chmod 600 "$tmp_file"
  mv "$tmp_file" "$ARGS_CACHE_FILE"
}

template_files() {
  case "$COMPONENT" in
    yaml|all)
      printf '%s\n' \
        00-admin-prerequisites.yaml \
        01-storage-config.yaml \
        02-storage-stack.yaml \
        03-main-compute.yaml \
        04-pool-client.yaml \
        opgs-pool.yaml
      ;;
    admin)
      printf '%s\n' 00-admin-prerequisites.yaml
      ;;
    storage)
      printf '%s\n' 01-storage-config.yaml 02-storage-stack.yaml
      ;;
    compute)
      printf '%s\n' 03-main-compute.yaml
      ;;
    pool-client)
      printf '%s\n' 04-pool-client.yaml
      ;;
    pool)
      printf '%s\n' opgs-pool.yaml
      ;;
    *)
      echo "Invalid COMPONENT: $COMPONENT" >&2
      usage >&2
      exit 1
      ;;
  esac
}

render_placeholder() {
  local file="$1"
  local placeholder="$2"
  local value="$3"

  PLACEHOLDER="__${placeholder}__" VALUE="$value" \
    perl -0pi -e 's/\Q$ENV{PLACEHOLDER}\E/$ENV{VALUE}/g' "$file"
}

render_file() {
  local name="$1"
  local source="$TEMPLATE_DIR/$name"
  local target="$GENERATED_DIR/$name"
  local key
  local data_root="${arg_values[DATA_ROOT]}"

  if [[ "$data_root" != /* ]]; then
    data_root="$ROOT_DIR/$data_root"
  fi

  if [[ ! -f "$source" ]]; then
    echo "Template not found: $source" >&2
    exit 1
  fi

  cp "$source" "$target"
  for key in "${render_keys[@]}"; do
    if [[ "$key" == "DATA_ROOT" ]]; then
      render_placeholder "$target" "$key" "$data_root"
    else
      render_placeholder "$target" "$key" "${arg_values[$key]}"
    fi
  done
  render_placeholder "$target" "COMPUTE_SHELL_DIR" "$COMPUTE_SHELL_DIR"
  render_placeholder "$target" "COORDINATOR_CODE_SHA" "$(cat "$ROOT_DIR/bin/k8s_local.py" "$ROOT_DIR/bin/branching_coordinator.py" "$ROOT_DIR/bin/coordinator_client.py" "$ROOT_DIR/../common/pitr_common.py" | sha256sum | cut -d ' ' -f 1)"
}

render_all() {
  local name

  mkdir -p "$GENERATED_DIR"
  mapfile -t selected_templates < <(template_files)
  for name in "${selected_templates[@]}"; do
    render_file "$name"
  done

  if grep -H "__[A-Z0-9_]*__" "${selected_templates[@]/#/$GENERATED_DIR/}"; then
    echo "ERROR: unresolved placeholders remain in generated YAML" >&2
    exit 1
  fi
}

apply_file() {
  local file="$1"

  kubectl apply --server-side --force-conflicts -f "$file"
  kubectl annotate -f "$file" \
    kubectl.kubernetes.io/last-applied-configuration- \
    --overwrite >/dev/null 2>&1 || true
}

ensure_namespace() {
  local namespace="${arg_values[NAMESPACE]}"

  kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f -
}

ensure_compute_assets() {
  local namespace="${arg_values[NAMESPACE]}"
  local script_path="$COMPUTE_SHELL_DIR/compute.sh"

  if [[ ! -f "$script_path" ]]; then
    echo "compute.sh not found: $script_path" >&2
    exit 1
  fi

  kubectl create configmap branching-compute-assets \
    -n "$namespace" \
    --from-file=compute.sh="$script_path" \
    --from-file=repair_pool_binding.py="$ROOT_DIR/bin/repair_pool_binding.py" \
    --dry-run=client \
    -o yaml | kubectl apply --server-side --force-conflicts -f -
  kubectl annotate configmap branching-compute-assets \
    -n "$namespace" \
    kubectl.kubernetes.io/last-applied-configuration- \
    --overwrite >/dev/null 2>&1 || true
}

ensure_coordinator_assets() {
  local namespace="${arg_values[NAMESPACE]}"
  local credentials="$ROOT_DIR/.coordinator"
  mkdir -p "$credentials"
  chmod 700 "$credentials"
  if ! kubectl get secret branching-pool-api -n "$namespace" >/dev/null 2>&1; then
    python3 - "$credentials" <<'PY_TOKEN'
import os, secrets, sys
from pathlib import Path
root = Path(sys.argv[1])
for name in ("token", "coordinator-token"):
    path = root / name
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        out.write(secrets.token_hex(32))
PY_TOKEN
    kubectl create secret generic branching-pool-api -n "$namespace" \
      --from-file=token="$credentials/token" \
      --from-file=coordinator-token="$credentials/coordinator-token"
  fi
  # Read the existing credential so a second deployment host does not rotate it.
  kubectl get secret branching-pool-api -n "$namespace" -o json |
    python3 -c 'import base64,json,os,sys; from pathlib import Path; p=Path(sys.argv[1]); d=json.load(sys.stdin)["data"]; fd=os.open(str(p),os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600); os.write(fd,base64.b64decode(d["coordinator-token"])); os.close(fd)' "$credentials/coordinator-token"
  set_arg "COORDINATOR_TOKEN_FILE=.coordinator/coordinator-token"
  kubectl create configmap branching-coordinator-code -n "$namespace" \
    --from-file=k8s_local.py="$ROOT_DIR/bin/k8s_local.py" \
    --from-file=branching_coordinator.py="$ROOT_DIR/bin/branching_coordinator.py" \
    --from-file=coordinator_client.py="$ROOT_DIR/bin/coordinator_client.py" \
    --from-file=pitr_common.py="$ROOT_DIR/../common/pitr_common.py" \
    --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f -
}

delete_one_shot_jobs_before_apply() {
  local namespace="${arg_values[NAMESPACE]}"

  kubectl delete job -n "$namespace" \
    docker-control-plane-init \
    storage-controller-register-safekeepers \
    --ignore-not-found >/dev/null 2>&1 || true
}

migrate_pool_client_strategy() {
  local namespace="${arg_values[NAMESPACE]}"
  local strategy
  # SSA retains the old default rollingUpdate object. Remove it explicitly
  # before applying Recreate; a failed GET/PATCH must stop deployment.
  strategy=$(kubectl get deployment branching-pool-client -n "$namespace" \
    --ignore-not-found -o jsonpath='{.spec.strategy.type}')
  if [[ "$strategy" == "RollingUpdate" ]]; then
    kubectl patch deployment branching-pool-client -n "$namespace" --type=merge \
      -p '{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null}}}'
  fi
}

apply_generated() {
  local name
  local namespace="${arg_values[NAMESPACE]}"

  ensure_namespace
  case "$COMPONENT" in
    all|compute|pool|pool-client)
      ensure_coordinator_assets
      ;;
  esac
  case "$COMPONENT" in
    all|storage)
      delete_one_shot_jobs_before_apply
      ;;
  esac
  case "$COMPONENT" in
    all|pool)
      ensure_compute_assets
      ;;
  esac

  for name in "${selected_templates[@]}"; do
    if [[ "$name" == "04-pool-client.yaml" ]]; then
      migrate_pool_client_strategy
    fi
    apply_file "$GENERATED_DIR/$name"
  done

  kubectl delete job -n "$namespace" \
    storage-controller-register-safekeepers \
    --ignore-not-found >/dev/null 2>&1 || true
}

wait_for_deployment() {
  local name="$1"
  local namespace="${arg_values[NAMESPACE]}"

  kubectl rollout status "deployment/$name" -n "$namespace" --timeout=300s
}

wait_for_jobs_and_cleanup() {
  local namespace="${arg_values[NAMESPACE]}"

  if kubectl get job docker-control-plane-init -n "$namespace" >/dev/null 2>&1; then
    kubectl wait --for=condition=complete job/docker-control-plane-init -n "$namespace" --timeout=180s
  fi

  if [[ "${arg_values[KEEP_JOBS]}" != "true" ]]; then
    kubectl delete job docker-control-plane-init -n "$namespace" --ignore-not-found >/dev/null 2>&1 || true
  fi
}

wait_for_pool() {
  local namespace="${arg_values[NAMESPACE]}"
  local deadline=$((SECONDS + 300))
  local snapshot metrics replicas allocated total warm available idle
  while (( SECONDS < deadline )); do
    # One snapshot keeps desired replicas and status counters consistent.
    if ! snapshot=$(kubectl get dbresourcepool opgs-pool -n "$namespace" --ignore-not-found -o json); then
      sleep 3
      continue
    fi
    if [[ -z "$snapshot" ]]; then
      return
    fi
    metrics=$(python3 -c '
import json, sys
pool = json.load(sys.stdin)
replicas = int(pool["spec"]["replicas"])
status = pool.get("status", {})
allocated = int(status.get("allocatedInstances", 0))
total = int(status.get("totalInstances", 0))
warm = int(status.get("warmInstances", 0))
available = int(status.get("availableInstances", 0))
print(replicas, allocated, total, warm, available, max(0, replicas - allocated))
' <<<"$snapshot")
    read -r replicas allocated total warm available idle <<<"$metrics"
    if (( total >= replicas && warm >= idle && available >= idle )); then
      return
    fi
    sleep 3
  done
  echo "Timed out waiting for opgs-pool: replicas=${replicas:-?} allocated=${allocated:-?} total=${total:-?} warm=${warm:-?} available=${available:-?}; expected idle=${idle:-?}" >&2
  kubectl get dbresourcepool opgs-pool -n "$namespace" -o wide >&2 || true
  exit 1
}

wait_for_ready() {
  local namespace="${arg_values[NAMESPACE]}"

  case "$COMPONENT" in
    all|storage)
      wait_for_deployment storage-broker
      wait_for_deployment docker-control-plane
      wait_for_jobs_and_cleanup
      wait_for_deployment storage-controller-db
      wait_for_deployment storage-controller
      wait_for_deployment endpoint-storage
      wait_for_deployment pageserver
      wait_for_deployment safekeeper1
      ;;
  esac

  case "$COMPONENT" in
    all|compute)
      wait_for_deployment compute-main
      ;;
  esac

  case "$COMPONENT" in
    all|pool-client)
      wait_for_deployment branching-pool-client
      ;;
  esac

  case "$COMPONENT" in
    all|pool)
      wait_for_pool
      ;;
  esac

  kubectl get pods -n "$namespace" -o wide
}

print_result() {
  echo "Generated YAML:"
  for name in "${selected_templates[@]}"; do
    echo "  $GENERATED_DIR/$name"
  done

  if [[ "$COMPONENT" == "yaml" ]]; then
    echo "YAML generated only. Nothing applied."
  else
    echo "Applied component: $COMPONENT"
  fi
}

COMPONENT="all"
declare -A arg_seen=()
declare -A arg_values=()
arg_keys=()
cli_args=()
selected_templates=()
render_keys=(
  NAMESPACE
  DATA_ROOT
  STORAGE_IMAGE
  COMPUTE_IMAGE
  OG_VERSION
  NODE_NAME
  COMPOSE_PROJECT
  NUM_SAFEKEEPERS
)

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    COMPONENT=*)
      COMPONENT="${arg#COMPONENT=}"
      ;;
    *)
      cli_args+=("$arg")
      ;;
  esac
done

load_cached_args
for arg in "${cli_args[@]}"; do
  set_arg "$arg"
done

set_default_arg "NAMESPACE" "branch"
set_default_arg "DATA_ROOT" ".neon"
set_default_arg "STORAGE_IMAGE" "og_storage:k8s"
set_default_arg "COMPUTE_IMAGE" "og_compute:k8s"
set_default_arg "OG_VERSION" "V702"
set_default_arg "NODE_NAME" ""
set_default_arg "COMPOSE_PROJECT" "branch"
set_default_arg "NUM_SAFEKEEPERS" "1"
set_default_arg "WAIT" "true"
set_default_arg "KEEP_JOBS" "false"

if [[ "$COMPONENT" != "yaml" ]]; then
  require_cmd kubectl
fi
require_cmd perl

render_all

if [[ "$COMPONENT" != "yaml" ]]; then
  apply_generated
  if [[ "${arg_values[WAIT]}" == "true" ]]; then
    wait_for_ready
  fi
fi

if [[ "$COMPONENT" != "yaml" ]]; then
  coordinator_ip=$(kubectl get service branching-coordinator -n "${arg_values[NAMESPACE]}" -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
  if [[ -n "$coordinator_ip" ]]; then
    set_arg "COORDINATOR_URL=http://${coordinator_ip}:8787"
  fi
fi
save_args_cache
print_result

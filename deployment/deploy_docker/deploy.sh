#!/usr/bin/env bash
################################################################################
# Branching Docker 一键部署脚本
#
# 用法：
#   bash deploy.sh arm --skip-pull
#   bash deploy.sh x86 --skip-pull
#
# 脚本会拉取对应架构镜像，初始化 Branching Docker control plane，
# 并创建、启动默认的 main endpoint。
#
# 运行前请先执行 install.sh 下载运行时所需文件。
################################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}"
ENV_FILE="${DEPLOY_DIR}/deploy.env"
COMPOSE_DIR="${DEPLOY_DIR}/docker-compose"

CLI_SKIP_PULL="false"

STORAGE_IMAGE_REPOSITORY="swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/og_storage"
COMPUTE_IMAGE_REPOSITORY="swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/og_compute"
LOCAL_STORAGE_IMAGE="og_storage:latest"
LOCAL_COMPUTE_IMAGE="og_compute:latest"
STORAGE_IMAGE_ENV_NAME="NE""ON_IMAGE"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*" >&2; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_step()  { echo -e "\n${CYAN}========== $* ==========${NC}" >&2; }

usage() {
    cat >&2 <<'EOF'
用法：
  bash deploy.sh arm --skip-pull
  bash deploy.sh x86 --skip-pull

可选环境变量：
  SKIP_PULL     设置为 true 时跳过镜像拉取，并使用本地默认 tag
  DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH
                设置后启用 control plane HTTP JWT 认证
  DOCKER_CONTROL_PLANE_TOKEN
                启用认证后供 docker_local 调用 control plane 的 Admin JWT
  CONTROL_PLANE_JWT_TOKEN
                启用认证后供 storage_controller 回调 control plane 的 Admin JWT
EOF
}

die() {
    log_error "$*"
    exit 1
}

check_command() {
    command -v "$1" >/dev/null 2>&1 || die "未找到命令 $1，请先安装后再运行脚本"
}

parse_architecture() {
    if [[ $# -lt 1 || $# -gt 2 ]]; then
        usage
        exit 1
    fi

    local architecture=""
    local argument
    for argument in "$@"; do
        case "${argument}" in
            --skip-pull)
                CLI_SKIP_PULL="true"
                ;;
            arm|aarch64)
                [[ -z "${architecture}" ]] || die "架构参数只能指定一次"
                architecture="arm"
                ;;
            x86|x86_64|amd64)
                [[ -z "${architecture}" ]] || die "架构参数只能指定一次"
                architecture="x86"
                ;;
            *)
                usage
                die "未知参数或不支持的架构参数: ${argument}"
                ;;
        esac
    done

    [[ -n "${architecture}" ]] || die "必须指定架构参数 arm 或 x86"
    IMAGE_ARCH="${architecture}"
}

load_runtime_configuration() {
    if [[ -f "${ENV_FILE}" ]]; then
        set -a
        # shellcheck source=/dev/null
        source "${ENV_FILE}"
        set +a
    fi

    COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-branching_poc}"
    OG_VERSION="${OG_VERSION:-V702}"
    NUM_PAGESERVERS="${NUM_PAGESERVERS:-1}"
    NUM_SAFEKEEPERS="${NUM_SAFEKEEPERS:-1}"
    INIT_FORCE="${INIT_FORCE:-must-not-exist}"
    INIT_TIMEOUT="${INIT_TIMEOUT:-120}"
    HEALTH_CHECK_TIMEOUT="${HEALTH_CHECK_TIMEOUT:-120}"
    if [[ "${CLI_SKIP_PULL}" == "true" ]]; then
        SKIP_PULL="true"
    else
        SKIP_PULL="${SKIP_PULL:-false}"
    fi
    CREATE_DEFAULT_TENANT="${CREATE_DEFAULT_TENANT:-true}"
    CREATE_DEFAULT_ENDPOINT="${CREATE_DEFAULT_ENDPOINT:-true}"
    DEFAULT_ENDPOINT_NAME="${DEFAULT_ENDPOINT_NAME:-main}"
    DEFAULT_BRANCH_NAME="${DEFAULT_BRANCH_NAME:-main}"
    DEFAULT_ENDPOINT_PG_PORT="${DEFAULT_ENDPOINT_PG_PORT:-55433}"
    DEFAULT_ENDPOINT_HTTP_PORT="${DEFAULT_ENDPOINT_HTTP_PORT:-3080}"
    DOCKER_CONTROL_PLANE_HOST_PORT="${DOCKER_CONTROL_PLANE_HOST_PORT:-8080}"
    DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH="${DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH:-}"
    DOCKER_CONTROL_PLANE_TOKEN="${DOCKER_CONTROL_PLANE_TOKEN:-}"
    CONTROL_PLANE_JWT_TOKEN="${CONTROL_PLANE_JWT_TOKEN:-${DOCKER_CONTROL_PLANE_TOKEN}}"
    STORAGE_CONTROLLER_HOST_PORT="${STORAGE_CONTROLLER_HOST_PORT:-1234}"
}

check_docker_environment() {
    log_step "检查部署环境"

    check_command curl
    check_command docker
    check_command python3

    docker compose version >/dev/null 2>&1 || die "当前 Docker 不支持 docker compose 子命令"
    docker info >/dev/null 2>&1 || die "Docker 服务未运行，或当前用户没有 Docker 权限"

    log_info "系统架构: $(uname -m)"
    log_info "目标镜像架构: ${IMAGE_ARCH}"
}

validate_runtime_files() {
    log_step "校验部署文件"

    local required_files=(
        "${ENV_FILE}"
        "${COMPOSE_DIR}/docker-compose.yml"
        "${COMPOSE_DIR}/bin/docker_local"
        "${COMPOSE_DIR}/bin/docker_local.py"
        "${COMPOSE_DIR}/pageserver_config/identity.toml"
        "${COMPOSE_DIR}/pageserver_config/pageserver.toml"
        "${COMPOSE_DIR}/pageserver_config/metadata.json"
        "${DEPLOY_DIR}/compute/shell/compute.sh"
        "${DEPLOY_DIR}/compute/shell/storage_controller_db.sh"
    )
    local file

    for file in "${required_files[@]}"; do
        [[ -f "${file}" ]] || die "缺少部署文件: ${file}，请先执行 install.sh"
    done

    [[ -x "${COMPOSE_DIR}/bin/docker_local" ]] || die "文件不可执行: ${COMPOSE_DIR}/bin/docker_local"
    [[ -x "${COMPOSE_DIR}/bin/docker_local.py" ]] || die "文件不可执行: ${COMPOSE_DIR}/bin/docker_local.py"
    [[ -x "${DEPLOY_DIR}/compute/shell/compute.sh" ]] || die "文件不可执行: ${DEPLOY_DIR}/compute/shell/compute.sh"
    [[ -x "${DEPLOY_DIR}/compute/shell/storage_controller_db.sh" ]] || die "文件不可执行: ${DEPLOY_DIR}/compute/shell/storage_controller_db.sh"
}

pull_and_tag_images() {
    if [[ "${SKIP_PULL}" == "true" ]]; then
        log_step "跳过 Docker 镜像拉取"
        log_info "使用本地镜像: ${LOCAL_COMPUTE_IMAGE}"
        log_info "使用本地镜像: ${LOCAL_STORAGE_IMAGE}"
        docker image inspect "${LOCAL_COMPUTE_IMAGE}" >/dev/null 2>&1 \
            || die "本地镜像不存在: ${LOCAL_COMPUTE_IMAGE}"
        docker image inspect "${LOCAL_STORAGE_IMAGE}" >/dev/null 2>&1 \
            || die "本地镜像不存在: ${LOCAL_STORAGE_IMAGE}"
        return
    fi

    log_step "拉取 ${IMAGE_ARCH} Docker 镜像"

    local storage_image="${STORAGE_IMAGE_REPOSITORY}:${IMAGE_ARCH}"
    local compute_image="${COMPUTE_IMAGE_REPOSITORY}:${IMAGE_ARCH}"

    log_info "拉取 ${compute_image}"
    docker pull "${compute_image}"
    log_info "拉取 ${storage_image}"
    docker pull "${storage_image}"

    log_info "设置本地 compute 镜像 tag: ${LOCAL_COMPUTE_IMAGE}"
    docker tag "${compute_image}" "${LOCAL_COMPUTE_IMAGE}"
    log_info "设置本地 storage 镜像 tag: ${LOCAL_STORAGE_IMAGE}"
    docker tag "${storage_image}" "${LOCAL_STORAGE_IMAGE}"
}

validate_compute_image_contents() {
    log_step "校验 compute 镜像运行文件"

    docker run --rm --entrypoint /bin/sh "${LOCAL_COMPUTE_IMAGE}" -ec '
        test -x /shell/compute.sh
        test -x /shell/storage_controller_db.sh
        test -f /var/db/gaussdb/configs/config.json
    ' || die "compute 镜像缺少 endpoint 所需的运行文件"
}

validate_compose_configuration() {
    log_step "校验 Docker Compose 配置"

    (
        cd "${COMPOSE_DIR}"
        env \
        "OG_VERSION=${OG_VERSION}" \
        "${STORAGE_IMAGE_ENV_NAME}=${LOCAL_STORAGE_IMAGE}" \
        "COMPUTE_IMAGE=${LOCAL_COMPUTE_IMAGE}" \
        "COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME}" \
        "DOCKER_CONTROL_PLANE_HOST_PORT=${DOCKER_CONTROL_PLANE_HOST_PORT}" \
        "DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH=${DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH}" \
        "CONTROL_PLANE_JWT_TOKEN=${CONTROL_PLANE_JWT_TOKEN}" \
        "STORAGE_CONTROLLER_HOST_PORT=${STORAGE_CONTROLLER_HOST_PORT}" \
        docker compose config >/dev/null
    )
}

run_docker_local() {
    (
        cd "${COMPOSE_DIR}"
        export OG_VERSION
        export "${STORAGE_IMAGE_ENV_NAME}=${LOCAL_STORAGE_IMAGE}"
        export COMPUTE_IMAGE="${LOCAL_COMPUTE_IMAGE}"
        export COMPOSE_PROJECT_NAME
        export DOCKER_CONTROL_PLANE_HOST_PORT
        export DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH
        export DOCKER_CONTROL_PLANE_TOKEN
        export CONTROL_PLANE_JWT_TOKEN
        export STORAGE_CONTROLLER_HOST_PORT
        ./bin/docker_local "$@"
    )
}

initialize_branching() {
    log_step "初始化 Branching control plane"

    run_docker_local init \
        --compose-project "${COMPOSE_PROJECT_NAME}" \
        --og-version "${OG_VERSION}" \
        --storage-image "${LOCAL_STORAGE_IMAGE}" \
        --compute-image "${LOCAL_COMPUTE_IMAGE}" \
        --num-pageservers "${NUM_PAGESERVERS}" \
        --num-safekeepers "${NUM_SAFEKEEPERS}" \
        --force "${INIT_FORCE}" \
        --timeout "${INIT_TIMEOUT}"
}

start_branching_services() {
    log_step "启动 Branching 存储服务"
    run_docker_local start --timeout "${HEALTH_CHECK_TIMEOUT}"
}

create_default_tenant() {
    if [[ "${CREATE_DEFAULT_TENANT}" != "true" ]]; then
        log_info "跳过默认 tenant 创建"
        return
    fi

    log_step "创建默认 tenant 和 main branch"
    run_docker_local tenant create \
        --branch-name "${DEFAULT_BRANCH_NAME}" \
        --set-default
}

create_and_start_default_endpoint() {
    if [[ "${CREATE_DEFAULT_ENDPOINT}" != "true" ]]; then
        log_info "跳过默认 endpoint 创建"
        return
    fi

    log_step "创建并启动默认 endpoint"
    run_docker_local endpoint create "${DEFAULT_ENDPOINT_NAME}" \
        --branch-name "${DEFAULT_BRANCH_NAME}" \
        --pg-port "${DEFAULT_ENDPOINT_PG_PORT}" \
        --http-port "${DEFAULT_ENDPOINT_HTTP_PORT}"
    run_docker_local endpoint start "${DEFAULT_ENDPOINT_NAME}" \
        --timeout "${HEALTH_CHECK_TIMEOUT}"
}

wait_for_http() {
    local url="$1"
    local description="$2"
    local elapsed=0
    local interval=2

    log_info "等待 ${description}: ${url}"
    while (( elapsed < HEALTH_CHECK_TIMEOUT )); do
        if curl -fsS "${url}" >/dev/null 2>&1; then
            log_info "${description} 已就绪"
            return 0
        fi
        sleep "${interval}"
        elapsed=$((elapsed + interval))
    done

    die "${description} 在 ${HEALTH_CHECK_TIMEOUT} 秒内未就绪: ${url}"
}

wait_for_pageservers_registered() {
    local elapsed=0
    local interval=2
    local nodes_url="http://127.0.0.1:${STORAGE_CONTROLLER_HOST_PORT}/control/v1/node"

    log_info "等待 pageserver 注册到 Storage controller: ${nodes_url}"
    while (( elapsed < HEALTH_CHECK_TIMEOUT )); do
        if curl -fsS "${nodes_url}" | python3 -c '
import json
import sys

needed = int(sys.argv[1])
nodes = json.load(sys.stdin)
active = [
    node for node in nodes
    if node.get("availability") == "Active" and node.get("scheduling") == "Active"
]
if len(active) < needed:
    raise SystemExit(1)
' "${NUM_PAGESERVERS}" >/dev/null 2>&1
        then
            log_info "pageserver 已注册并可调度"
            return
        fi
        sleep "${interval}"
        elapsed=$((elapsed + interval))
    done

    die "pageserver 在 ${HEALTH_CHECK_TIMEOUT} 秒内未注册到 Storage controller"
}

health_check() {
    log_step "执行服务健康检查"

    wait_for_http "http://127.0.0.1:${DOCKER_CONTROL_PLANE_HOST_PORT}/ready" "Docker control plane"
    wait_for_http "http://127.0.0.1:${STORAGE_CONTROLLER_HOST_PORT}/ready" "Storage controller"

    if [[ "${CREATE_DEFAULT_ENDPOINT}" == "true" ]]; then
        wait_for_http "http://127.0.0.1:${DEFAULT_ENDPOINT_HTTP_PORT}/status" "默认 compute endpoint"
    fi
}

print_summary() {
    log_step "部署完成"

    cat <<EOF
部署成功！

部署目录:
  ${DEPLOY_DIR}

镜像:
  storage: ${LOCAL_STORAGE_IMAGE}
  compute: ${LOCAL_COMPUTE_IMAGE}
  架构:    ${IMAGE_ARCH}

服务:
  control plane:      http://127.0.0.1:${DOCKER_CONTROL_PLANE_HOST_PORT}
  storage controller: http://127.0.0.1:${STORAGE_CONTROLLER_HOST_PORT}
EOF

    if [[ "${CREATE_DEFAULT_ENDPOINT}" == "true" ]]; then
        cat <<EOF
  compute endpoint:   PostgreSQL 127.0.0.1:${DEFAULT_ENDPOINT_PG_PORT}
                      HTTP       http://127.0.0.1:${DEFAULT_ENDPOINT_HTTP_PORT}
EOF
    fi

    cat <<EOF

后续管理请使用：
  ${COMPOSE_DIR}/bin/docker_local
EOF
}

main() {
    parse_architecture "$@"
    validate_runtime_files
    load_runtime_configuration
    check_docker_environment
    pull_and_tag_images
    validate_compute_image_contents
    validate_compose_configuration
    initialize_branching
    start_branching_services
    wait_for_pageservers_registered
    create_default_tenant
    create_and_start_default_endpoint
    health_check
    print_summary
}

main "$@"

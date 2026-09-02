#!/usr/bin/env bash
################################################################################
# Branching Docker 部署文件安装脚本
# 下载此脚本：
# curl -fsSL https://raw.gitcode.com/opengauss/neon/raw/neon_release_9129.0/deployment/deploy_docker/install.sh -o install.sh
# 用法：
#   bash install.sh
#
# 脚本会创建 openGauss-branching-deploy 目录，并通过网络下载部署运行文件。
################################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/openGauss-branching-deploy"
ENV_FILE="${DEPLOY_DIR}/deploy.env"
COMPOSE_DIR="${DEPLOY_DIR}/docker-compose"

RAW_BASE_URL="${RAW_BASE_URL:-https://raw.gitcode.com/opengauss/neon/raw/neon_release_9129}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*" >&2; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_step()  { echo -e "\n${CYAN}========== $* ==========${NC}" >&2; }

die() {
    log_error "$*"
    exit 1
}

check_command() {
    command -v "$1" >/dev/null 2>&1 || die "未找到命令 $1，请先安装后再运行脚本"
}

validate_raw_base_url() {
    if [[ "${RAW_BASE_URL}" == *"your-org"* || "${RAW_BASE_URL}" == *"your-repo"* ]]; then
        die "请设置 RAW_BASE_URL 为实际部署文件所在仓库的 raw 基址"
    fi
}

prepare_directories() {
    log_step "创建部署目录"

    mkdir -p \
        "${COMPOSE_DIR}/bin" \
        "${COMPOSE_DIR}/pageserver_config" \
        "${DEPLOY_DIR}/compute/shell"
}

download_file() {
    local remote_path="$1"
    local target_path="$2"
    local api_url=""

    log_info "下载 ${remote_path}"
    mkdir -p "$(dirname "${target_path}")"
    if [[ -d "${target_path}" ]]; then
        rm -rf "${target_path}"
    fi
    if curl -fsSL "${RAW_BASE_URL}/${remote_path}" -o "${target_path}"; then
        return
    fi

    if [[ "${RAW_BASE_URL}" =~ ^https://raw\.gitcode\.com/([^/]+)/([^/]+)/raw/(.+)$ ]]; then
        api_url="https://gitcode.com/api/v5/repos/${BASH_REMATCH[1]}/${BASH_REMATCH[2]}/raw/${remote_path}?ref=${BASH_REMATCH[3]}"
        log_warn "raw.gitcode.com 下载失败，尝试 GitCode API raw: ${remote_path}"
        curl -fsSL "${api_url}" -o "${target_path}"
        return
    fi

    die "下载失败: ${remote_path}"
}

download_runtime_files() {
    log_step "下载部署文件"

    download_file "deployment/deploy_docker/deploy.sh" "${DEPLOY_DIR}/deploy.sh"
    download_file "deployment/deploy_docker/deploy.env" "${ENV_FILE}"
    download_file "docker-compose/docker-compose.yml" "${COMPOSE_DIR}/docker-compose.yml"
    download_file "docker-compose/bin/docker_local" "${COMPOSE_DIR}/bin/docker_local"
    download_file "docker-compose/bin/docker_local.py" "${COMPOSE_DIR}/bin/docker_local.py"
    download_file "docker-compose/pageserver_config/identity.toml" "${COMPOSE_DIR}/pageserver_config/identity.toml"
    download_file "docker-compose/pageserver_config/pageserver.toml" "${COMPOSE_DIR}/pageserver_config/pageserver.toml"
    download_file "docker-compose/pageserver_config/metadata.json" "${COMPOSE_DIR}/pageserver_config/metadata.json"
    download_file "compute/shell/compute.sh" "${DEPLOY_DIR}/compute/shell/compute.sh"
    download_file "compute/shell/storage_controller_db.sh" "${DEPLOY_DIR}/compute/shell/storage_controller_db.sh"
}

set_runtime_permissions() {
    log_step "设置部署文件权限"

    chmod 0755 \
        "${DEPLOY_DIR}/deploy.sh" \
        "${DEPLOY_DIR}/compute/shell/compute.sh" \
        "${DEPLOY_DIR}/compute/shell/storage_controller_db.sh" \
        "${COMPOSE_DIR}/bin/docker_local" \
        "${COMPOSE_DIR}/bin/docker_local.py"

    chmod 0644 \
        "${ENV_FILE}" \
        "${COMPOSE_DIR}/docker-compose.yml" \
        "${COMPOSE_DIR}/pageserver_config/identity.toml" \
        "${COMPOSE_DIR}/pageserver_config/pageserver.toml" \
        "${COMPOSE_DIR}/pageserver_config/metadata.json"
}

print_summary() {
    log_step "安装完成"

    cat <<EOF
部署文件已下载到:
  ${DEPLOY_DIR}

后续部署请执行:
  cd ${SCRIPT_DIR}
  bash openGauss-branching-deploy/deploy.sh <arm|x86> [--skip-pull]
EOF
}

main() {
    check_command curl
    validate_raw_base_url
    prepare_directories
    download_runtime_files
    set_runtime_permissions
    print_summary
}

main "$@"

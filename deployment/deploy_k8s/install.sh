#!/usr/bin/env bash
################################################################################
# Branching Kubernetes 部署文件安装脚本
# 下载此脚本：
# curl -fsSL https://raw.gitcode.com/opengauss/neon/raw/neon_release_9129/deployment/deploy_k8s/install.sh -o install.sh
# 用法：
#   bash install.sh
#
# 脚本会创建 openGauss-branching-deploy 目录，并通过网络下载 Kubernetes 部署文件。
################################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/openGauss-branching-deploy"
K8S_DIR="${DEPLOY_DIR}/deploy_k8s"
TEMPLATE_DIR="${K8S_DIR}/templates"
BIN_DIR="${K8S_DIR}/bin"
ENV_FILE="${K8S_DIR}/quick-start-branching.env"
COMPUTE_SHELL_DIR="${DEPLOY_DIR}/compute/shell"

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

download_file() {
    local base_url="$1"
    local remote_path="$2"
    local target_path="$3"
    local repository_path=""
    local branch_name=""
    local api_url=""

    log_info "下载 ${remote_path}"
    mkdir -p "$(dirname "${target_path}")"

    if curl -fsSL "${base_url}/${remote_path}" -o "${target_path}"; then
        return
    fi

    if [[ "${base_url}" =~ ^https://raw\.gitcode\.com/([^/]+)/([^/]+)/raw/([^/]+)(/(.*))?$ ]]; then
        branch_name="${BASH_REMATCH[3]}"
        repository_path="${BASH_REMATCH[5]:-}"
        if [[ -n "${repository_path}" ]]; then
            remote_path="${repository_path}/${remote_path}"
        fi
        api_url="https://gitcode.com/api/v5/repos/${BASH_REMATCH[1]}/${BASH_REMATCH[2]}/raw/${remote_path}?ref=${branch_name}"
        log_warn "raw.gitcode.com 下载失败，尝试 GitCode API raw: ${remote_path}"
        curl -fsSL "${api_url}" -o "${target_path}" || die "下载失败: ${remote_path}"
        return
    fi

    die "下载失败: ${remote_path}"
}

prepare_directories() {
    log_step "创建部署目录"

    mkdir -p \
        "${TEMPLATE_DIR}" \
        "${BIN_DIR}" \
        "${COMPUTE_SHELL_DIR}"
}

# source path | installed path relative to DEPLOY_DIR | file mode
runtime_files() {
    cat <<'FILES'
deployment/deploy_k8s/quick-start-branching.sh|deploy_k8s/quick-start-branching.sh|0755
deployment/deploy_k8s/README.md|deploy_k8s/README.md|0644
deployment/deploy_k8s/.gitignore|deploy_k8s/.gitignore|0644
deployment/deploy_k8s/quick-start-branching.env|deploy_k8s/quick-start-branching.env|0600
deployment/deploy_k8s/bin/k8s_local|deploy_k8s/bin/k8s_local|0755
deployment/deploy_k8s/bin/k8s_local.py|deploy_k8s/bin/k8s_local.py|0755
deployment/deploy_k8s/bin/k8s_local_fast.py|deploy_k8s/bin/k8s_local_fast.py|0755
deployment/deploy_k8s/bin/coordinator_client.py|deploy_k8s/bin/coordinator_client.py|0644
deployment/deploy_k8s/bin/branching_coordinator.py|deploy_k8s/bin/branching_coordinator.py|0755
deployment/deploy_k8s/bin/repair_pool_binding.py|deploy_k8s/bin/repair_pool_binding.py|0755
deployment/common/pitr_common.py|common/pitr_common.py|0644
deployment/deploy_k8s/templates/00-admin-prerequisites.yaml|deploy_k8s/templates/00-admin-prerequisites.yaml|0644
deployment/deploy_k8s/templates/01-storage-config.yaml|deploy_k8s/templates/01-storage-config.yaml|0644
deployment/deploy_k8s/templates/02-storage-stack.yaml|deploy_k8s/templates/02-storage-stack.yaml|0644
deployment/deploy_k8s/templates/03-main-compute.yaml|deploy_k8s/templates/03-main-compute.yaml|0644
deployment/deploy_k8s/templates/04-pool-client.yaml|deploy_k8s/templates/04-pool-client.yaml|0644
deployment/deploy_k8s/templates/opgs-pool.yaml|deploy_k8s/templates/opgs-pool.yaml|0644
compute/shell/compute.sh|compute/shell/compute.sh|0755
compute/shell/storage_controller_db.sh|compute/shell/storage_controller_db.sh|0755
FILES
}

download_runtime_files() {
    log_step "下载 Kubernetes 部署文件"
    local source target mode
    while IFS='|' read -r source target mode; do
        download_file "${RAW_BASE_URL}" "$source" "${DEPLOY_DIR}/$target"
    done < <(runtime_files)
}

set_runtime_permissions() {
    log_step "设置部署文件权限"
    local source target mode
    while IFS='|' read -r source target mode; do
        chmod "$mode" "${DEPLOY_DIR}/$target"
    done < <(runtime_files)
}

print_summary() {
    log_step "安装完成"

    cat <<EOF
部署文件已下载到:
  ${DEPLOY_DIR}

后续部署请执行:
  cd ${DEPLOY_DIR}/deploy_k8s
  ./quick-start-branching.sh COMPONENT=all

如需覆盖本机参数，请编辑:
  ${ENV_FILE}
EOF
}

main() {
    check_command curl
    prepare_directories
    download_runtime_files
    set_runtime_permissions
    print_summary
}

main "$@"

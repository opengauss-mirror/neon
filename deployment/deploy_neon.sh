#!/bin/bash

###############################################################################
# Neon Branching 分布式自动化部署脚本
# 
# 使用方法：
#   1. 修改 path.conf 配置文件（只需修改顶部的 IP 和 host 变量）
#   2. 运行 ./deploy_neon.sh
###############################################################################

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/path.conf"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    log_error "配置文件不存在: $CONFIG_FILE"
    exit 1
fi

###############################################################################
# 函数: 替换配置文件中的变量
###############################################################################
expand_vars() {
    local file="$1"
    local temp_file=$(mktemp)
    
    # 导出所有需要的变量给 envsubst 使用
    export MACHINE_A_IP MACHINE_B_IP MACHINE_B_USER
    export NEON_SOURCE_A_DIR NEON_SOURCE_B_DIR
    export NEON_BIN_A_DIR NEON_BIN_B_DIR
    export NEON_DATA_A_DIR NEON_DATA_B_DIR
    export p_host1 p_host2 p_host3
    export s_host1 s_host2 s_host3
    
    # 使用envsubst替换变量
    envsubst < "$file" > "$temp_file"
    cat "$temp_file"
    rm -f "$temp_file"
}

###############################################################################
# 读取配置
###############################################################################
log_info "读取配置文件: $CONFIG_FILE"

# 读取 Bash 变量
eval "$(grep '^[A-Z_]*=' "$CONFIG_FILE")"
eval "$(grep '^[ps]_host[0-9]*=' "$CONFIG_FILE")"

# 验证必要的配置项
required_vars=(
    "MACHINE_A_IP"
    "MACHINE_B_IP"
    "MACHINE_B_USER"
    "NEON_SOURCE_A_DIR"
    "NEON_DATA_A_DIR"
    "NEON_BIN_A_DIR"
    "NEON_BIN_B_DIR"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        log_error "配置项 $var 未设置"
        exit 1
    fi
done

# 统一使用机器A的路径作为主路径
NEON_SOURCE_DIR="${NEON_SOURCE_A_DIR}"
NEON_DATA_DIR="${NEON_DATA_A_DIR}"
NEON_BIN_DIR="${NEON_BIN_A_DIR}"

# 定义关键路径
OPENGAUSS_DIR="${NEON_SOURCE_DIR}/og_install/V702"
OPENGAUSS_BASE_DIR="$(dirname "${OPENGAUSS_DIR}")"
CONTROL_PLANE_DIR="${NEON_SOURCE_DIR}/control_plane"

# 当前用户
CURRENT_USER=$(whoami)

log_info "============================================"
log_info "Neon 分布式部署配置"
log_info "============================================"
log_info "机器A:"
log_info "  IP: $MACHINE_A_IP"
log_info "  源代码: $NEON_SOURCE_A_DIR"
log_info "  二进制: $NEON_BIN_A_DIR"
log_info "  数据目录: $NEON_DATA_A_DIR"
log_info "机器B:"
log_info "  IP: $MACHINE_B_IP"
log_info "  用户: $MACHINE_B_USER"
log_info "  源代码: $NEON_SOURCE_B_DIR"
log_info "  二进制: $NEON_BIN_B_DIR"
log_info "  数据目录: $NEON_DATA_B_DIR"
log_info ""
log_info "组件分布:"
log_info "  Pageserver 1: $p_host1"
log_info "  Pageserver 2: $p_host2"
log_info "  Pageserver 3: $p_host3"
log_info "  Safekeeper 1: $s_host1"
log_info "  Safekeeper 2: $s_host2"
log_info "  Safekeeper 3: $s_host3"
log_info "============================================"

###############################################################################
# 步骤1: 检查编译产物
###############################################################################
log_step "步骤1: 检查编译产物"

required_bins=(
    "${NEON_BIN_DIR}/pageserver"
    "${NEON_BIN_DIR}/safekeeper"
    "${NEON_BIN_DIR}/storage_broker"
    "${NEON_BIN_DIR}/neon_local"
    "${NEON_BIN_DIR}/compute_ctl"
    "${OPENGAUSS_DIR}/bin/gaussdb"
    "${OPENGAUSS_DIR}/bin/gs_ctl"
)

for bin in "${required_bins[@]}"; do
    if [ ! -f "$bin" ]; then
        log_error "二进制文件不存在: $bin"
        log_error "请先编译 Neon 项目"
        exit 1
    fi
    log_info "✓ 找到: $bin"
done

# 检查 Neon 扩展库
required_libs=(
    "${OPENGAUSS_DIR}/lib/postgresql/neon.so"
    "${OPENGAUSS_DIR}/lib/postgresql/neon_utils.so"
    "${OPENGAUSS_DIR}/lib/postgresql/neon_rmgr.so"
    "${OPENGAUSS_DIR}/lib/postgresql/neon_walredo.so"
)

for lib in "${required_libs[@]}"; do
    if [ ! -f "$lib" ]; then
        log_error "库文件不存在: $lib"
        exit 1
    fi
    log_info "✓ 找到: $lib"
done

###############################################################################
# 步骤2: 配置SSH免密登录
###############################################################################
log_step "步骤2: 配置SSH免密登录到机器B"

if ssh -o BatchMode=yes -o ConnectTimeout=5 "${MACHINE_B_USER}@${MACHINE_B_IP}" "echo 2>&1" &>/dev/null; then
    log_info "✓ SSH免密登录已配置"
else
    log_warn "SSH免密登录未配置，开始配置..."
    
    if [ ! -f ~/.ssh/id_rsa.pub ]; then
        log_info "生成SSH密钥..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -C "${CURRENT_USER}@${MACHINE_A_IP}"
    fi
    
    log_info "复制公钥到机器B (需要输入密码)..."
    ssh-copy-id -i ~/.ssh/id_rsa.pub "${MACHINE_B_USER}@${MACHINE_B_IP}"
    
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "${MACHINE_B_USER}@${MACHINE_B_IP}" "echo 2>&1" &>/dev/null; then
        log_info "✓ SSH免密登录配置成功"
    else
        log_error "SSH免密登录配置失败"
        exit 1
    fi
fi

###############################################################################
# 步骤3: 配置 NEON_REPO_DIR 环境变量
###############################################################################
# log_step "步骤3: 配置 NEON_REPO_DIR 环境变量"

# # 机器A
# log_info "在机器A配置 NEON_REPO_DIR..."
# BASHRC_A="$HOME/.bashrc"
# NEON_REPO_LINE="export NEON_REPO_DIR=${NEON_DATA_A_DIR}"

# if ! grep -q "NEON_REPO_DIR" "$BASHRC_A" 2>/dev/null; then
#     echo "" >> "$BASHRC_A"
#     echo "# Neon 数据目录（由 deploy_neon.sh 自动添加）" >> "$BASHRC_A"
#     echo "$NEON_REPO_LINE" >> "$BASHRC_A"
#     log_info "✓ 已添加到 $BASHRC_A"
# else
#     log_info "✓ NEON_REPO_DIR 已存在"
# fi

# export NEON_REPO_DIR="${NEON_DATA_A_DIR}"
# source "$BASHRC_A" || true

# # 机器B
# log_info "在机器B配置 NEON_REPO_DIR..."
# ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" "
#     if ! grep -q 'NEON_REPO_DIR' ~/.bashrc 2>/dev/null; then
#         echo '' >> ~/.bashrc
#         echo '# Neon 数据目录' >> ~/.bashrc
#         echo 'export NEON_REPO_DIR=${NEON_DATA_B_DIR}' >> ~/.bashrc
#     fi
#     source ~/.bashrc || true
# "

# log_info "✓ 环境变量配置完成"

###############################################################################
# 步骤4: 分发文件到机器B
###############################################################################
log_step "步骤4: 分发文件到机器B"

log_info "在机器B创建目录..."
ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" "mkdir -p ${NEON_BIN_B_DIR} ${NEON_SOURCE_B_DIR}/og_install"

# 复制二进制
log_info "复制 Neon 二进制到机器B..."
find "${NEON_BIN_A_DIR}" -maxdepth 1 -type f -perm -111 -exec scp {} \
    "${MACHINE_B_USER}@${MACHINE_B_IP}:${NEON_BIN_B_DIR}/" \;

# 复制 OpenGauss
if ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" "[ -f ${NEON_SOURCE_B_DIR}/og_install/V702/bin/gaussdb ]"; then
    log_info "✓ 机器B已有 OpenGauss"
else
    log_info "复制 OpenGauss 到机器B (约800MB)..."
    scp -r "${OPENGAUSS_DIR}" "${MACHINE_B_USER}@${MACHINE_B_IP}:${NEON_SOURCE_B_DIR}/og_install/" || {
        log_error "OpenGauss 复制失败"
        exit 1
    }
    log_info "✓ OpenGauss 复制完成"
fi

log_info "✓ 文件分发完成"

###############################################################################
# 步骤5: 生成配置文件
###############################################################################
log_step "步骤5: 生成 cargo neon 配置文件"

# 获取 TOML 配置部分起始行
TOML_START=$(grep -n "^default_tenant_id" "$CONFIG_FILE" | cut -d':' -f1)

# 生成机器A配置（只包含机器A的 pageserver，但包含所有 safekeeper）
CONFIG_MACHINE_A="${CONTROL_PLANE_DIR}/neon_machine_a.conf"
log_info "生成机器A配置: $CONFIG_MACHINE_A"

# 创建临时文件
TEMP_CONFIG_A=$(mktemp)

{
    echo "# 机器A配置（自动生成）"
    echo "pg_distrib_dir = \"${OPENGAUSS_BASE_DIR}\""
    echo "neon_distrib_dir = \"${NEON_BIN_A_DIR}\""
    echo ""
    
    # 基本配置
    tail -n +${TOML_START} "$CONFIG_FILE" | head -3
    echo ""
    
    # 只提取机器A的 pageserver
    awk '
    /^\[\[pageservers\]\]/{
        if(in_ps && length(section)>0) {
            print "[[pageservers]]"
            print section
        }
        in_ps=1
        section=""
        next
    }
    in_ps && !/^\[\[/{
        section=section $0 "\n"
    }
    in_ps && /^\[\[/{
        if(length(section)>0) {
            print "[[pageservers]]"
            print section
        }
        in_ps=0
        exit
    }
    END {
        if(in_ps && length(section)>0) {
            print "[[pageservers]]"
            print section
        }
    }
    ' "$CONFIG_FILE" | while IFS= read -r line; do
        if [[ "$line" == "[[pageservers]]" ]]; then
            current_ps_section="$line"$'\n'
            in_section=1
        elif [[ $in_section -eq 1 && "$line" =~ ^id=([0-9]+) ]]; then
            ps_id="${BASH_REMATCH[1]}"
            ps_host_var="p_host${ps_id}"
            if [[ "${!ps_host_var}" == "${MACHINE_A_IP}" ]]; then
                should_include=1
            else
                should_include=0
            fi
            current_ps_section+="$line"$'\n'
        elif [[ $in_section -eq 1 && -z "$line" ]]; then
            if [[ $should_include -eq 1 ]]; then
                echo -n "$current_ps_section"
            fi
            in_section=0
            current_ps_section=""
        elif [[ $in_section -eq 1 ]]; then
            current_ps_section+="$line"$'\n'
        fi
    done
    
    # 包含所有 safekeeper（Storage Controller 需要知道所有 safekeeper）
    sed -n '/^\[\[safekeepers\]\]/,/^\[broker\]/p' "$CONFIG_FILE" | head -n -1
    
    echo ""
    # broker、endpoint_storage 和 storage_controller 配置
    sed -n '/^\[broker\]/,$p' "$CONFIG_FILE"
} > "$TEMP_CONFIG_A"

# 使用 expand_vars 替换变量
expand_vars "$TEMP_CONFIG_A" > "$CONFIG_MACHINE_A"
rm -f "$TEMP_CONFIG_A"

# 生成机器B配置（只包含机器B的组件）
CONFIG_MACHINE_B="${CONTROL_PLANE_DIR}/neon_machine_b.conf"
log_info "生成机器B配置: $CONFIG_MACHINE_B"

# 创建临时文件
TEMP_CONFIG_B=$(mktemp)

{
    echo "# 机器B配置（自动生成）"
    echo "pg_distrib_dir = \"${NEON_SOURCE_B_DIR}/og_install\""
    echo "neon_distrib_dir = \"${NEON_BIN_B_DIR}\""
    echo ""
    
    # 基本配置
    tail -n +${TOML_START} "$CONFIG_FILE" | head -3
    echo ""
    
    # 提取所有 pageserver，稍后过滤机器B的
    awk '
    /^\[\[pageservers\]\]/{
        if(in_ps && length(section)>0) {
            print "[[pageservers]]"
            print section
        }
        in_ps=1
        section=""
        next
    }
    in_ps && !/^\[\[/{
        section=section $0 "\n"
    }
    in_ps && /^\[\[/{
        if(length(section)>0) {
            print "[[pageservers]]"
            print section
        }
        in_ps=0
        exit
    }
    END {
        if(in_ps && length(section)>0) {
            print "[[pageservers]]"
            print section
        }
    }
    ' "$CONFIG_FILE" | while IFS= read -r line; do
        if [[ "$line" == "[[pageservers]]" ]]; then
            current_ps_section="$line"$'\n'
            in_section=1
        elif [[ $in_section -eq 1 && "$line" =~ ^id=([0-9]+) ]]; then
            ps_id="${BASH_REMATCH[1]}"
            ps_host_var="p_host${ps_id}"
            if [[ "${!ps_host_var}" == "${MACHINE_B_IP}" ]]; then
                should_include=1
            else
                should_include=0
            fi
            current_ps_section+="$line"$'\n'
        elif [[ $in_section -eq 1 && -z "$line" ]]; then
            if [[ $should_include -eq 1 ]]; then
                echo -n "$current_ps_section"
            fi
            in_section=0
            current_ps_section=""
        elif [[ $in_section -eq 1 ]]; then
            current_ps_section+="$line"$'\n'
        fi
    done
    
    # 提取所有 safekeeper，稍后过滤机器B的
    awk '
    /^\[\[safekeepers\]\]/{
        if(in_sk && length(section)>0) {
            print "[[safekeepers]]"
            print section
        }
        in_sk=1
        section=""
        next
    }
    in_sk && !/^\[\[/{
        section=section $0 "\n"
    }
    in_sk && /^\[\[/{
        if(length(section)>0) {
            print "[[safekeepers]]"
            print section
        }
        in_sk=0
        exit
    }
    END {
        if(in_sk && length(section)>0) {
            print "[[safekeepers]]"
            print section
        }
    }
    ' "$CONFIG_FILE" | while IFS= read -r line; do
        if [[ "$line" == "[[safekeepers]]" ]]; then
            current_sk_section="$line"$'\n'
            in_section=1
        elif [[ $in_section -eq 1 && "$line" =~ ^id\ =\ ([0-9]+) ]]; then
            sk_id="${BASH_REMATCH[1]}"
            sk_host_var="s_host${sk_id}"
            if [[ "${!sk_host_var}" == "${MACHINE_B_IP}" ]]; then
                should_include=1
            else
                should_include=0
            fi
            current_sk_section+="$line"$'\n'
        elif [[ $in_section -eq 1 && -z "$line" ]]; then
            if [[ $should_include -eq 1 ]]; then
                echo -n "$current_sk_section"
            fi
            in_section=0
            current_sk_section=""
        elif [[ $in_section -eq 1 ]]; then
            current_sk_section+="$line"$'\n'
        fi
    done
    
    echo ""
    # 机器B必须包含 broker 和 endpoint_storage 配置（即使不在B上运行）
    # 否则 cargo neon init 会报 "missing field" 错误
    sed -n '/^\[broker\]/,/^\[storage_controller\]/p' "$CONFIG_FILE" | head -n -1
} > "$TEMP_CONFIG_B"

# 使用 expand_vars 替换变量
expand_vars "$TEMP_CONFIG_B" > "$CONFIG_MACHINE_B"
rm -f "$TEMP_CONFIG_B"

log_info "✓ 配置文件生成完成"

###############################################################################
# 步骤6: 初始化环境
###############################################################################
log_step "步骤6: 初始化 Neon 环境"

# 机器A
log_info "初始化机器A..."
cd "${NEON_SOURCE_A_DIR}"
export NEON_REPO_DIR="${NEON_DATA_A_DIR}"
cargo neon init --config="${CONFIG_MACHINE_A}" 2>&1 | grep -v "already exists" || true

# 机器B
log_info "初始化机器B..."
ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" "mkdir -p ${NEON_SOURCE_B_DIR}/control_plane"
scp "${CONFIG_MACHINE_B}" "${MACHINE_B_USER}@${MACHINE_B_IP}:${NEON_SOURCE_B_DIR}/control_plane/"

ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" \
    "cd ${NEON_SOURCE_B_DIR} && \
     NEON_REPO_DIR=${NEON_DATA_B_DIR} ${NEON_BIN_B_DIR}/neon_local init \
        --config=${NEON_SOURCE_B_DIR}/control_plane/neon_machine_b.conf 2>&1 | grep -v 'already exists' || true"

ssh "${MACHINE_B_USER}@${MACHINE_B_IP}" "
    cd ${NEON_DATA_B_DIR} && 
    rm -rf endpoints endpoint_storage storage_broker 
    echo '已清理机器B上不需要的目录'
"

log_info "✓ 环境初始化完成"

###############################################################################
# 步骤7: 生成启动/停止脚本
###############################################################################
log_step "步骤7: 生成启动和停止脚本"

# 判断组件分布
declare -A on_machine_b
for i in 1 2 3; do
    ps_host_var="p_host$i"
    sk_host_var="s_host$i"
    
    [ "${!ps_host_var}" == "${MACHINE_B_IP}" ] && on_machine_b["ps$i"]="yes"
    [ "${!sk_host_var}" == "${MACHINE_B_IP}" ] && on_machine_b["sk$i"]="yes"
done

# 生成启动脚本
cat > "${SCRIPT_DIR}/start_neon.sh" << 'EOFSTART'
#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 只读取 Bash 变量部分（不包括 TOML 配置）
eval "$(grep '^[A-Z_]*=' "${SCRIPT_DIR}/path.conf")"
eval "$(grep '^[ps]_host[0-9]*=' "${SCRIPT_DIR}/path.conf")"

log_info() { echo -e "\033[0;32m[INFO]\033[0m $1"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $1"; }

log_info "========================================"
log_info "启动 Neon 分布式服务"
log_info "========================================"

cd "${NEON_SOURCE_A_DIR}"
export NEON_REPO_DIR="${NEON_DATA_A_DIR}"

# 机器A组件
log_info ">>> 启动机器A组件"
cargo neon storage_broker start --start-timeout 10s
cargo neon storage_controller start --start-timeout 30s
cargo neon endpoint-storage start --start-timeout 10s

EOFSTART

# 添加 Safekeeper 启动命令
for i in 1 2 3; do
    sk_host_var="s_host$i"
    if [ "${!sk_host_var}" == "${MACHINE_A_IP}" ]; then
        echo "cargo neon safekeeper start $i --start-timeout 10s" >> "${SCRIPT_DIR}/start_neon.sh"
    fi
done

# 添加 Pageserver 启动命令（机器A）
for i in 1 2 3; do
    ps_host_var="p_host$i"
    if [ "${!ps_host_var}" == "${MACHINE_A_IP}" ]; then
        echo "cargo neon pageserver start --id $i --start-timeout 30s" >> "${SCRIPT_DIR}/start_neon.sh"
    fi
done

# 添加机器B组件
cat >> "${SCRIPT_DIR}/start_neon.sh" << 'EOFSTART'

# 机器B组件
log_info ">>> 启动机器B组件"
EOFSTART

for i in 1 2 3; do
    sk_host_var="s_host$i"
    if [ "${!sk_host_var}" == "${MACHINE_B_IP}" ]; then
        cat >> "${SCRIPT_DIR}/start_neon.sh" << EOFSTART
ssh "\${MACHINE_B_USER}@\${MACHINE_B_IP}" "NEON_REPO_DIR=\${NEON_DATA_B_DIR} \${NEON_BIN_B_DIR}/neon_local safekeeper start $i --start-timeout 10s"
EOFSTART
    fi
done

for i in 1 2 3; do
    ps_host_var="p_host$i"
    if [ "${!ps_host_var}" == "${MACHINE_B_IP}" ]; then
        cat >> "${SCRIPT_DIR}/start_neon.sh" << EOFSTART
ssh "\${MACHINE_B_USER}@\${MACHINE_B_IP}" "NEON_REPO_DIR=\${NEON_DATA_B_DIR} \${NEON_BIN_B_DIR}/neon_local pageserver start --id $i --start-timeout 30s"
EOFSTART
    fi
done

cat >> "${SCRIPT_DIR}/start_neon.sh" << 'EOFSTART'

log_info "✓ 所有服务已启动"
EOFSTART

chmod +x "${SCRIPT_DIR}/start_neon.sh"

# 生成停止脚本
cat > "${SCRIPT_DIR}/stop_neon.sh" << 'EOFSTOP'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 只读取 Bash 变量部分（不包括 TOML 配置）
eval "$(grep '^[A-Z_]*=' "${SCRIPT_DIR}/path.conf")"
eval "$(grep '^[ps]_host[0-9]*=' "${SCRIPT_DIR}/path.conf")"

log_info() { echo -e "\033[0;32m[INFO]\033[0m $1"; }

log_info "========================================"
log_info "停止 Neon 分布式服务"
log_info "========================================"

cd "${NEON_SOURCE_A_DIR}"
export NEON_REPO_DIR="${NEON_DATA_A_DIR}"

# 首先停止所有 endpoint（计算节点）
log_info ">>> 停止所有 endpoint 计算节点"
if [ -d "${NEON_DATA_A_DIR}/endpoints" ]; then
    for endpoint in $(ls "${NEON_DATA_A_DIR}/endpoints/" 2>/dev/null); do
        log_info "停止 endpoint: $endpoint"
        cargo neon endpoint stop "$endpoint" --mode fast 2>/dev/null || true
    done
fi

# 机器B组件
log_info ">>> 停止机器B组件"
EOFSTOP

for i in 1 2 3; do
    ps_host_var="p_host$i"
    if [ "${!ps_host_var}" == "${MACHINE_B_IP}" ]; then
        echo "ssh \"\${MACHINE_B_USER}@\${MACHINE_B_IP}\" \"NEON_REPO_DIR=\${NEON_DATA_B_DIR} \${NEON_BIN_B_DIR}/neon_local pageserver stop --id $i -m fast 2>/dev/null\" || true" >> "${SCRIPT_DIR}/stop_neon.sh"
    fi
done

for i in 1 2 3; do
    sk_host_var="s_host$i"
    if [ "${!sk_host_var}" == "${MACHINE_B_IP}" ]; then
        echo "ssh \"\${MACHINE_B_USER}@\${MACHINE_B_IP}\" \"NEON_REPO_DIR=\${NEON_DATA_B_DIR} \${NEON_BIN_B_DIR}/neon_local safekeeper stop $i -m fast 2>/dev/null\" || true" >> "${SCRIPT_DIR}/stop_neon.sh"
    fi
done

cat >> "${SCRIPT_DIR}/stop_neon.sh" << 'EOFSTOP'

# 机器A组件
log_info ">>> 停止机器A组件"
cargo neon endpoint-storage stop -m fast 2>/dev/null || true
cargo neon storage_controller stop -m fast 2>/dev/null || true
EOFSTOP

for i in 1 2 3; do
    ps_host_var="p_host$i"
    if [ "${!ps_host_var}" == "${MACHINE_A_IP}" ]; then
        echo "cargo neon pageserver stop --id $i -m fast 2>/dev/null || true" >> "${SCRIPT_DIR}/stop_neon.sh"
    fi
done

for i in 1 2 3; do
    sk_host_var="s_host$i"
    if [ "${!sk_host_var}" == "${MACHINE_A_IP}" ]; then
        echo "cargo neon safekeeper stop $i -m fast 2>/dev/null || true" >> "${SCRIPT_DIR}/stop_neon.sh"
    fi
done

cat >> "${SCRIPT_DIR}/stop_neon.sh" << 'EOFSTOP'
cargo neon storage_broker stop -m fast 2>/dev/null || true

log_info "✓ 所有服务已停止"
EOFSTOP

chmod +x "${SCRIPT_DIR}/stop_neon.sh"

log_info "✓ 启动脚本: ${SCRIPT_DIR}/start_neon.sh"
log_info "✓ 停止脚本: ${SCRIPT_DIR}/stop_neon.sh"

###############################################################################
# 完成
###############################################################################
log_info ""
log_info "========================================"
log_info "部署完成！"
log_info "========================================"
log_info ""
log_info "下一步:"
log_info "1. 启动: ${SCRIPT_DIR}/start_neon.sh"
log_info "2. 创建租户: cd ${NEON_SOURCE_A_DIR} && cargo neon tenant create"
log_info ""

#!/bin/bash

# WalRedo 问题自动化测试脚本
# 用法: ./test_walredo.sh [build|test|clean|all]

set -e  # 遇到错误立即退出（在需要的地方会临时禁用）

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目路径
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEON_DIR="${PROJECT_DIR}/.neon"
LOG_DIR="${PROJECT_DIR}/test_logs"

# 创建日志目录
mkdir -p "${LOG_DIR}"

# 日志文件
BUILD_LOG="${LOG_DIR}/build_$(date +%Y%m%d_%H%M%S).log"
TEST_LOG="${LOG_DIR}/test_$(date +%Y%m%d_%H%M%S).log"

# 打印带颜色的日志
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ============================================
# 阶段 1: 编译
# ============================================
build_project() {
    log_info "开始编译项目..."
    echo "编译日志: ${BUILD_LOG}"
    
    cd "${PROJECT_DIR}"
    
    # 记录开始时间
    local start_time=$(date +%s)
    
    # 执行编译
    if make -j$(nproc) -s > "${BUILD_LOG}" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_success "编译成功！耗时: ${duration} 秒"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_error "编译失败！耗时: ${duration} 秒"
        log_error "查看详细日志: ${BUILD_LOG}"
        tail -n 50 "${BUILD_LOG}"
        return 1
    fi
}

# ============================================
# 阶段 2: 问题复现
# ============================================
init_neon_env() {
    log_info "初始化 Neon 环境..."
    cd "${PROJECT_DIR}"
    
    # 清理旧环境
    if [ -d "${NEON_DIR}" ]; then
        log_warning "检测到旧的 .neon 目录，正在清理..."
        rm -rf "${NEON_DIR}"
    fi
    
    # 初始化
    log_info "执行: cargo neon init"
    if ! cargo neon init >> "${TEST_LOG}" 2>&1; then
        log_error "Neon 初始化失败"
        return 1
    fi
    log_success "Neon 初始化完成"
}

start_neon_services() {
    log_info "启动 Neon 服务..."
    
    log_info "执行: cargo neon start"
    if ! cargo neon start >> "${TEST_LOG}" 2>&1; then
        log_error "Neon 启动失败"
        return 1
    fi
    
    # 等待服务启动
    log_info "等待服务启动 (5秒)..."
    sleep 5
    
    log_success "Neon 服务启动完成"
}

create_tenant_and_endpoint() {
    log_info "创建 Tenant 和 Endpoint..."
    
    log_info "执行: cargo neon tenant create --set-default"
    if ! cargo neon tenant create --set-default >> "${TEST_LOG}" 2>&1; then
        log_error "创建 Tenant 失败"
        return 1
    fi
    
    log_info "执行: cargo neon endpoint create main"
    if ! cargo neon endpoint create main >> "${TEST_LOG}" 2>&1; then
        log_error "创建 Endpoint 失败"
        return 1
    fi
    
    log_info "执行: cargo neon endpoint start main"
    # 忽略 "already exists" 错误，因为 endpoint 可能已经启动
    cargo neon endpoint start main >> "${TEST_LOG}" 2>&1 || true
    
    # 等待 endpoint 完全启动
    log_info "等待 Endpoint 完全启动 (10秒)..."
    sleep 10
    
    log_success "Tenant 和 Endpoint 创建完成"
}

get_connection_string() {
    # 从 endpoint.json 获取连接信息
    local endpoint_json="${NEON_DIR}/endpoints/main/endpoint.json"
    if [ ! -f "$endpoint_json" ]; then
        log_error "找不到 endpoint 配置文件: $endpoint_json"
        return 1
    fi
    
    local pg_port=$(grep -oP '"pg_port":\s*\K\d+' "$endpoint_json")
    if [ -z "$pg_port" ]; then
        log_error "无法从配置文件获取端口"
        return 1
    fi
    
    echo "postgresql://cloud_admin@127.0.0.1:${pg_port}/postgres"
}

# 步骤1: 修改密码
change_password() {
    log_info "步骤1: 登录数据库并修改密码..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    log_info "连接字符串: ${connection_string}"
    
    # 创建修改密码的 SQL 脚本
    local SQL_SCRIPT="${LOG_DIR}/change_password.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
-- 修改密码
ALTER ROLE cloud_admin PASSWORD 'Huawei12#$';
\echo '密码修改成功'
EOF
    
    log_info "执行修改密码 SQL..."
    local sql_output="${LOG_DIR}/change_password_output_$(date +%Y%m%d_%H%M%S).log"
    
    if timeout 30s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1; then
        log_success "密码修改成功"
        cat "${sql_output}"
        return 0
    else
        log_error "密码修改失败"
        cat "${sql_output}"
        return 1
    fi
}

# 步骤1: 修改密码并创建表
change_password_and_create_table() {
    log_info "步骤1: 登录数据库修改密码并创建表..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    log_info "连接字符串: ${connection_string}"
    
    # 创建 SQL 脚本：修改密码 + 创建表 + 插入数据 + 查询表 OID
    local SQL_SCRIPT="${LOG_DIR}/change_pwd_create_table.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
-- 步骤1: 修改密码
ALTER ROLE cloud_admin PASSWORD 'Huawei12#$';
\echo '密码修改成功'

-- 步骤2: 创建表 t
DROP TABLE IF EXISTS t;
CREATE TABLE t(key int, value text);
\echo '表 t 创建成功'

-- 步骤3: 插入测试数据
INSERT INTO t VALUES (1, 'test_data_1');
INSERT INTO t VALUES (2, 'test_data_2');
INSERT INTO t VALUES (3, 'test_data_3');
\echo '测试数据插入成功'

-- 步骤4: 验证数据已插入
SELECT * FROM t ORDER BY key;

-- 步骤5: 查询表 t 的 OID 和 relfilenode
SELECT oid, relname, relfilenode, reltablespace 
FROM pg_class 
WHERE relname = 't';

-- 步骤6: 查询 pg_class 中表 t 的详细信息
SELECT c.oid as table_oid, 
       c.relfilenode,
       c.relname,
       n.nspname as schema_name
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname = 't';

\echo '========================================='
\echo '表信息和数据已记录，请保存 OID 用于重启后验证'
\echo '========================================='
EOF
    
    log_info "执行 SQL 脚本..."
    local sql_output="${LOG_DIR}/create_table_output_$(date +%Y%m%d_%H%M%S).log"
    
    if timeout 30s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1; then
        log_success "密码修改和建表成功"
        cat "${sql_output}"
        
        # 从输出中提取表 OID
        local table_oid=$(grep -oP '^\s*\K\d+(?=\s*\|\s*t\s*\|)' "${sql_output}" | head -1)
        if [ -n "$table_oid" ]; then
            echo "${table_oid}" > "${LOG_DIR}/table_t_oid.txt"
            log_info "表 t 的 OID: ${table_oid}"
        fi
        
        return 0
    else
        log_error "执行失败"
        cat "${sql_output}"
        return 1
    fi
}

# 步骤2: 停止 main endpoint
stop_main_endpoint() {
    log_info "步骤2: 停止 main 分支..."
    
    # 先检查 endpoint 状态
    local status=$(cargo neon endpoint list 2>/dev/null | grep "main" | awk '{print $NF}')
    log_info "当前 endpoint 状态: ${status}"
    
    # 如果已经是 stopped 或 crashed 状态，视为成功
    if [[ "${status}" == "stopped" ]] || [[ "${status}" == "crashed" ]]; then
        log_warning "endpoint 已经处于 ${status} 状态，跳过停止操作"
        sleep 1
        return 0
    fi
    
    if cargo neon endpoint stop main >> "${TEST_LOG}" 2>&1; then
        log_success "main 分支已停止"
        sleep 3
        return 0
    else
        # 再次检查状态，如果是 stopped 或 crashed，仍然视为成功
        status=$(cargo neon endpoint list 2>/dev/null | grep "main" | awk '{print $NF}')
        if [[ "${status}" == "stopped" ]] || [[ "${status}" == "crashed" ]]; then
            log_warning "停止命令返回错误但 endpoint 已停止 (${status})"
            sleep 1
            return 0
        fi
        log_error "停止 main 分支失败，当前状态: ${status}"
        return 1
    fi
}

# 步骤3: 重启 main endpoint
restart_main_endpoint() {
    log_info "步骤3: 重启 main 分支..."
    
    # 等待端口释放（防止 TIME_WAIT 状态导致启动失败）
    local port=55432
    local max_wait=60
    local waited=0
    log_info "等待端口 ${port} 释放 (最多等待 ${max_wait} 秒)..."
    
    while ss -tlnp 2>/dev/null | grep -q ":${port} " || \
          netstat -tlnp 2>/dev/null | grep -q ":${port} "; do
        if [ $waited -ge $max_wait ]; then
            log_warning "端口 ${port} 仍被占用，尝试继续启动..."
            break
        fi
        sleep 2
        waited=$((waited + 2))
        if [ $((waited % 10)) -eq 0 ]; then
            log_info "已等待 ${waited} 秒..."
        fi
    done
    
    if [ $waited -gt 0 ] && [ $waited -lt $max_wait ]; then
        log_success "端口 ${port} 已释放，等待了 ${waited} 秒"
    fi
    
    if cargo neon endpoint start main >> "${TEST_LOG}" 2>&1; then
        log_success "main 分支重启命令已执行"
        log_info "等待 endpoint 完全启动 (10秒)..."
        sleep 10
        return 0
    else
        log_error "重启 main 分支失败"
        return 1
    fi
}

# 步骤4: 验证登录
verify_login() {
    log_info "步骤4: 验证能否成功登录数据库..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    log_info "连接字符串: ${connection_string}"
    
    # 创建简单的验证 SQL
    local SQL_SCRIPT="${LOG_DIR}/verify_login.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
-- 简单的登录验证查询
SELECT current_user, current_database();
\echo '登录验证成功!'
EOF
    
    log_info "执行登录验证 SQL（超时时间：30秒）..."
    local sql_output="${LOG_DIR}/verify_login_output_$(date +%Y%m%d_%H%M%S).log"
    local sql_pid=""
    
    # 在后台执行 gsql
    timeout 30s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1 &
    sql_pid=$!
    
    # 监控执行时间
    local elapsed=0
    local stuck=false
    
    while kill -0 $sql_pid 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        
        if [ $elapsed -eq 5 ]; then
            log_warning "登录验证已超过 5 秒，可能卡住了..."
        fi
        
        if [ $elapsed -ge 30 ]; then
            log_error "登录验证超时（30秒）"
            stuck=true
            break
        fi
        
        # 每5秒输出一次状态
        if [ $((elapsed % 5)) -eq 0 ]; then
            log_info "登录验证中... (${elapsed}秒)"
        fi
    done
    
    # 等待进程结束
    wait $sql_pid 2>/dev/null
    local exit_code=$?
    
    # 检查结果
    log_info "登录验证输出:"
    cat "${sql_output}"
    
    # 检查是否有 "invalid role OID" 错误
    if grep -q "invalid role OID" "${sql_output}"; then
        log_error "检测到 'invalid role OID' 错误，问题已复现！"
        return 1
    fi
    
    # 检查是否有其他 FATAL 错误
    if grep -q "FATAL:" "${sql_output}"; then
        log_error "检测到 FATAL 错误！"
        grep "FATAL:" "${sql_output}"
        return 1
    fi
    
    if [ $stuck = true ] || [ $exit_code -eq 124 ]; then
        log_error "登录验证超时或卡住，问题已复现！"
        return 1
    elif [ $exit_code -eq 0 ]; then
        log_success "登录验证成功！（耗时 ${elapsed} 秒）"
        return 0
    else
        log_error "登录验证失败，退出码: ${exit_code}"
        return 1
    fi
}

# 步骤5: 验证表是否存在
verify_table_exists() {
    log_info "步骤5: 验证表 t 是否仍然存在..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    log_info "连接字符串: ${connection_string}"
    
    # 创建验证表存在的 SQL
    local SQL_SCRIPT="${LOG_DIR}/verify_table.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
-- [LAYERDBG] 首先禁用所有索引扫描，强制使用 SeqScan
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;

\echo '=== 使用 SeqScan 查询 pg_class ==='

-- 查询表 t 是否存在（使用 SeqScan）
EXPLAIN (COSTS off) SELECT oid, relname, relfilenode FROM pg_class WHERE relname = 't';
SELECT oid, relname, relfilenode 
FROM pg_class 
WHERE relname = 't';

\echo '=== 恢复索引扫描并再次查询 ==='

-- 恢复索引扫描
SET enable_indexscan = on;
SET enable_indexonlyscan = on;
SET enable_bitmapscan = on;

-- 再次查询表 t（可能使用 IndexScan）
EXPLAIN (COSTS off) SELECT oid, relname, relfilenode FROM pg_class WHERE relname = 't';
SELECT oid, relname, relfilenode 
FROM pg_class 
WHERE relname = 't';

\echo '=== 对比查询 pg_class 中全部表 ==='

-- 显示 pg_class 中所有的用户表，不带过滤条件
SELECT oid, relname, relfilenode 
FROM pg_class 
WHERE relkind = 'r' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
ORDER BY oid;

-- 检查表数据结构
\d t

\echo '=== 验证表数据是否存在 ==='

-- 查询表数据数量
SELECT COUNT(*) AS row_count FROM t;

-- 查询所有表数据
SELECT * FROM t ORDER BY key;

-- 验证期望的数据是否存在
SELECT 
    CASE WHEN EXISTS (SELECT 1 FROM t WHERE key = 1 AND value = 'test_data_1') 
         THEN '✓ 数据 (1, test_data_1) 存在' 
         ELSE '✗ 数据 (1, test_data_1) 丢失！' 
    END AS check_1,
    CASE WHEN EXISTS (SELECT 1 FROM t WHERE key = 2 AND value = 'test_data_2') 
         THEN '✓ 数据 (2, test_data_2) 存在' 
         ELSE '✗ 数据 (2, test_data_2) 丢失！' 
    END AS check_2,
    CASE WHEN EXISTS (SELECT 1 FROM t WHERE key = 3 AND value = 'test_data_3') 
         THEN '✓ 数据 (3, test_data_3) 存在' 
         ELSE '✗ 数据 (3, test_data_3) 丢失！' 
    END AS check_3;

\echo '表 t 验证完成'
EOF
    
    log_info "执行表验证 SQL（超时时间：30秒）..."
    local sql_output="${LOG_DIR}/verify_table_output_$(date +%Y%m%d_%H%M%S).log"
    local sql_pid=""
    
    # 在后台执行 gsql
    timeout 30s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1 &
    sql_pid=$!
    
    # 监控执行时间
    local elapsed=0
    local stuck=false
    
    while kill -0 $sql_pid 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        
        if [ $elapsed -eq 5 ]; then
            log_warning "表验证已超过 5 秒，可能卡住了..."
        fi
        
        if [ $elapsed -ge 30 ]; then
            log_error "表验证超时（30秒）"
            stuck=true
            break
        fi
        
        if [ $((elapsed % 5)) -eq 0 ]; then
            log_info "表验证中... (${elapsed}秒)"
        fi
    done
    
    # 等待进程结束
    wait $sql_pid 2>/dev/null
    local exit_code=$?
    
    # 检查结果
    log_info "表验证输出:"
    cat "${sql_output}"
    
    # 检查是否有 FATAL 错误
    if grep -q "FATAL:" "${sql_output}"; then
        log_error "检测到 FATAL 错误！"
        grep "FATAL:" "${sql_output}"
        return 1
    fi
    
    # 检查是否表不存在
    if grep -q "relation.*does not exist" "${sql_output}"; then
        log_error "表 t 不存在！数据丢失！"
        return 1
    fi
    
    # 检查是否有其他错误
    if grep -q "ERROR:" "${sql_output}"; then
        log_error "检测到 ERROR！"
        grep "ERROR:" "${sql_output}"
        return 1
    fi
    
    # 检查是否找到表
    if grep -q "| t |" "${sql_output}" || grep -qE "^\s*\d+\s*\|\s*t\s*\|" "${sql_output}"; then
        log_success "表 t 存在！"
    else
        log_warning "无法确认表 t 是否存在，请检查输出"
    fi
    
    # 检查数据是否丢失
    if grep -q "丢失" "${sql_output}"; then
        log_error "数据丢失！部分或全部数据未能从重启中恢复！"
        grep "丢失" "${sql_output}"
        return 1
    fi
    
    # 检查数据行数
    local row_count=$(grep -oP 'row_count\s*\n\s*-+\s*\n\s*\K\d+' "${sql_output}" || echo "0")
    if [ "$row_count" = "0" ]; then
        log_error "表 t 存在但数据为空！数据丢失！"
        return 1
    elif [ "$row_count" != "3" ]; then
        log_warning "表 t 数据行数不匹配，期望 3 行，实际 ${row_count} 行"
    else
        log_success "表 t 数据完整，共 ${row_count} 行"
    fi
    
    if [ $stuck = true ] || [ $exit_code -eq 124 ]; then
        log_error "表验证超时或卡住，问题已复现！"
        return 1
    elif [ $exit_code -eq 0 ]; then
        log_success "表验证成功！（耗时 ${elapsed} 秒）"
        return 0
    else
        log_error "表验证失败，退出码: ${exit_code}"
        return 1
    fi
}

# 建表测试：修改密码 + 建表 + 重启 + 验证表存在
# =====================================================
# 带主键约束的表测试 (test_branch)
# =====================================================
create_test_branch_table() {
    log_info "创建带主键约束的表 test_branch..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    log_info "连接字符串: ${connection_string}"
    
    # 创建 SQL 脚本
    local SQL_SCRIPT="${LOG_DIR}/create_test_branch.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
-- 步骤1: 修改密码
ALTER ROLE cloud_admin PASSWORD 'Huawei12#$';
\echo '密码修改成功'

-- 步骤2: 创建带主键约束的表 test_branch
DROP TABLE IF EXISTS test_branch;
CREATE TABLE test_branch (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
\echo '表 test_branch 创建成功'

-- 步骤3: 插入测试数据
INSERT INTO test_branch (name, value) VALUES ('test1', 100);
INSERT INTO test_branch (name, value) VALUES ('test2', 200);
INSERT INTO test_branch (name, value) VALUES ('test3', 300);
\echo '测试数据插入成功'

-- 步骤4: 查询表数据
\echo '=== 查询表数据 ==='
SELECT * FROM test_branch ORDER BY id;

-- 步骤5: 查询表信息
\echo '=== 表信息 ==='
SELECT oid, relname, relfilenode, reltablespace 
FROM pg_class 
WHERE relname = 'test_branch';

-- 步骤6: 查询索引信息
\echo '=== 索引信息 ==='
SELECT c.oid, c.relname, c.relfilenode 
FROM pg_class c 
WHERE c.relname LIKE 'test_branch%';

\echo '========================================='
\echo '表 test_branch 创建完成'
\echo '========================================='
EOF
    
    log_info "执行 SQL 脚本..."
    local sql_output="${LOG_DIR}/create_test_branch_output_$(date +%Y%m%d_%H%M%S).log"
    
    if timeout 60s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1; then
        log_success "表 test_branch 创建成功"
        cat "${sql_output}"
        return 0
    else
        log_error "表 test_branch 创建失败"
        cat "${sql_output}"
        return 1
    fi
}

verify_test_branch_table() {
    log_info "验证表 test_branch 是否存在且数据完整..."
    
    local connection_string=$(get_connection_string)
    if [ -z "$connection_string" ]; then
        return 1
    fi
    
    # 创建验证 SQL 脚本
    local SQL_SCRIPT="${LOG_DIR}/verify_test_branch.sql"
    cat > "${SQL_SCRIPT}" <<'EOF'
\echo '=== 验证表 test_branch 是否存在 ==='
SELECT EXISTS (
    SELECT 1 FROM pg_class WHERE relname = 'test_branch'
) AS table_exists;

\echo '=== 查询表数据 ==='
SELECT * FROM test_branch ORDER BY id;

\echo '=== 统计行数 ==='
SELECT COUNT(*) AS row_count FROM test_branch;

\echo '=== 验证主键索引是否存在 ==='
SELECT c.relname AS index_name
FROM pg_class c
JOIN pg_index i ON c.oid = i.indexrelid
JOIN pg_class t ON i.indrelid = t.oid
WHERE t.relname = 'test_branch' AND i.indisprimary;

\echo '=== 验证完成 ==='
EOF
    
    log_info "执行验证 SQL..."
    local sql_output="${LOG_DIR}/verify_test_branch_output_$(date +%Y%m%d_%H%M%S).log"
    
    set +e  # 临时允许错误
    timeout 30s gsql "${connection_string}" -f "${SQL_SCRIPT}" > "${sql_output}" 2>&1
    local exit_code=$?
    set -e
    
    cat "${sql_output}"
    
    # 检查是否有错误
    if grep -q "ERROR" "${sql_output}"; then
        log_error "验证过程中检测到 ERROR"
        grep "ERROR" "${sql_output}"
        return 1
    fi
    
    # 检查表是否存在
    if grep -q "t$" "${sql_output}" || grep -q "| t" "${sql_output}"; then
        log_success "表 test_branch 存在"
    else
        log_error "表 test_branch 不存在或查询失败"
        return 1
    fi
    
    # 检查行数
    if grep -qE "3|row_count.*3" "${sql_output}"; then
        log_success "表 test_branch 数据完整 (3 行)"
    else
        log_warning "表 test_branch 数据可能不完整"
    fi
    
    log_success "表 test_branch 验证通过"
    return 0
}

run_branch_test() {
    log_info "开始执行带主键约束的表重启测试..."
    log_info "========================================"
    log_info "测试步骤:"
    log_info "  1. 登录数据库修改密码"
    log_info "  2. 创建带主键约束的表 test_branch"
    log_info "  3. 插入测试数据"
    log_info "  4. 查询表数据"
    log_info "  5. 停止 main 分支"
    log_info "  6. 重启 main 分支"
    log_info "  7. 验证表和数据是否存在"
    log_info "========================================"
    echo ""
    
    # 步骤1-4: 创建表和插入数据
    if ! create_test_branch_table; then
        log_error "步骤1-4失败: 创建表或插入数据失败"
        return 1
    fi
    echo ""
    
    # 步骤5: 停止 main endpoint
    if ! stop_main_endpoint; then
        log_error "步骤5失败: 停止 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤6: 重启 main endpoint
    if ! restart_main_endpoint; then
        log_error "步骤6失败: 重启 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤7: 验证登录和表数据
    log_info "步骤7: 验证登录..."
    if ! verify_login; then
        log_error "步骤7失败: 登录验证失败"
        log_error "========================================"
        log_error "问题已复现！重启后无法登录数据库"
        log_error "========================================"
        return 1
    fi
    
    log_info "步骤7: 验证表 test_branch..."
    if ! verify_test_branch_table; then
        log_error "步骤7失败: 表验证失败"
        log_error "========================================"
        log_error "问题已复现！表 test_branch 丢失或数据不完整"
        log_error "========================================"
        return 1
    fi
    echo ""
    
    log_success "========================================="
    log_success "带主键约束的表重启测试通过！"
    log_success "表 test_branch 存在且数据完整"
    log_success "========================================="
    return 0
}

# =====================================================
# 原有的简单表测试 (table t)
# =====================================================
run_table_test() {
    log_info "开始执行建表和重启测试..."
    log_info "========================================"
    log_info "测试步骤:"
    log_info "  1. 登录数据库修改密码并创建表 t(key int, value text)"
    log_info "  2. 查询并记录表 OID"
    log_info "  3. 停止 main 分支"
    log_info "  4. 重启 main 分支"
    log_info "  5. 验证能否成功登录（超时30秒判定失败）"
    log_info "  6. 验证表 t 是否仍然存在"
    log_info "========================================"
    echo ""
    
    # 步骤1: 修改密码并创建表
    if ! change_password_and_create_table; then
        log_error "步骤1失败: 修改密码或建表失败"
        return 1
    fi
    echo ""
    
    # 步骤2: 停止 main endpoint
    if ! stop_main_endpoint; then
        log_error "步骤2失败: 停止 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤3: 重启 main endpoint
    if ! restart_main_endpoint; then
        log_error "步骤3失败: 重启 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤4: 验证登录
    if ! verify_login; then
        log_error "步骤4失败: 登录验证失败（可能超时或遇到错误）"
        log_error "========================================"
        log_error "问题已复现！重启后无法登录数据库"
        log_error "========================================"
        return 1
    fi
    echo ""
    
    # 步骤5: 验证表是否存在
    if ! verify_table_exists; then
        log_error "步骤5失败: 表验证失败"
        log_error "========================================"
        log_error "问题已复现！表 t 丢失或无法访问"
        log_error "========================================"
        return 1
    fi
    echo ""
    
    log_success "========================================="
    log_success "建表重启测试通过！表 t 仍然存在"
    log_success "========================================="
    return 0
}

run_password_test() {
    log_info "开始执行密码修改和重启测试..."
    log_info "========================================"
    log_info "测试步骤:"
    log_info "  1. 登录数据库修改密码"
    log_info "  2. 停止 main 分支"
    log_info "  3. 重启 main 分支"
    log_info "  4. 验证能否成功登录"
    log_info "========================================"
    echo ""
    
    # 步骤1: 修改密码
    if ! change_password; then
        log_error "步骤1失败: 修改密码失败"
        return 1
    fi
    echo ""
    
    # 步骤2: 停止 main endpoint
    if ! stop_main_endpoint; then
        log_error "步骤2失败: 停止 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤3: 重启 main endpoint
    if ! restart_main_endpoint; then
        log_error "步骤3失败: 重启 main 分支失败"
        return 1
    fi
    echo ""
    
    # 步骤4: 验证登录
    if ! verify_login; then
        log_error "步骤4失败: 登录验证失败"
        log_error "========================================"
        log_error "问题已复现！重启后无法登录数据库"
        log_error "========================================"
        return 1
    fi
    echo ""
    
    log_success "========================================"
    log_success "所有步骤成功！重启后可以正常登录数据库"
    log_success "========================================"
    return 0
}

test_reproduce() {
    local test_type="${1:-branch}"  # 默认带主键约束的表测试
    
    log_info "开始问题复现测试 (类型: ${test_type})..."
    echo "测试日志: ${TEST_LOG}"
    echo "" > "${TEST_LOG}"
    
    local start_time=$(date +%s)
    
    # 执行各个步骤
    if ! init_neon_env; then
        log_error "环境初始化失败"
        return 1
    fi
    
    if ! start_neon_services; then
        log_error "服务启动失败"
        return 1
    fi
    
    if ! create_tenant_and_endpoint; then
        log_error "创建 Tenant/Endpoint 失败"
        return 1
    fi
    
    # 根据测试类型执行不同的测试
    local test_result=0
    case "$test_type" in
        password)
            if ! run_password_test; then
                log_warning "密码测试失败或问题已复现"
                test_result=1
            fi
            ;;
        table)
            if ! run_table_test; then
                log_warning "建表测试失败或问题已复现"
                test_result=1
            fi
            ;;
        branch)
            if ! run_branch_test; then
                log_warning "带主键约束的表测试失败或问题已复现"
                test_result=1
            fi
            ;;
        *)
            log_error "未知的测试类型: $test_type (可选: password, table, branch)"
            return 1
            ;;
    esac
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # 收集日志
    log_info "收集诊断日志..."
    local timestamp=$(date +%Y%m%d_%H%M%S)
    
    if [ -f "${NEON_DIR}/pageserver_1/pageserver.log" ]; then
        local pageserver_copy="${LOG_DIR}/pageserver_${timestamp}.log"
        cp "${NEON_DIR}/pageserver_1/pageserver.log" "${pageserver_copy}"
        log_info "Pageserver 日志已复制到: ${pageserver_copy}"
        
        # 提取 LAYERDBG 日志
        log_info "提取 LAYERDBG 调试日志..."
        grep "LAYERDBG" "${pageserver_copy}" > "${LOG_DIR}/layerdbg_${timestamp}.log" 2>/dev/null || true
        
        # 提取 walredo stderr 日志
        log_info "提取 walredo stderr 日志..."
        grep "wal-redo-postgres-stderr" "${pageserver_copy}" > "${LOG_DIR}/walredo_stderr_${timestamp}.log" 2>/dev/null || true
        
        # 提取 pg_authid 相关日志
        log_info "提取 pg_authid 相关日志..."
        grep -E "14774|39B6|pg_authid" "${pageserver_copy}" > "${LOG_DIR}/pg_authid_${timestamp}.log" 2>/dev/null || true
    fi
    
    # 收集 compute 日志
    if [ -f "${NEON_DIR}/endpoints/main/compute.log" ]; then
        local compute_copy="${LOG_DIR}/compute_${timestamp}.log"
        cp "${NEON_DIR}/endpoints/main/compute.log" "${compute_copy}"
        log_info "Compute 日志已复制到: ${compute_copy}"
    fi
    
    if [ $test_result -eq 0 ]; then
        log_success "测试完成！耗时: ${duration} 秒"
    else
        log_warning "测试完成（有错误）！耗时: ${duration} 秒"
    fi
    
    return $test_result
}

# ============================================
# 阶段 3: 环境清理
# ============================================
cleanup_env() {
    log_info "开始清理环境..."
    
    # Kill 所有相关进程
    log_info "停止所有 neon_branch_dev 相关进程..."
    pkill -9 -f "neon_branch_dev" || true
    sleep 2
    
    # 确认进程已停止
    local remaining=$(pgrep -f "neon_branch_dev" | wc -l)
    if [ $remaining -gt 0 ]; then
        log_warning "仍有 ${remaining} 个进程在运行"
        pgrep -af "neon_branch_dev"
    else
        log_success "所有进程已停止"
    fi
    
    # 删除 .neon 目录
    if [ -d "${NEON_DIR}" ]; then
        log_info "删除 .neon 目录..."
        rm -rf "${NEON_DIR}"
        log_success ".neon 目录已删除"
    fi
    
    # 可选：清理构建产物（谨慎使用）
    if [ "$1" = "--deep" ]; then
        log_warning "执行深度清理（删除构建产物）..."
        read -p "确认要删除所有构建产物？这将需要重新编译！(y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cd "${PROJECT_DIR}"
            make clean || true
            log_success "构建产物已清理"
        else
            log_info "跳过构建产物清理"
        fi
    fi
    
    log_success "环境清理完成"
}

# ============================================
# 主函数
# ============================================
show_usage() {
    cat <<EOF
用法: $0 [COMMAND] [OPTIONS]

命令:
  build           仅执行编译
  test [TYPE]     执行问题复现测试
                  TYPE: branch (默认) - 带主键约束的表测试
                        table - 简单建表测试
                        password - 密码修改测试
  clean           仅执行环境清理
  all [TYPE]      执行所有阶段（默认）
  help            显示此帮助信息

选项:
  --deep          (仅用于 clean) 深度清理，包括删除构建产物

示例:
  $0                    # 执行所有阶段（带主键约束的表测试）
  $0 all branch         # 执行所有阶段（带主键约束的表测试）
  $0 all table          # 执行所有阶段（简单建表测试）
  $0 all password       # 执行所有阶段（密码测试）
  $0 build              # 仅编译
  $0 test               # 仅测试（带主键约束的表）
  $0 test branch        # 仅测试（带主键约束的表）
  $0 test table         # 仅测试（简单建表）
  $0 clean              # 清理环境
  $0 clean --deep       # 深度清理（包括构建产物）

日志位置:
  构建日志:     ${LOG_DIR}/build_*.log
  测试日志:     ${LOG_DIR}/test_*.log
  Pageserver:   ${LOG_DIR}/pageserver_*.log
  Compute:      ${LOG_DIR}/compute_*.log
  LAYERDBG:     ${LOG_DIR}/layerdbg_*.log
  walredo:      ${LOG_DIR}/walredo_stderr_*.log
  pg_authid:    ${LOG_DIR}/pg_authid_*.log
EOF
}

main() {
    local command="${1:-all}"
    local option="${2:-}"
    
    log_info "========================================"
    log_info "WalRedo 问题自动化测试脚本"
    log_info "项目目录: ${PROJECT_DIR}"
    log_info "日志目录: ${LOG_DIR}"
    log_info "========================================"
    echo ""
    
    case "$command" in
        build)
            build_project
            ;;
        test)
            test_reproduce "$option"
            ;;
        clean)
            cleanup_env "$option"
            ;;
        all)
            local test_type="${option:-branch}"
            log_info "执行完整测试流程 (测试类型: ${test_type})..."
            echo ""
            
            # 1. 编译
            log_info "步骤 1/3: 编译"
            if ! build_project; then
                log_error "编译失败，终止测试"
                exit 1
            fi
            echo ""
            
            # 2. 测试
            log_info "步骤 2/3: 问题复现 (类型: ${test_type})"
            test_reproduce "$test_type"
            local test_exit=$?
            echo ""
            
            # 3. 清理
            log_info "步骤 3/3: 环境清理"
            cleanup_env
            echo ""
            
            # 总结
            log_info "========================================"
            if [ $test_exit -eq 0 ]; then
                log_success "所有步骤完成！"
            else
                log_warning "测试完成，但有错误发生"
                log_info "请查看日志文件获取详细信息"
            fi
            log_info "========================================"
            
            exit $test_exit
            ;;
        help|--help|-h)
            show_usage
            exit 0
            ;;
        *)
            log_error "未知命令: $command"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# 捕获 Ctrl+C
trap 'log_warning "收到中断信号，正在清理..."; cleanup_env; exit 130' INT TERM

# 执行主函数
main "$@"



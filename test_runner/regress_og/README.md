# Neon Branching 测试框架使用指南

本文档详细介绍了 Neon Branching 测试框架的使用方法，包括环境配置、测试用例编写、分支管理等内容。

---

## 目录

1. [快速开始](#快速开始)
2. [环境配置](#环境配置)
3. [目录结构](#目录结构)
4. [运行测试](#运行测试)
5. [编写测试用例](#编写测试用例)
6. [创建测试计划](#创建测试计划)
7. [分支操作](#分支操作)
8. [端口配置](#端口配置)
9. [环境变量](#环境变量)
10. [常见问题](#常见问题)
11. [高级用法](#高级用法)

---

## 快速开始

### 1. 编译 Neon

```bash
# 进入 Neon 项目根目录
cd /path/to/neon_branch_dev

# 编译 debug 版本
cargo build

# 或编译 release 版本
cargo build --release
```

### 2. 设置环境变量

```bash
# 设置 openGauss 安装路径（必须）
export OPENGAUSS_DISTRIB_DIR=/path/to/og_install
export POSTGRES_DISTRIB_DIR=/path/to/og_install

# 设置 openGauss 版本（可选，默认 V702）
export DEFAULT_PG_VERSION=V702

# 设置编译类型（可选，默认 debug）
export BUILD_TYPE=debug
```

### 3. 运行测试

```bash
cd test_runner/regress_og

# 运行基本测试
make check-basic

# 运行分支继承测试
make check-branch-inherit

# 清理环境
make clean-all
```

---

## 环境配置

### 系统要求

| 依赖项 | 版本要求 | 说明 |
|--------|----------|------|
| openGauss | V702+ | 数据库内核 |
| Rust | 1.70+ | Neon 编译 |
| Bash | 4.0+ | 脚本执行 |

### 必需的系统环境变量

在 `~/.bashrc` 或 `~/.bash_profile` 中添加：

```bash
# ========================================
# Neon 测试框架必需的环境变量
# ========================================

# openGauss 安装目录（包含 V702 等版本子目录）
export OPENGAUSS_DISTRIB_DIR=/path/to/og_install
export POSTGRES_DISTRIB_DIR=/path/to/og_install

# openGauss 版本
export DEFAULT_PG_VERSION=V702

# 添加到 PATH
export PATH=$OPENGAUSS_DISTRIB_DIR/$DEFAULT_PG_VERSION/bin:$PATH

# 库路径
export LD_LIBRARY_PATH=$OPENGAUSS_DISTRIB_DIR/$DEFAULT_PG_VERSION/lib:$LD_LIBRARY_PATH

# Neon 二进制路径（编译后）
export NEON_ROOT=/path/to/neon_branch_dev
export PATH=$NEON_ROOT/target/debug:$PATH
```

### 验证环境

```bash
# 验证 gsql
gsql --version

# 验证 neon_local
neon_local --help

# 验证路径
echo $OPENGAUSS_DISTRIB_DIR
echo $NEON_ROOT
```

---

## 目录结构

```
test_runner/regress_og/
├── Makefile                    # 构建和测试自动化
├── README.md                   # 本文档
├── run_tests.sh                # 测试运行器（动态生成 gsql wrapper）
│
├── sql/                        # SQL 测试文件目录
│   ├── neon_env_setup.sh       # 环境启动脚本
│   ├── neon_env_cleanup.sh     # 环境清理脚本
│   ├── neon_setup.sql          # 数据库初始化
│   ├── neon_basic.sql          # 基本功能测试
│   ├── neon_branch_data_inherit_test.sql  # 分支继承测试
│   └── ...
│
├── expected/                   # 预期输出文件目录
│   ├── neon_basic.out
│   └── ...
│
├── results/                    # 测试结果输出目录（自动生成）
│   ├── neon_basic.out
│   ├── neon_basic.diff
│   └── ...
│
├── bin/                        # 工具脚本目录
│   ├── diff                    # 自定义 diff 工具
│   └── neon_gsql               # 静态 gsql wrapper（备用）
│
├── tmp-bin/                    # 动态生成的 wrapper（自动生成）
│   └── gsql                    # 带预设变量的 gsql wrapper
│
├── .neon/                      # Neon 运行时目录（自动生成）
│
├── basic_schedule              # 基本测试计划
├── branch_inherit_schedule     # 分支继承测试计划
└── *_schedule                  # 其他测试计划
```

---

## 运行测试

### 使用 Makefile（推荐）

```bash
# 查看所有可用目标
make help

# 常用测试目标
make check-basic           # 基本功能测试
make check-branch-inherit  # 分支数据继承测试
make check-branching       # 分支测试
make check-neon            # 完整测试套件

# 环境管理
make start-neon            # 启动 Neon
make stop-neon             # 停止 Neon
make stop-neon-force       # 强制停止（杀死残留进程）
make clean-all             # 完全清理

# 开发辅助
make list-tests            # 列出所有测试
make list-schedules        # 列出所有测试计划
make update-expected TEST=neon_basic  # 更新预期输出
```

### 使用 run_tests.sh

```bash
# 运行单个测试
./run_tests.sh --test=neon_basic --verbose

# 运行测试计划
./run_tests.sh --schedule=basic_schedule --verbose

# 指定连接参数
./run_tests.sh --test=neon_basic --host=127.0.0.1 --port=55432
```

### 手动运行 SQL 文件

```bash
# 启动环境
bash sql/neon_env_setup.sh

# 使用动态生成的 wrapper（推荐）
./tmp-bin/gsql -f sql/your_test.sql

# 或使用静态 wrapper
./bin/neon_gsql -f sql/your_test.sql
```

---

## 编写测试用例

### 步骤 1：创建 SQL 文件

在 `sql/` 目录下创建新的 `.sql` 文件：

```bash
touch sql/my_new_test.sql
```

### 步骤 2：编写测试内容

```sql
-- ============================================================================
-- my_new_test.sql
-- 测试描述：这里写测试的目的和内容
-- ============================================================================

\echo '=============================================='
\echo '  My New Test'
\echo '=============================================='

-- 显示当前连接信息
\conninfo

-- 创建测试表
DROP TABLE IF EXISTS test_table CASCADE;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INT
);

-- 插入测试数据
INSERT INTO test_table (name, value) VALUES
    ('row1', 100),
    ('row2', 200);

-- 验证数据
SELECT * FROM test_table ORDER BY id;

-- 清理
DROP TABLE IF EXISTS test_table CASCADE;

\echo '  Test Complete!'
```

### 步骤 3：运行测试生成预期输出

```bash
# 首次运行会自动创建 expected 文件
./run_tests.sh --test=my_new_test --verbose
```

### 步骤 4：验证并更新预期输出

```bash
# 查看结果
cat results/my_new_test.out

# 如果输出正确，更新预期文件
make update-expected TEST=my_new_test
```

---

## 创建测试计划

### 步骤 1：创建 schedule 文件

在项目根目录创建 `*_schedule` 文件：

```bash
touch my_test_schedule
```

### 步骤 2：编写测试计划

```
# my_test_schedule
# 测试计划描述
#
# 格式：test: <test_name>
# 可以指定多个测试，按顺序执行

# Phase 1: 环境启动（必须放在第一个）
test: neon_env_setup

# Phase 2: 数据库初始化（如果需要）
test: neon_setup

# Phase 3: 你的测试
test: my_new_test
test: another_test

# Phase 4: 清理（可选）
test: neon_cleanup
```

### 步骤 3：添加 Makefile 目标（可选）

编辑 `Makefile`，添加新的目标：

```makefile
# 在 .PHONY 行添加新目标
.PHONY: ... check-my-test

# 添加目标定义
check-my-test: setup
	@echo "=== Running My Test Suite ==="
	@bash sql/neon_env_setup.sh || exit 1
	@./run_tests.sh --schedule=my_test_schedule --host=$(HOST) --port=$(PORT) --skip-env-setup --verbose
```

### 步骤 4：运行测试计划

```bash
# 使用 Makefile
make check-my-test

# 或直接使用 run_tests.sh
./run_tests.sh --schedule=my_test_schedule --verbose
```

---

## 分支操作

### 在 SQL 文件中创建分支

测试框架通过 `\!` 命令执行 shell 命令来创建分支：

```sql
-- ============================================================
-- 创建新分支
-- ============================================================

-- 方法 1：使用 neon_local（需要在 PATH 中）
\! neon_local timeline branch --branch-name my_branch

-- 方法 2：使用完整路径（更可靠）
\! ${NEON_ROOT}/target/debug/neon_local timeline branch --branch-name my_branch

-- 从指定 LSN 创建分支
\! neon_local timeline branch --branch-name my_branch --ancestor-start-lsn 0/1234567
```

### 为分支创建端点

```sql
-- 创建端点（使用预设端口变量）
\! neon_local endpoint create ep-my-branch --branch-name my_branch --pg-port ${BRANCH1_PORT}

-- 启动端点
\! neon_local endpoint start ep-my-branch

-- 等待端点就绪
\! sleep 5

-- 查看端点列表
\! neon_local endpoint list
```

### 切换数据库实例

使用 `\c` 命令和预设端口变量切换连接：

```sql
-- ============================================================
-- 切换数据库实例
-- ============================================================

-- 显示预设的端口变量
\echo 'main_port:    ' :main_port
\echo 'branch1_port: ' :branch1_port

-- 切换到主分支
\c - - - :main_port

-- 切换到分支 1
\c - - - :branch1_port

-- 切换到分支 2
\c - - - :branch2_port

-- 验证当前连接
\conninfo
```

### 预设端口变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `:main_port` | 55432 | 主分支端口 |
| `:branch1_port` | 55435 | 分支 1 端口 |
| `:branch2_port` | 55436 | 分支 2 端口 |
| `:branch3_port` | 55437 | 分支 3 端口 |
| `:default_user` | cloud_admin | 默认用户 |
| `:default_db` | postgres | 默认数据库 |
| `:default_host` | 127.0.0.1 | 默认主机 |

### 预设环境变量（用于 \! 命令）

| 变量名 | 说明 |
|--------|------|
| `$NEON_ROOT` | Neon 项目根目录 |
| `$REGRESS_DIR` | 测试目录路径 |
| `$MAIN_PORT` | 主分支端口 |
| `$BRANCH1_PORT` | 分支 1 端口 |
| `$BRANCH2_PORT` | 分支 2 端口 |
| `$BRANCH3_PORT` | 分支 3 端口 |

---

## 端口配置

### 修改默认端口

编辑 `run_tests.sh` 中的端口配置：

```bash
# Port settings for multi-endpoint testing
# Main endpoint uses 55432, branches start from 55435
MAIN_PORT="55432"
BRANCH1_PORT="55435"
BRANCH2_PORT="55436"
BRANCH3_PORT="55437"
```

### 添加新的预设端口

#### 步骤 1：修改 run_tests.sh

```bash
# 在端口配置部分添加
BRANCH4_PORT="55438"
BRANCH5_PORT="55439"
```

#### 步骤 2：更新 generate_gsql_wrapper 函数

```bash
generate_gsql_wrapper() {
    # ... 现有代码 ...
    
    cat > "${wrapper}" << EOF
    # ... 现有代码 ...
    
    # 添加新的环境变量
    export BRANCH4_PORT="${BRANCH4_PORT}"
    export BRANCH5_PORT="${BRANCH5_PORT}"
    
    exec "\${GSQL_REAL}" \\
        # ... 现有变量 ...
        --variable=branch4_port=${BRANCH4_PORT} \\
        --variable=branch5_port=${BRANCH5_PORT} \\
        "\$@"
EOF
}
```

#### 步骤 3：在 SQL 中使用

```sql
-- 使用新的端口变量
\c - - - :branch4_port
\! neon_local endpoint create ep-branch4 --branch-name branch4 --pg-port ${BRANCH4_PORT}
```

---

## 环境变量

### 在 Makefile 中设置

```makefile
# 在 Makefile 顶部设置
export OPENGAUSS_DISTRIB_DIR := /path/to/og_install
export POSTGRES_DISTRIB_DIR := /path/to/og_install
export DEFAULT_PG_VERSION := V702
export BUILD_TYPE := debug
```

### 在命令行中覆盖

```bash
# 使用不同的端口
make check-basic PORT=55440

# 使用不同的主机
make check-basic HOST=192.168.1.100

# 使用 release 版本
BUILD_TYPE=release make check-basic

# 使用不同的 openGauss 版本
DEFAULT_PG_VERSION=V703 make check-basic
```

### 在 SQL 文件中访问

```sql
-- gsql 变量（使用 : 前缀）
\echo 'Port: ' :main_port

-- 环境变量（在 \! 命令中）
\! echo "NEON_ROOT is: $NEON_ROOT"
\! echo "Branch1 port is: $BRANCH1_PORT"
```

---

## 常见问题

### Q1: `neon_local: command not found`

**原因**：neon_local 不在 PATH 中

**解决方案**：
```bash
# 添加到 PATH
export PATH=$NEON_ROOT/target/debug:$PATH

# 或在 SQL 中使用完整路径
\! ${NEON_ROOT}/target/debug/neon_local endpoint list
```

### Q2: `\connect: expected authentication request from server, but received H`

**原因**：SSL 连接问题或端口不正确

**解决方案**：
1. 确保端点正在运行：`neon_local endpoint list`
2. 检查端口是否匹配
3. 确保使用正确的 gsql wrapper

### Q3: `openGauss directory 'bin' not found`

**原因**：`OPENGAUSS_DISTRIB_DIR` 设置不正确

**解决方案**：
```bash
# 确保使用绝对路径
export OPENGAUSS_DISTRIB_DIR=$(cd /path/to/og_install && pwd)
```

### Q4: 测试失败，diff 显示差异

**解决方案**：
```bash
# 查看差异
cat results/test_name.diff

# 如果输出正确，更新预期文件
make update-expected TEST=test_name
```

### Q5: 端点状态显示 "running, no pidfile"

**原因**：端点进程已终止但未正确清理

**解决方案**：
```bash
# 强制清理
make stop-neon-force
# 或
bash sql/neon_env_cleanup.sh --force
```

---

## 高级用法

### 并行测试

在 schedule 文件中，同一行的测试会并行执行：

```
# 串行执行
test: test1
test: test2

# 并行执行（同一行，空格分隔）
test: test1 test2 test3
```

### 自定义 diff 规则

编辑 `bin/diff` 添加规范化规则：

```bash
# 忽略时间戳
sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/TIMESTAMP/g'

# 忽略 LSN
sed 's/0\/[0-9A-F]\{7,8\}/X\/XXXXXXXX/g'
```

### 条件测试

```sql
-- 检查条件后决定是否继续
SELECT CASE 
    WHEN current_setting('server_version_num')::int >= 140000 
    THEN 'continue'
    ELSE 'skip'
END AS version_check;
```

### 数据继承验证模板

```sql
-- ============================================================
-- 分支数据继承验证模板
-- ============================================================

-- Step 1: 在主分支创建数据
\c - - - :main_port
CREATE TABLE test_data (id INT, value TEXT);
INSERT INTO test_data VALUES (1, 'from_main');

-- Step 2: 记录 LSN
SELECT pg_current_xlog_location() AS branch_lsn;

-- Step 3: 创建分支
\! neon_local timeline branch --branch-name test_branch

-- Step 4: 创建并启动端点
\! neon_local endpoint create ep-test --branch-name test_branch --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep-test
\! sleep 5

-- Step 5: 验证继承
\c - - - :branch1_port
SELECT * FROM test_data;  -- 应该能看到 from_main

-- Step 6: 验证隔离
INSERT INTO test_data VALUES (2, 'from_branch');
\c - - - :main_port
SELECT * FROM test_data;  -- 应该只有 from_main

-- Step 7: 清理
DROP TABLE test_data;
\! neon_local endpoint stop ep-test
```

---

## 完整示例

### 示例：创建完整的分支测试

1. **创建 SQL 文件** `sql/my_branch_test.sql`：

```sql
\echo '====== My Branch Test ======'

-- 在主分支创建数据
\c - - - :main_port
DROP TABLE IF EXISTS branch_test;
CREATE TABLE branch_test (id SERIAL, source TEXT, data INT);
INSERT INTO branch_test (source, data) VALUES ('main', 100);
SELECT * FROM branch_test;

-- 创建分支
\! neon_local timeline branch --branch-name my_test_branch
\! neon_local endpoint create ep-my-test --branch-name my_test_branch --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep-my-test
\! sleep 5

-- 验证继承
\c - - - :branch1_port
SELECT 
    CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS inheritance_test
FROM branch_test WHERE source = 'main';

-- 添加分支数据
INSERT INTO branch_test (source, data) VALUES ('branch', 200);

-- 验证隔离
\c - - - :main_port
SELECT 
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS isolation_test
FROM branch_test WHERE source = 'branch';

-- 清理
DROP TABLE branch_test;
\! neon_local endpoint stop ep-my-test

\echo '====== Test Complete ======'
```

2. **创建测试计划** `my_branch_test_schedule`：

```
test: neon_env_setup
test: my_branch_test
```

3. **运行测试**：

```bash
./run_tests.sh --schedule=my_branch_test_schedule --verbose
```

---

## 维护者信息

如有问题，请联系项目维护者或查看项目 Issue。

---

*最后更新：2025-12-27*

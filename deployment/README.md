# Neon Branching 两节点分布式部署指南

## 概述

本文档描述如何使用自动化脚本在两台机器上部署分布式 Neon Branching 系统。

### 系统架构

部署采用两节点架构，组件分布如下：

| 组件 | 机器A (主节点) | 机器B (从节点) |
|------|---------------|---------------|
| Storage Broker | ✓ | - |
| Storage Controller | ✓ | - |
| Endpoint Storage | ✓ | - |
| Safekeeper | ID=1 | ID=2, ID=3 |
| Pageserver | ID=1 | ID=2, ID=3 |
| Endpoints | ✓ | - |

> **注意**: Safekeeper和Pageserver分布可通过 `path.conf` 灵活配置。

---

## 前置条件

1. **环境准备**: 在机器A上已完成 Neon 项目编译，在机器B上已完成neon branching所需的依赖包和rust安装
2. **OpenGauss 安装**: `og_install/V702/` 目录下已安装 OpenGauss
3. **网络连通**: 两台机器之间网络互通，防火墙已开放相应端口
4. **用户权限**: 两台机器使用相同用户名
5. **机器限制**: 两台机器建议使用相同的操作系统版本

---

## 配置文件说明 (path.conf)

`path.conf` 是部署的核心配置文件，分为两部分：**Bash 变量** 和 **TOML 配置**。

### 第一部分：基本路径配置

```bash
# ============================================
# 机器A（本机）配置
# ============================================
MACHINE_A_IP=<机器A的IP地址>
NEON_SOURCE_A_DIR=<机器A源代码目录>
NEON_BIN_A_DIR="${NEON_SOURCE_A_DIR}/target/release"  # 编译产物目录
NEON_DATA_A_DIR=<机器A数据存储目录>

# ============================================
# 机器B（远程）配置
# ============================================
MACHINE_B_IP=<机器B的IP地址>
MACHINE_B_USER=<机器B的SSH用户名>
NEON_SOURCE_B_DIR=<机器B源代码目录>
NEON_BIN_B_DIR="${NEON_SOURCE_B_DIR}/target/release"  # 编译产物目录
NEON_DATA_B_DIR=<机器B数据存储目录>
```

### 第二部分：组件分布配置

通过修改 `p_host` 和 `s_host` 变量决定各组件部署在哪台机器：

```bash
# Pageserver 主机配置
p_host1=${MACHINE_A_IP}   # Pageserver 1 部署在机器A
p_host2=${MACHINE_B_IP}   # Pageserver 2 部署在机器B
p_host3=${MACHINE_B_IP}   # Pageserver 3 部署在机器B

# Safekeeper 主机配置
s_host1=${MACHINE_A_IP}   # Safekeeper 1 部署在机器A
s_host2=${MACHINE_B_IP}   # Safekeeper 2 部署在机器B
s_host3=${MACHINE_B_IP}   # Safekeeper 3 部署在机器B
```

### 第三部分：TOML 服务配置

文件后半部分为 TOML 格式的服务配置，包含：

- `default_tenant_id`: 默认租户ID
- `[[pageservers]]`: Pageserver 监听地址和端口
- `[[safekeepers]]`: Safekeeper 监听地址和端口
- `[broker]`: Storage Broker 配置
- `[storage_controller]`: Storage Controller 配置

> **提示**: TOML 配置中使用 `${变量名}` 引用上方定义的 Bash 变量，部署脚本会自动替换。

---

## 部署步骤

### 步骤1: 修改配置文件

```bash
cd <NEON_SOURCE_A_DIR>/deployment
vim path.conf
```

根据实际环境修改以下配置：
- 两台机器的 IP 地址
- 源代码、二进制、数据目录路径
- 机器B 的 SSH 用户名
- 组件分布（可选，默认配置即可）

### 步骤2: 执行部署脚本

```bash
./deploy_neon.sh
```

部署脚本将自动完成：
1. ✅ 检查编译产物（二进制文件和扩展库）
2. ✅ 配置 SSH 免密登录到机器B
3. ✅ 分发二进制文件和 OpenGauss 到机器B
4. ✅ 生成机器A和机器B的配置文件
5. ✅ 初始化两台机器的 Neon 环境
6. ✅ 生成 `start_neon.sh` 和 `stop_neon.sh` 脚本

### 步骤3: 启动服务

```bash
./start_neon.sh
```

### 步骤4: 创建租户和计算节点

```bash
cd ${NEON_SOURCE_A_DIR}
export NEON_REPO_DIR=${NEON_DATA_A_DIR}

# 创建租户
cargo neon tenant create

# 创建 timeline
cargo neon timeline create --branch-name main

# 启动计算节点
cargo neon endpoint start main --pg-port 55432
```

---

## 关键概念：NEON_REPO_DIR 环境变量

**`NEON_REPO_DIR` 是 Neon 系统的核心环境变量，用于指定数据目录位置。**

### 为什么需要设置

所有 `cargo neon` 命令都依赖此变量来定位：
- 配置文件（`config`）
- Pageserver 数据目录
- Safekeeper 数据目录
- Endpoint 数据目录
- PID 文件等

### 如何设置

在执行任何 `cargo neon` 命令前，必须先设置：

```bash
# 机器A - 设置为机器A的数据目录
export NEON_REPO_DIR=${NEON_DATA_A_DIR}

# 然后执行命令
cargo neon tenant list
cargo neon endpoint start main --pg-port 55432
```

### 机器B 的设置

在机器B上操作时，使用 `neon_local` 并设置对应的数据目录：

```bash
# SSH 到机器B后
export NEON_REPO_DIR=${NEON_DATA_B_DIR}
${NEON_BIN_B_DIR}/neon_local safekeeper status 2
```

### 持久化设置（可选）

将环境变量添加到 `~/.bashrc`：

```bash
echo 'export NEON_REPO_DIR=<数据目录路径>' >> ~/.bashrc
source ~/.bashrc
```

---

## 端口配置

| 组件 | ID | PG端口 | HTTP端口 | gRPC端口 |
|------|:--:|:------:|:--------:|:--------:|
| Pageserver | 1 | 62000 | 7898 | 49051 |
| Pageserver | 2 | 62001 | 7899 | 49052 |
| Pageserver | 3 | 62002 | 7900 | 49053 |
| Safekeeper | 1 | 3454 | 5676 | - |
| Safekeeper | 2 | 3455 | 5677 | - |
| Safekeeper | 3 | 3456 | 5678 | - |
| Storage Broker | - | - | - | 48051 |
| Storage Controller | - | - | 6082 | - |
| Compute Node | - | 55432 | 3080 | - |

---

## 常用运维命令

### 服务管理

```bash
# 启动所有服务
./start_neon.sh

# 停止所有服务
./stop_neon.sh
```

## 文件清单

| 文件 | 说明 |
|------|------|
| `path.conf` | 配置文件（需用户编辑） |
| `deploy_neon.sh` | 主部署脚本 |
| `start_neon.sh` | 启动脚本（自动生成） |
| `stop_neon.sh` | 停止脚本（自动生成） |
| `README.md` | 本文档 |

---

## 注意事项

1. **部署顺序**: 必须在机器A上执行部署脚本，脚本会自动处理机器B
2. **数据目录**: 两台机器的 `NEON_DATA_*_DIR` 应指向有足够空间的磁盘
3. **用户一致性**: 建议两台机器使用相同的用户名和目录结构
4. **防火墙**: 确保开放上述端口表中的所有端口
5. **NEON_REPO_DIR**: 执行任何 `cargo neon` 命令前务必设置此环境变量

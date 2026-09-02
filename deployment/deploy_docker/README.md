# docker一键部署指南

## 概述

本目录提供 Branching Docker 的两段式部署流程：

1. `install.sh` 负责创建 `openGauss-branching-deploy/` 目录，并通过 `curl` 下载部署文件。
2. `deploy.sh` 负责拉取/校验镜像，初始化 control plane，启动存储服务，并创建默认 `main` endpoint。

### 组件说明

| 组件 | 说明 | 默认端口 |
|------|------|----------|
| docker_control_plane | 控制平面 | `8080` |
| storage_controller | 存储调度与元数据服务 | `1234` |
| storage_broker | 存储 broker | `50051` |
| safekeeper | WAL 服务 | `7676` |
| endpoint_storage | endpoint 存储 | `9993` |
| 默认 compute endpoint | `main` endpoint | `55433` / `3080` |

## 环境要求

### 系统要求

- Linux
- 建议 ARM64 / `aarch64`
- 至少 8GB 内存
- 至少 20GB 可用磁盘

### 软件要求

- Docker
- Docker Compose v2
- `curl`
- `python3`

检查示例：

```bash
docker --version
docker compose version
curl --version
python3 --version
uname -m
```

### 网络要求

- 能访问 `raw.gitcode.com`
- 能访问 `gitcode.com`
- 能访问镜像仓库 `swr.cn-north-4.myhuaweicloud.com`

## 快速开始

### 第一步：安装部署文件

```bash
curl -fsSL https://raw.gitcode.com/opengauss/neon/raw/neon_release_9129/deployment/deploy_docker/install.sh -o install.sh
bash install.sh
```

默认会下载安装到：

```text
openGauss-branching-deploy/
```

### 第二步：执行部署

```bash
bash openGauss-branching-deploy/deploy.sh arm --skip-pull
```

如需使用 x86 参数：

```bash
bash openGauss-branching-deploy/deploy.sh x86 --skip-pull
```

## 安装说明

`install.sh` 会下载以下内容：

- `deploy.sh`
- `deploy.env`
- `docker-compose/docker-compose.yml`
- `docker-compose/bin/docker_local`
- `docker-compose/bin/docker_local.py`
- `docker-compose/pageserver_config/*.toml`
- `docker-compose/pageserver_config/metadata.json`
- `compute/shell/compute.sh`
- `compute/shell/storage_controller_db.sh`

如果远端 raw 地址变更，可在安装时覆盖：

```bash
RAW_BASE_URL=https://raw.gitcode.com/<owner>/<repo>/raw/<branch> bash install.sh
```

## 部署说明

`deploy.sh` 只负责部署执行，不再下载文件。运行前请确认 `install.sh` 已完成。

常用行为：

- `--skip-pull` 跳过镜像拉取，直接使用本地 `og_compute:latest` 和 `og_storage:latest`
- 脚本会等待 pageserver 注册到 storage controller 后，再创建默认 tenant
- 然后创建并启动默认 `main` endpoint

### 关键配置

`openGauss-branching-deploy/deploy.env` 是主要部署配置文件，常见项包括：

| 变量 | 说明 |
|------|------|
| `COMPOSE_PROJECT_NAME` | Compose 项目前缀 |
| `OG_VERSION` | openGauss 版本标识 |
| `NUM_PAGESERVERS` | pageserver 数量 |
| `NUM_SAFEKEEPERS` | safekeeper 数量 |
| `INIT_FORCE` | 初始化行为控制 |
| `INIT_TIMEOUT` | 初始化超时 |
| `HEALTH_CHECK_TIMEOUT` | 健康检查超时 |
| `CREATE_DEFAULT_TENANT` | 是否创建默认 tenant |
| `CREATE_DEFAULT_ENDPOINT` | 是否创建默认 endpoint |
| `DEFAULT_ENDPOINT_PG_PORT` | 默认 endpoint PostgreSQL 端口 |
| `DEFAULT_ENDPOINT_HTTP_PORT` | 默认 endpoint HTTP 端口 |
| `DOCKER_CONTROL_PLANE_HOST_PORT` | control plane 宿主机端口 |
| `STORAGE_CONTROLLER_HOST_PORT` | storage controller 宿主机端口 |
| `SKIP_PULL` | 是否跳过镜像拉取 |

## 常用命令

进入部署目录后，可使用 `docker_local` 管理实例：

```bash
cd openGauss-branching-deploy
./docker-compose/bin/docker_local status
./docker-compose/bin/docker_local logs --tail 100
./docker-compose/bin/docker_local stop
./docker-compose/bin/docker_local start
```

查看 endpoint：

```bash
./docker-compose/bin/docker_local endpoint list
./docker-compose/bin/docker_local endpoint status main
```

## 访问服务

部署成功后，常用访问地址：

- control plane: `http://127.0.0.1:8080`
- storage controller: `http://127.0.0.1:1234`
- endpoint HTTP: `http://127.0.0.1:3080`
- endpoint PostgreSQL: `127.0.0.1:55433`

如修改了 `deploy.env` 中的端口，以上地址也会随之变化。

## 文件说明

| 文件 | 作用 |
|------|------|
| `install.sh` | 创建部署目录并下载部署文件 |
| `deploy.sh` | 执行部署流程 |
| `deploy.env` | 部署配置 |
| `docker-compose/` | compose 与运行时配置 |
| `compute/shell/compute.sh` | endpoint 启动脚本 |
| `compute/shell/storage_controller_db.sh` | storage controller DB 初始化脚本 |

## 说明

- `deploy.sh` 的命令示例统一为 `bash openGauss-branching-deploy/deploy.sh <arm|x86> --skip-pull`
- `install.sh` 仅负责下载，不负责启动服务
- 如果提示缺少部署文件，请先运行 `install.sh`

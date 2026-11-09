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

默认使用宿主机本地目录作为Pageserver的远端存储，保持原有部署方式不变。需要使用S3兼容对象存储时，先按照“对象存储部署”章节补充配置，再执行相同的 `deploy.sh`命令。

### 使用指定镜像

需要使用某个测试或发布镜像时，先拉取交付说明中指定的镜像标签，再设置为原有部署脚本使用的本地标签：

```bash
IMAGE_TAG='<镜像标签>'
REGISTRY='swr.cn-north-4.myhuaweicloud.com/kunpeng-ai'

docker pull "${REGISTRY}/og_storage:${IMAGE_TAG}"
docker pull "${REGISTRY}/og_compute:${IMAGE_TAG}"
docker tag "${REGISTRY}/og_storage:${IMAGE_TAG}" og_storage:latest
docker tag "${REGISTRY}/og_compute:${IMAGE_TAG}" og_compute:latest

bash openGauss-branching-deploy/deploy.sh arm --skip-pull
```

镜像标签由交付版本决定，部署目录和命令保持一致。`--skip-pull` 会使用上面设置的本地镜像。
离线导入镜像时同样先设置为这两个本地标签，再执行原有部署命令。
主节点、只读节点以及存储服务应使用同一次发布中匹配的计算和存储镜像。

## 对象存储部署

对象存储模式用于将Pageserver的Layer和远端元数据，以及Safekeeper的完整WAL和Partial WAL写入S3兼容对象存储。MinIO、AWS S3以及提供S3兼容接口的对象存储均使用同一套配置方式。

启用前需要准备以下信息：

- S3 Endpoint；
- 已创建的Bucket；
- Bucket所在Region；
- Access Key和Secret Key；
- Pageserver与Safekeeper各自使用的Prefix。

部署脚本不会创建Bucket。Pageserver和Safekeeper可以使用同一个Bucket，但必须配置不同Prefix。

### 1. 修改部署配置

安装脚本执行完成后，编辑：

```bash
vi openGauss-branching-deploy/deploy.env
```

将远端存储模式改为 `s3`并填写连接参数：

```bash
REMOTE_STORAGE_MODE="s3"

S3_ENDPOINT="http://<S3_HOST_OR_IP>:<S3_PORT>"
S3_BUCKET="<S3_BUCKET_NAME>"
S3_REGION="<S3_REGION>"
PAGESERVER_S3_PREFIX="<PAGESERVER_S3_PREFIX>"
SAFEKEEPER_S3_PREFIX="<SAFEKEEPER_S3_PREFIX>"
S3_CONCURRENCY_LIMIT="20"

OBJECT_STORAGE_CREDENTIALS_FILE="./object_storage.env"
```

对于未单独配置Region的MinIO，通常使用 `us-east-1`。`S3_ENDPOINT`必须是Pageserver和Safekeeper容器能够访问的地址，不能填写只在宿主机自身有效的 `127.0.0.1`。

### 2. 创建凭证文件

复制凭证模板并填写实际值：

```bash
cp openGauss-branching-deploy/object_storage.env.example openGauss-branching-deploy/object_storage.env
vi openGauss-branching-deploy/object_storage.env

chmod 600 openGauss-branching-deploy/object_storage.env
```

使用临时凭证时，可以继续增加：

```bash
AWS_SESSION_TOKEN="<session-token>"
```

凭证不会写入Pageserver和Safekeeper配置文件。部署脚本会在启动和后续执行 `docker_local`管理命令时，将凭证传入相关容器。

### 3. 执行部署

ARM服务器执行：

```bash
bash openGauss-branching-deploy/deploy.sh arm --skip-pull
```

x86服务器执行：

```bash
bash openGauss-branching-deploy/deploy.sh x86 --skip-pull
```

部署脚本会检查对象存储必填参数、凭证文件、Prefix冲突以及Endpoint连通性，然后为默认和动态Pageserver生成S3配置，并为所有Safekeeper增加远端WAL配置。

### 4. 检查配置

部署完成后可以检查Pageserver实际使用的配置：

```bash
docker exec neon_poc-pageserver-1 \
  grep '^remote_storage' /data/.neon/pageserver/pageserver.toml
```

检查Safekeeper启动参数：

```bash
docker exec neon_poc-safekeeper-1 \
  sh -c 'tr "\0" " " < /proc/1/cmdline'
```

输出中应分别包含S3 Bucket、Endpoint和对应Prefix。随后产生业务数据，通过Pageserver和Safekeeper状态接口以及对象存储中的对象确认上传进度。

### 切回本地模式

保持或恢复以下配置即可使用原有LocalFs方式：

```bash
REMOTE_STORAGE_MODE="localfs"
```

本地模式不要求创建 `object_storage.env`，也不会为Safekeeper增加对象存储参数。

远端存储模式应在新集群初始化时确定。将已有LocalFs集群改成S3配置不会自动把原有对象迁移到S3；测试切换模式时应使用新的部署目录，或者设置 `INIT_FORCE="remove-all-contents"`重新初始化。

## 安装说明

`install.sh` 会下载以下内容：

- `deploy.sh`
- `deploy.env`
- `object_storage.env.example`
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
| `REMOTE_STORAGE_MODE` | 远端存储模式，支持 `localfs`和 `s3`，默认 `localfs` |
| `S3_ENDPOINT` | S3兼容对象存储访问地址 |
| `S3_BUCKET` | 已创建的Bucket名称 |
| `S3_REGION` | Bucket所在Region |
| `PAGESERVER_S3_PREFIX` | Pageserver对象路径前缀 |
| `SAFEKEEPER_S3_PREFIX` | Safekeeper对象路径前缀 |
| `OBJECT_STORAGE_CREDENTIALS_FILE` | S3访问凭证文件路径 |

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

更多详细命令见 [`docker-compose/README.md`](../../docker-compose/README.md)。

## 创建只读备机

主节点部署完成后，继续使用原有 `docker_local` 命令创建只读 endpoint。
交付文件应包含新版 `compute/shell/compute.sh` 和 `docker-compose/bin/docker_local.py`。
`install.sh` 已通过原有下载流程获取这两个文件；应从包含本功能的代码版本安装。
已有部署更新时应保留其 `deploy.env`、对象存储凭据和 `.neon` 数据目录。
宿主机的 `compute.sh` 会挂载到容器内，因此更新镜像时也要同步更新该脚本。

以下假设读写 endpoint 名为 `main`，在部署目录执行：

```bash
cd openGauss-branching-deploy

PRIMARY_INFO=$(./docker-compose/bin/docker_local endpoint status main)
TENANT_ID=$(printf '%s\n' "$PRIMARY_INFO" | jq -er '.tenant_id')
TIMELINE_ID=$(printf '%s\n' "$PRIMARY_INFO" | jq -er '.timeline_id')

./docker-compose/bin/docker_local endpoint create reader1 \
  --tenant-id "$TENANT_ID" --timeline-id "$TIMELINE_ID" \
  --hot-standby --pg-port 55440 --http-port 3082
./docker-compose/bin/docker_local endpoint start reader1
./docker-compose/bin/docker_local endpoint status reader1
```

备机与主机使用相同 tenant/timeline，持续接收主机提交的数据。
`--hot-standby` 后面不加 `true`。以 `endpoint status reader1` 返回的 `host_pg_port`
为实际数据库端口；如果指定端口被占用，工具会自动选择其他端口。

在安装了 `gsql` 的客户端，使用主机上已有的数据库账号和密码连接备机：

```bash
gsql -h 127.0.0.1 -p 55440 -U branch_merge -d postgres
```

以上地址用于部署宿主机；其他客户端填写部署宿主机地址。检查恢复状态：

```sql
SELECT pg_is_in_recovery();  -- t
SHOW transaction_read_only;  -- on
```

在主节点写入并提交数据后，在备机查询对应表验证同步。复制为异步。
停止、重新启动备机仍使用原有命令：

```bash
./docker-compose/bin/docker_local endpoint stop reader1
./docker-compose/bin/docker_local endpoint start reader1
```

功能原理、回放进度和回归检查见 [只读备机说明](../../docs/opengauss-read-replica.md)。

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
| `object_storage.env.example` | 对象存储凭证模板，不包含真实凭证 |
| `object_storage.env` | 用户创建的对象存储访问凭证，仅S3模式需要 |
| `compute/shell/compute.sh` | endpoint 启动脚本 |
| `compute/shell/storage_controller_db.sh` | storage controller DB 初始化脚本 |

## 说明

- `deploy.sh` 的命令示例统一为 `bash openGauss-branching-deploy/deploy.sh <arm|x86> --skip-pull`
- `install.sh` 仅负责下载，不负责启动服务
- 如果提示缺少部署文件，请先运行 `install.sh`

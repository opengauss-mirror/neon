# docker_local 使用说明

本目录通过 `bin/docker_local` 管理本地 Neon/openGauss PoC 环境。日常启停、扩缩容、创建 tenant/endpoint，都用这条命令。

默认在本目录执行：

```bash
cd neon_docker/docker-compose
```

## 前置条件

本机已有镜像：

- `og_storage:latest`：control plane、pageserver、safekeeper、storage_controller、endpoint_storage
- `og_compute:latest`：compute endpoint、`storage_controller_db`

可选环境变量（不设则用默认值）：

```bash
export OG_VERSION=V702
export NEON_IMAGE=og_storage:latest
export COMPUTE_IMAGE=og_compute:latest
export COMPOSE_PROJECT_NAME=neon_poc
```

## 一键启停

### 首次初始化并启动

```bash
# 初始化本地状态，拉起 docker_control_plane
# 已有 .neon 时用 --force=remove-all-contents 清空重建
bin/docker_local init --force=remove-all-contents

# 启动存储侧基础服务
# 默认：1 个 pageserver + 1 个 safekeeper + storage_controller 等
bin/docker_local start
```

需要一开始就起多个节点时，在 `init` 指定数量：

```bash
bin/docker_local init \
  --force=remove-all-contents \
  --num-pageservers 2 \
  --num-safekeepers 2

bin/docker_local start
```

`init` 会把 `num_pageservers` / `num_safekeepers` 写入 `.neon/control_plane/docker_local.json`。之后 `start` / `stop` 按这份配置处理动态节点。

### 日常启停

```bash
# 启动（control plane 已在时，直接拉起存储组件）
bin/docker_local start

# 停止 endpoint + 存储组件；默认不停 docker_control_plane
bin/docker_local stop

# 连 control plane 一起停
bin/docker_local stop --include-control-plane

```

### 创建可写 compute

存储起来后，再创建 tenant 和 endpoint：

```bash
bin/docker_local tenant create --set-default

bin/docker_local endpoint create main \
  --branch-name main

bin/docker_local endpoint start main
```

连接：

```bash

docker compose -p neon_poc   -f docker-compose.yml exec  main   gsql -d postgres -U cloud_admin -p 55433 -h 127.0.0.1
```

容器名规则：`${COMPOSE_PROJECT_NAME}-${endpoint_id}-1`。上面示例里 endpoint_id 是 `main`，项目名默认 `neon_poc`。

### 查看状态

```bash
bin/docker_local status
bin/docker_local ps
bin/docker_local ps --all
bin/docker_local logs pageserver -f
bin/docker_local logs safekeeper --tail 200
```

检查 pageserver 是否已注册到 storage_controller：

```bash
curl -s http://127.0.0.1:1234/control/v1/node | jq
```

## 常用操作

### pageserver

```bash
# 动态增加 pageserver2（node_id=1002）
bin/docker_local pageserver add 2

# 查看 / 启停 / 删除
bin/docker_local pageserver list
bin/docker_local pageserver stop 2
bin/docker_local pageserver start 2
bin/docker_local pageserver remove 2
bin/docker_local pageserver remove 2 --remove-data

# 往指定 node 填充 / 迁移 shard
bin/docker_local pageserver fill 1002
bin/docker_local pageserver migrate <tenant-shard-id> 1002
```

`pageserver add` 成功后会把 `num_pageservers` 至少提升到对应序号，保证全局 `bin/docker_local stop` 能停掉它。

默认静态 pageserver 是 ordinal `1` / node `1001`，端口默认 `9898`。动态节点从 `2` 开始，默认 HTTP 端口 `9897 + ordinal`。

### safekeeper

```bash
bin/docker_local safekeeper add 2
bin/docker_local safekeeper list
bin/docker_local safekeeper stop 2
bin/docker_local safekeeper start 2
bin/docker_local safekeeper remove 2

# 把已有 timeline 迁到新的 safekeeper 集合
bin/docker_local safekeeper migrate \
  --tenant-id <tenant_id> \
  --timeline-id <timeline_id> \
  --new-sk-set 1,2
```

`safekeeper add` 同样会更新 `num_safekeepers`，全局 `stop` 会带上这些动态节点。

### endpoint / compute

compute 全部通过 endpoint 动态创建，不再作为静态服务：

```bash
bin/docker_local endpoint create main --branch-name main --pg-port 55433 --http-port 3080
bin/docker_local endpoint start main
bin/docker_local endpoint stop main
bin/docker_local endpoint list
bin/docker_local endpoint status main
bin/docker_local endpoint destroy main
```

再建一个只读/分支 endpoint 示例：

```bash
bin/docker_local timeline branch --branch-name feature --ancestor-branch-name main
bin/docker_local endpoint create feature --branch-name feature --pg-port 55434 --http-port 3081
bin/docker_local endpoint start feature
```

### tenant / timeline

```bash
bin/docker_local tenant create --set-default
bin/docker_local tenant list
bin/docker_local tenant describe

bin/docker_local timeline list
bin/docker_local timeline branch --branch-name feature --ancestor-branch-name main
```

### 单服务启停

也可以只操作某一个基础服务：

```bash
bin/docker_local start --service pageserver
bin/docker_local stop --service safekeeper
bin/docker_local storage-controller restart
bin/docker_local storage-broker logs -f
```

## 配置与数据

| 路径 | 作用 |
| --- | --- |
| `bin/docker_local` | CLI 入口 |
| `bin/docker_local.py` | 实现 |
| `pageserver_config/` | 默认 pageserver（node 1001）配置 |
| `ext-src/` | 构建 `og_compute` 镜像用，不参与运行时启停 |
| `.neon/control_plane/docker_local.json` | CLI 本地配置（节点数量、镜像等） |
| `.neon/pageserver` / `.neon/pageserverN` | pageserver 数据 |
| `.neon/safekeeper` / `.neon/safekeeperN` | safekeeper 数据 |
| `.neon/<endpoint>/` | endpoint 数据 |
| `.neon/shared_remote_storage` | 共享 remote storage |
| `.neon/control_plane/overrides/` | 动态服务 override（pageserver / safekeeper / endpoint） |

动态服务不会手写进静态 compose 文件，而是由 `docker_local` / `docker_control_plane` 生成到 `.neon/control_plane/overrides/`。

## 清理与重建

只停容器、保留数据：

```bash
bin/docker_local stop --include-control-plane
```

清空状态并重新初始化：

```bash
bin/docker_local init --force=remove-all-contents
bin/docker_local start
```

若宿主机删不掉 `.neon`（权限被容器改过），可在本目录执行：

```bash
docker run --rm -u 0:0 -v "$PWD:/work" og_storage:latest \
  /bin/sh -ec 'rm -rf /work/.neon'
```

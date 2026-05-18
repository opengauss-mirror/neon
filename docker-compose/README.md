# neon_docker Guest Compose 使用说明

本文说明 `docker-compose.yml`、`neon:latest_opgs`、`compute-node-opengauss-v702` 的用途和启停方式。下面命令默认在 `neon_docker/docker-compose` 目录下执行。

## 镜像说明

### `neon:latest_opgs`

`neon:latest_opgs` 是 Neon 存储侧和控制侧服务镜像。在当前 compose 里用于运行：

- `storage_broker`
- `storage_controller`
- `pageserver`
- `safekeeper1`、`safekeeper2`、`safekeeper3`
- `endpoint_storage`
- 一次性初始化目录权限的 `data_permissions`

该镜像包含 Neon 存储相关二进制，以及 `/usr/local/${OG_VERSION}` 下的 openGauss 发行目录。`docker-compose.yml` 只使用这个镜像，不会构建它。

### `compute-node-opengauss-v702:latest`

`compute-node-opengauss-v702:latest` 是 openGauss 计算节点镜像，内置了 Neon 扩展、openGauss 兼容补丁、配置文件和启动脚本，用于运行：

- `compute1`
- `storage_controller_db`
- `compute_is_ready`


## 一键启停

### 启动全部组件

```bash
OG_VERSION=V702 \
NEON_IMAGE=neon:latest_opgs \
COMPUTE_IMAGE=compute-node-opengauss-v702:latest \
docker compose -f docker-compose.yml up -d
```

等待 compute 就绪：

```bash
docker compose -f docker-compose.yml logs -f compute_is_ready
```

看到下面日志说明 compute 已可连接：

```text
All computes are started
```

### 停止全部组件

停止并删除容器但保留数据目录：

```bash
docker compose -f docker-compose.yml down
```

停止容器：

```bash
docker compose -f docker-compose.yml stop
```

### 单独启停组件

重启单个组件：

```bash
docker compose -f docker-compose.yml restart compute1
docker compose -f docker-compose.yml restart pageserver
docker compose -f docker-compose.yml restart safekeeper1
```

修改 YAML 里的 command/env/config 后，建议强制重建对应容器：

```bash
docker compose -f docker-compose.yml up -d --force-recreate pageserver
docker compose -f docker-compose.yml up -d --force-recreate safekeeper1
docker compose -f docker-compose.yml up -d --force-recreate compute1
```

## SQL 写入验证

`compute_is_ready` 打印 `All computes are started` 后，可执行：

```bash
docker compose -f docker-compose.yml exec -T compute1 /bin/bash <<'EOF'
export GAUSSHOME=/usr/local/V702
export LD_LIBRARY_PATH=/usr/lib64:/usr/local/V702/lib
export LD_PRELOAD=/usr/lib64/liblapacke.so.3
export PATH=/usr/local/V702/bin:$PATH
/usr/local/V702/bin/gsql -d postgres -U cloud_admin -p 55433 -h localhost <<'SQL'
DROP TABLE IF EXISTS guest_insert_test;
CREATE TABLE guest_insert_test(id int primary key, note text);
INSERT INTO guest_insert_test VALUES (1, 'ok');
SELECT * FROM guest_insert_test ORDER BY id;
SQL
EOF
```

## 配置修改和生效方式

### pageserver

guest compose 中，pageserver 配置通过 bind mount 挂载：

```
docker-compose/pageserver_config/pageserver.toml
docker-compose/pageserver_config/identity.toml
```

这些文件被挂载到容器内的：

```
/data/.neon/pageserver/pageserver.toml
/data/.neon/pageserver/identity.toml
```

持久修改方式：直接编辑本地的配置文件，然后重启 pageserver：

```bash
docker compose -f docker-compose.yml restart pageserver
```

或者强制重建：

```bash
docker compose -f docker-compose.yml up -d --force-recreate pageserver
```

### safekeeper

safekeeper 主要通过 compose 里的环境变量和启动参数配置：

- `SAFEKEEPER_ID`
- `SAFEKEEPER_ADVERTISE_URL`
- `BROKER_ENDPOINT`
- `--listen-pg`
- `--listen-http`
- `-D /data/.neon/safekeeperN`

修改 `docker-compose.yml` 对应的 `safekeeperN` 服务后，重建该 safekeeper：

```bash
docker compose -f docker-compose.yml up -d --force-recreate safekeeper1
```

### compute

compute 的参数来源主要是 `compute_ctl` 读取的 JSON 配置：

```text
/var/db/gaussdb/configs/config.json
```

在 `docker-compose.yml` 中，这个文件通过 bind mount 从项目目录挂载：

```
compute/gaussdb/configs/config.json
```

持久修改方式：编辑 `compute/gaussdb/configs/config.json`，然后重启或重建 compute：

```bash
docker compose -f docker-compose.yml restart compute1
```

compute 启动时会生成实际的 openGauss 配置：

```text
/var/db/gaussdb/compute/postgresql.conf
```

所以 `shared_buffers` 这类参数不要只改生成后的 `postgresql.conf`，应改 `config.json`，然后重启 compute 进程。

`shared_buffers` 属于 postmaster 级参数，不能只靠 `SELECT pg_reload_conf()` 生效，必须重启 compute 进程。

### storage_controller_db

guest compose 用 `compute-node-opengauss-v702` 启动 `storage_controller_db`，入口脚本为：

```text
/shell/storage_controller_db.sh
```

数据保存在 `.neon/storage_controller_db` 目录。如果修改脚本，需要重建基础镜像或增加 bind mount，然后 recreate `storage_controller_db`。

## `data_permissions` 的作用

`data_permissions` 是一次性 helper 服务。它以 root 用户运行，在其他服务启动前创建所需数据目录并设置权限。

guest compose 用它初始化 `.neon` 目录下的各个子目录。

如果某些目录由容器 root 创建，导致宿主机用户无法删除，可以在 `neon_docker/docker-compose` 下用 root 容器清理：

```bash
docker run --rm -u 0:0 -v "$PWD:/work" neon:latest_opgs /bin/sh -ec 'rm -rf /work/.neon'
```

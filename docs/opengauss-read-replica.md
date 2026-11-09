# openGauss 共享存储只读备机

本功能对应持续跟随主节点的只读计算节点。一个 Neon tenant/timeline
保留一个读写计算节点，可以增加多个只读计算节点；它们共享 Pageserver。
不包含备机提升、主备切换或故障自动接管。

## 工作方式

- 主节点通过 walproposer 将 WAL 写入 Safekeeper。
- 备机启动线程使用 PostgreSQL 流复制协议从 Safekeeper 获取**已提交 WAL**，
  不参与 walproposer 选举。连接故障后轮换 `neon.safekeepers` 中的地址重试。
- openGauss 原生串行恢复负责事务状态、CSN、恢复冲突和缓存页回放。
  冷数据页按备机回放 LSN 从 Pageserver 获取，不读取主节点进程的数据目录。
  启动时从基础备份的 CLOG 初始化已提交事务的 CSN；当时尚未结束的事务，
  继续由后续提交或回滚 WAL 处理。
- shared_buffers、预取缓存和本地文件缓存正常工作；不再每次查询都丢弃缓存、
  向 Pageserver 请求最新页。备机有自身的缓存和恢复状态，不保存完整关系数据副本。
- 备机定期向 Safekeeper 报告回放 LSN，供其汇总并传递 Pageserver 的备机保留位置。
  长时间落后仍受 Neon 的 WAL/历史保留策略限制，不提供无限期保留保证。

分担的是 SQL 执行、连接、内存和缓存压力。多个计算节点仍共享存储服务；
如果瓶颈是 Pageserver 本身，增加只读计算节点不能自动解决该瓶颈。

## 镜像与运行时要求

主节点、备机和 Pageserver 的 WAL redo 运行时应使用匹配的 openGauss 内核和
Neon 扩展。必须同时更新内核、`neon.so`、`neon_walredo.so` 和 `compute_ctl`，
本地管理工具 `neon_local` 也应使用同一版本构建；
仅替换扩展不能提供所需的内核恢复钩子。主节点的新内核还会在空闲时定期记录
运行事务快照，使新备机无需等待业务写入即可进入可查询状态。

## Docker 启动

使用包含本功能的新版 `og_storage`、`og_compute` 镜像，并同步新版部署目录中的
`compute/shell/compute.sh` 和 `docker-compose/bin/docker_local.py`。
Docker 的启动脚本会跳过备机上的用户创建、角色修改和主机写入 LSN 检查；
数据库用户和密码在主节点创建，再由 WAL 同步至备机。
旧版 `object-storage-v1` 镜像早于本功能实现，需要先重新构建镜像并更新主节点和
存储侧运行时。部署继续使用现有的 `install.sh` / `deploy.sh` 流程，
镜像选择和创建备机的操作见 [Docker 部署指南](../deployment/deploy_docker/README.md)。

交付时应同步更新上述两个宿主机脚本：部署会把宿主机的 `compute.sh` 挂载到容器内，
因此只更换镜像不会替换旧的启动脚本。

以下命令在**现有部署**的 `docker-compose` 目录执行，假设读写 endpoint 名为 `main`。
沿用该部署的镜像、Compose 项目名和认证环境配置。

```bash
# 如主节点名称不是 main，将此处 main 替换为实际名称。
PRIMARY_INFO=$(bin/docker_local endpoint status main)
TENANT_ID=$(printf '%s\n' "$PRIMARY_INFO" | jq -er '.tenant_id')
TIMELINE_ID=$(printf '%s\n' "$PRIMARY_INFO" | jq -er '.timeline_id')

bin/docker_local endpoint create reader1 \
  --tenant-id "$TENANT_ID" \
  --timeline-id "$TIMELINE_ID" \
  --hot-standby \
  --pg-port 55440 --http-port 3082

bin/docker_local endpoint start reader1
bin/docker_local endpoint status reader1
bin/docker_local ps
```

`docker_local` 的 `--hot-standby` 是布尔开关，后面不加 `true`。
`reader1` 与主节点使用相同 tenant/timeline，因此会持续读取主节点的新提交；
不需要创建新分支，也不要指定 `--lsn` 或 `--static-lsn`。
工具会自动生成 `spec.mode="Replica"`、存储连接和独立的数据目录。
`hot_standby=on` 本身并不能把一个主节点变成备机。

容器内数据库端口仍为 `55433`，宿主机请求端口是 `55440`。
如果端口被占用，CLI 会自动选择其他端口，以 `endpoint status reader1` 中的
`host_pg_port` 为准。在安装了 `gsql` 的客户端，使用主节点已有的账号和密码连接：

```bash
gsql -h 127.0.0.1 -p 55440 -U branch_merge -d postgres
```

```sql
SELECT pg_is_in_recovery();                    -- t
SHOW transaction_read_only;                    -- on
SELECT * FROM pg_last_xlog_replay_location();   -- term、location
```

停止、重新启动备机：

```bash
bin/docker_local endpoint stop reader1
bin/docker_local endpoint start reader1
```

增加第二台备机时使用不同 endpoint 名称和宿主机端口，tenant/timeline 仍与主节点相同。
应用将读请求发到对应的宿主机数据库端口。

## 宿主机直接启动

在已初始化的本地 Neon 环境中，主节点按原有方式启动，再创建同分支备机：

```bash
export NEON_REPO_DIR=/absolute/path/to/neon-repo

neon_local endpoint create reader1 \
  --branch-name main --hot-standby true \
  --pg-port 55440 --external-http-port 55442 --internal-http-port 55443
neon_local endpoint start reader1
```

增加备机时使用不同 endpoint 名称和端口。应用自行将读请求发到备机端口；
本改动不增加 SQL 路由或负载均衡服务。

`compute_ctl` 会生成 `recovery.conf` 的 `standby_mode='on'`，并为 Replica
模式设置 `hot_standby=on`、`recovery_max_workers=1`、`recovery_parse_workers=1`。
备机 WAL 来源取自 `neon.safekeepers`，不使用 openGauss 原生 HA walreceiver 协议。
手工配置时还需保留现有的 `neon.tenant_id`、`neon.timeline_id`、
`neon.pageserver_connstring` 等共享存储设置，并设置 `neon.compute_mode=replica`。

备机正常停止、重启仍使用 `neon_local endpoint stop/start reader1`。
启动时重新从 Pageserver 获取基础状态，随后继续跟随 WAL。

## 检查

在备机执行：

```sql
SELECT pg_is_in_recovery();                    -- 必须为 true
SHOW transaction_read_only;                    -- on
SELECT * FROM pg_last_xlog_replay_location();   -- openGauss 返回 term、location 两列
```

在主节点执行 `SELECT pg_current_xlog_location()`，与备机返回的 location 比较，
可以观察追赶进度。复制是异步的，主节点提交成功后备机可能暂时读不到新数据；
要求读到指定提交时，应先等待备机回放到对应 LSN。

回归脚本需要 Python 3 和 psycopg2，在专用测试数据库上执行：

```bash
python3 test_runner/regress_og/read_replica/check_read_replica.py \
  --primary 'host=127.0.0.1 port=55430 user=cloud_admin dbname=postgres' \
  --replica 'host=127.0.0.1 port=55440 user=cloud_admin dbname=postgres' \
  --replica 'host=127.0.0.1 port=55450 user=cloud_admin dbname=postgres'
```

脚本创建唯一命名的测试表并清理，检查多页数据、缓存页和索引的持续更新、
未提交数据隔离、回滚、可重复读、DDL 和写入拒绝。

在本地测试环境可追加 `--restart-endpoint reader1 --neon-local /path/to/neon_local`，
同时设置 `NEON_REPO_DIR`。此选项会停止并重启第一个 `--replica` 对应的 endpoint，
验证主节点存在未提交事务时的备机启动、已提交数据可见性和后续提交的快照隔离。

## 当前范围

备机持续保持恢复状态，写入由内核拒绝；修改会话只读默认值不能将其变成主节点。
当前 WAL 输入仅接入串行恢复，请保留上述恢复线程配置。
备机查询可能因原生恢复冲突而被取消，例如主节点 VACUUM 清理其仍需的旧版本。
本功能没有实现备机提升或透明读后写一致性，也不承诺主节点故障后自动接管。

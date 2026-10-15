
# 按指定时间创建恢复分支（PITR）

三个 CLI 都提供 `pitr BRANCH --parent PARENT --time RFC3339`。命令自动查询目标时间的 LSN，并从该 LSN 创建新分支。父库的数据和连接入口保持原状。

## 命令

在源码根目录执行：

```bash
./target/release/neon_local pitr restore-demo \
  --parent main --time '2026-10-12T10:00:00+08:00'

./docker-compose/bin/docker_local pitr restore-demo \
  --parent main --time '2026-10-12T10:00:00+08:00'

./deployment/deploy_k8s/bin/k8s_local pitr restore-demo \
  --namespace branch --parent compute-main \
  --time '2026-10-12T10:00:00+08:00'
```

这些时间只是格式示例，必须换成当前父 timeline 保留范围内的实际时间。
K8s 资源池分支可追加 `--from-pool opgs-pool`。K8s 保留现有 `--timeout`、`--og-version` 等环境选项。
neon_local 沿用 `NEON_REPO_DIR`；docker_local 沿用 control-plane URL 和默认 tenant 配置。

## 时间和查询

必须带时区。支持 `Z` 和 `+08:00` 等 RFC3339 时区，CLI 自动转换为 UTC `Z`，解决 pageserver 不接受偏移时区格式的问题。不要省略时区。支持最多 9 位小数秒。

CLI 自动解析父分支的 tenant/timeline，定位 shard 0 查询时间对应的 LSN。
K8s 在集群内部请求 pageserver，不需要手动 port-forward。
Docker 使用对应 pageserver 的映射端口；启用 pageserver JWT 时可设置 `PAGESERVER_JWT_TOKEN`。
neon_local 使用现有 pageserver 配置和认证。

只有 `kind=present` 可以继续：
- `past`：早于可查询范围，可能已被 GC 清理或早于父 timeline 的分支起点。
- `future`：晚于当前可用的提交历史。
- `nodata`：缺少可用的事务提交时间记录。

这些状态都会失败退出，不会使用最新 LSN 代替目标恢复点。时间映射沿用 pageserver 的事务提交时间算法，多个并发提交时仍受该算法语义限制。

## 预检查和创建

追加 `--dry-run` 只查询时间和 LSN、打印计划，不创建 timeline/compute、不申请 LSN 租约。
正式执行先在每个分片申请显式 LSN 租约（默认 10 分钟），检查租约有效后复用现有分支创建流程。
历史已被清理、租约未授予或分支名称重复时，命令失败退出。
创建失败时保留底层错误；特别是 K8s rollout 失败可能已经创建 timeline 或 compute，重试前应先检查现有资源。

neon_local / docker_local 创建 timeline；需要 compute 时继续执行已有命令：

```bash
./target/release/neon_local endpoint create restore-demo --branch-name restore-demo
./target/release/neon_local endpoint start restore-demo

./docker-compose/bin/docker_local endpoint create restore-demo --branch-name restore-demo
./docker-compose/bin/docker_local endpoint start restore-demo
```

K8s 会创建并启动 compute，可直接查看或连接：

```bash
./deployment/deploy_k8s/bin/k8s_local branch status restore-demo --namespace branch
./deployment/deploy_k8s/bin/k8s_local branch connect restore-demo --namespace branch
```


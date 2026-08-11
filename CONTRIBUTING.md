# 贡献指南

请遵循常规的软件工程实践：为重要改动补充测试，必要时添加说明性注释，并尽量保持代码风格与当前仓库一致。

Rust 代码提交前建议使用 `cargo fmt` 和 `cargo clippy` 整理格式与静态检查。修改已有代码时，尽量让相关代码比修改前更清晰、更容易维护。

## Pre-commit hook

当前仓提供了一个示例提交前检查脚本：`pre-commit.py`。启用方式如下：

```bash
make setup-pre-commit-hook
```

该命令会把 `.git/hooks/pre-commit` 链接到仓库里的 `pre-commit.py`。Git hook 是本地配置，不会随仓库自动启用；每个本地 clone 如需提交前检查，都需要执行一次上述命令。

每次 `git commit` 前，hook 会检查本次已暂存的文件，并跳过第三方或生成目录，例如 `vendor/`、`target/`、`build/`、`og_install/`。当前检查内容包括：

- 暂存了 Rust 文件时，运行 `cargo fmt --check`。
- 暂存了 Python 文件时，运行仓库已有的 Python 检查，详见 [obligatory checks](/docs/sourcetree.md#obligatory-checks)。

提交 Python 改动前，请先运行 `./scripts/pysync` 安装所需工具依赖：

```bash
./scripts/pysync
```

如果默认 PyPI 源下载较慢，可以临时指定镜像源：

```bash
PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple \
PIP_TRUSTED_HOST=mirrors.aliyun.com \
./scripts/pysync
```

如需手动执行自动格式化，可运行：

```bash
make fmt
```

日常使用流程如下：

```bash
make setup-pre-commit-hook   # 每个本地 clone 只需要执行一次

# 平时开发完成后
git add ...
git commit -m "..."

# 如果 pre-commit 因格式问题失败，先自动修复，再重新暂存和提交
make fmt
git add ...
git commit -m "..."
```

仓库还提供了 `./run_clippy.sh` 用于对整个项目运行 `cargo clippy`，以及 `./scripts/reformat` 用于执行全量格式化工具。

如需临时跳过提交前检查，可以使用：

```bash
git commit --no-verify
```

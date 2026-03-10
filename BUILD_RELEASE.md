# Neon Release 编译指南

## 快速编译

```bash
#配置openGauss第三方库路径
export OPENGAUSS_BINARYLIBS_DIR="xxx"
cd neon_branch_dev

# 完整编译
BUILD_TYPE=release make -j$(nproc)

# 初始化并配置
./target/release/neon_local init
make configure-release

# 启动服务
./target/release/neon_local start
```

## 验证编译结果

```bash
# 检查可执行文件
ls -lh target/release/neon_local
ls -lh target/release/pageserver
ls -lh target/release/safekeeper
ls -lh og_install/V702/lib/postgresql/neon.so

# 检查配置
grep neon_distrib_dir .neon/config
# 应显示：neon_distrib_dir = ".../target/release"
```

## 清理重编

```bash
# 停止服务
./target/release/neon_local stop

# 完全清理
make distclean
rm -rf .neon

# 重新编译
BUILD_TYPE=release make -j$(nproc)
./target/release/neon_local init
make configure-release
```

## Debug/Release 切换

```bash
# 切换到 release
make configure-release
./target/release/neon_local restart

# 切换到 debug
make configure-debug
./target/debug/neon_local restart
```

## 常用命令

```bash
# 查看配置
make show-config

# 只编译 Rust 组件
BUILD_TYPE=release make neon -j$(nproc)

# 只编译扩展
BUILD_TYPE=release make neon-pg-ext

# 查看服务状态
./target/release/neon_local status
```

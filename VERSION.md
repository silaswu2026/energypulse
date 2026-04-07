# EnergyPulse 版本管理制度

## 分支模型 (Git Flow简化版)

```
main      → 生产环境 (稳定版本，只接受合并)
develop   → 测试环境 (开发分支，功能集成)
feature/* → 功能分支 (从develop创建，开发完成后合并回develop)
```

## 工作流程

### 1. 开发新功能

```bash
# 从develop创建功能分支
git checkout develop
git pull origin develop
git checkout -b feature/新功能名

# 开发代码...
git add .
git commit -m "feat: 新增功能描述"
git push origin feature/新功能名

# 创建Pull Request合并到develop
# 在GitHub上操作
```

### 2. 部署到测试环境

```bash
# 确保develop分支是最新的
git checkout develop
git pull origin develop

# 运行部署脚本
./scripts/deploy_test.sh

# 访问测试环境验证
# http://服务器IP:8080
```

### 3. 发布到生产环境

```bash
# 1. 确保develop已通过测试
# 2. 创建Pull Request: develop → main
# 3. 代码审查后合并到main

# 4. 部署到生产
./scripts/deploy_prod.sh

# 5. 访问生产环境
# http://服务器IP
```

## 环境对照

| 环境 | 分支 | 端口 | 用途 | 数据库 |
|------|------|------|------|--------|
| 测试 | develop | 8080 | 预览开发功能 | energypulse_test |
| 生产 | main | 80 | 正式对外服务 | energypulse |

## 版本号规则

格式: `v主版本.次版本.修订号`

- 主版本: 重大架构变更 (如v1→v2)
- 次版本: 新功能发布 (如v1.1→v1.2)
- 修订号: bug修复 (如v1.1.0→v1.1.1)

示例:
- v1.0.0: 初始发布
- v1.1.0: 新增周报功能
- v1.1.1: 修复数据采集bug

## 回滚机制

如果生产环境出现问题:

```bash
# 查看历史标签
git tag | grep backup

# 回滚到指定版本
git checkout backup-20240407-120000
./scripts/deploy_prod.sh
```

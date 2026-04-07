#!/bin/bash
set -e
cd /opt/energypulse

# 获取新版本号
NEW_VERSION=$(./scripts/version_manager.sh increment-beta)
echo "=== 部署Beta版本: $NEW_VERSION ==="

# 版本号转换为docker project名 (beta-1.0.1 -> beta-1-0-1)
PROJECT_NAME=$(echo $NEW_VERSION | sed "s/\./-/g")

# 拉取最新代码
git checkout develop
git pull origin develop

# 使用新版本号部署 (不停止旧版本，实现多版本并存)
BETA_VERSION=$NEW_VERSION BETA_PORT=6666 docker-compose -f docker-compose.beta.yml -p $PROJECT_NAME up --build -d

IP=$(hostname -I | awk "{print \$1}")
echo ""
echo "✅ Beta版本 $NEW_VERSION 已部署"
echo "   访问地址: http://${IP}:6666"
echo "   版本标识: $PROJECT_NAME"
echo ""
echo "📋 历史版本列表:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep ep_web_beta || echo "   暂无其他版本"

#!/bin/bash
set -e
cd /opt/energypulse

echo "=== 部署Release版本 ==="
echo "⚠️  危险操作!"
echo ""
echo "当前Beta版本: $(./scripts/version_manager.sh get-beta)"
echo "当前Prod版本: $(./scripts/version_manager.sh get-prod)"
echo ""
read -p "确认将Beta晋升为Release? (输入release确认): " confirm

if [ "$confirm" != "release" ]; then
    echo "已取消"
    exit 1
fi

# 晋升版本号
NEW_VERSION=$(./scripts/version_manager.sh promote)
echo "新版本号: v$NEW_VERSION"

# 创建Git标签
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"
git push origin "v$NEW_VERSION"

# 切换到main分支
git checkout main
git merge develop -m "Merge develop for v$NEW_VERSION"
git push origin main

# 停止旧生产容器
docker-compose -f docker-compose.prod.yml down 2>/dev/null || true

# 部署新版本 (端口8888)
PROD_PORT=8888 docker-compose -f docker-compose.prod.yml -p prod up --build -d

IP=$(hostname -I | awk "{print \$1}")
echo ""
echo "✅ Release v$NEW_VERSION 已部署"
echo "   生产地址: http://${IP}:8888"

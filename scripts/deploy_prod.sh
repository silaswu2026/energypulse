#!/bin/bash
set -e
cd /opt/energypulse

echo "=== 部署到生产环境 ==="
echo "⚠️  危险操作: 请确认测试已通过!"
read -p "输入 deploy 确认部署: " confirm

if [ "$confirm" != "deploy" ]; then
    echo "已取消部署"
    exit 1
fi

# 备份当前版本
tag="backup-$(date +%Y%m%d-%H%M%S)"
git tag "$tag"
echo "✅ 已创建备份标签: $tag"

# 切换到main分支并拉取最新
git checkout main
git pull origin main

# 停止并重新部署
docker-compose -f docker-compose.prod.yml down
docker-compose -f docker-compose.prod.yml up --build -d

IP=$(hostname -I | awk "{print \$1}")
echo ""
echo "✅ 生产环境已部署"
echo "   Web地址: http://${IP}"
echo "   日报地址: http://${IP}/report/daily/latest"

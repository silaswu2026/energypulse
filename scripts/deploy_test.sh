#!/bin/bash
set -e
cd /opt/energypulse

echo "=== 部署到测试环境 ==="

# 拉取最新代码
git checkout develop
git pull origin develop

# 停止并重新部署
docker-compose -f docker-compose.test.yml down 2>/dev/null || true
docker-compose -f docker-compose.test.yml up --build -d

IP=$(hostname -I | awk "{print \$1}")
echo ""
echo "✅ 测试环境已部署"
echo "   Web地址: http://${IP}:8080"
echo "   日报地址: http://${IP}:8080/report/daily/latest"

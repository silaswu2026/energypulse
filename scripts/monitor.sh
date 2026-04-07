#!/bin/bash
echo "=== $(date) 系统状态监控 ==="
docker ps --format "table {{.Names}}\t{{.Status}}"
echo "=================================="

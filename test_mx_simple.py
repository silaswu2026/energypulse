#!/usr/bin/env python3
import sys
sys.path.insert(0, "/app/skills")

from mx_adapter import query_cn_stock

print("Testing mx_adapter...")
result = query_cn_stock("宁德时代")
if result:
    print("Symbol:", result.get("symbol"))
    print("Close:", result.get("close"))
    print("Change:", result.get("change_pct"))
else:
    print("Failed to get data")

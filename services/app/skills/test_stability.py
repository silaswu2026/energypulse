"""
妙想Skills稳定性测试
批量查询A股能源板块股票，测试成功率和响应时间
"""

import time
import json
import logging
from datetime import datetime
from mx_adapter import query_cn_stock, collect_cn_energy_stocks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mx_test")

# 测试股票列表 - 能源板块核心股
TEST_STOCKS = [
    "中国神华", "陕西煤业", "兖矿能源", "中煤能源", "山西焦煤",
    "潞安环能", "平煤股份", "晋控煤业", "华阳股份", "美锦能源",
    "中国海油", "中国石油", "中国石化", "广汇能源", "九丰能源",
    "长江电力", "中国核电", "华能国际", "三峡能源", "国投电力"
]

# 新能源
NEW_ENERGY_STOCKS = [
    "宁德时代", "比亚迪", "隆基绿能", "通威股份", "阳光电源",
    "亿纬锂能", "恩捷股份", "天赐材料"
]


def test_single_query(stock_name: str, timeout: int = 30) -> dict:
    """测试单只股票查询"""
    start_time = time.time()
    
    try:
        result = query_cn_stock(stock_name)
        elapsed = time.time() - start_time
        
        if result and result.get("close"):
            return {
                "stock": stock_name,
                "success": True,
                "price": result.get("close"),
                "change_pct": result.get("change_pct"),
                "elapsed": round(elapsed, 2),
                "error": None
            }
        else:
            return {
                "stock": stock_name,
                "success": False,
                "price": None,
                "change_pct": None,
                "elapsed": round(elapsed, 2),
                "error": "No data returned"
            }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "stock": stock_name,
            "success": False,
            "price": None,
            "change_pct": None,
            "elapsed": round(elapsed, 2),
            "error": str(e)
        }


def run_stability_test(stock_list: list, batch_name: str):
    """运行稳定性测试"""
    print("\n" + =*60)
    print(f"🧪 稳定性测试: {batch_name}")
    print(f"{=*60}")
    print(f"测试时间: {datetime.now().strftime(%Y-%m-%d %H:%M:%S)}")
    print(f"股票数量: {len(stock_list)}")
    print(=*60 + \n)
    
    results = []
    success_count = 0
    total_time = 0
    
    for i, stock in enumerate(stock_list, 1):
        print(f"[{i}/{len(stock_list)}] 查询 {stock}...", end=" ", flush=True)
        
        result = test_single_query(stock)
        results.append(result)
        
        if result["success"]:
            success_count += 1
            total_time += result["elapsed"]
            change_str = f"{result[change_pct]:+.2f}%" if result[change_pct] else "N/A"
            print(f"✅ {result[price]} ({change_str}) - {result[elapsed]}s")
        else:
            print(f"❌ 失败: {result[error][:50]}")
        
        # 间隔0.5秒避免请求过快
        time.sleep(0.5)
    
    # 统计结果
    success_rate = (success_count / len(stock_list)) * 100
    avg_time = total_time / success_count if success_count > 0 else 0
    
    print("\n" + =*60)
    print("📊 测试结果统计")
    print(f"{=*60}")
    print(f"总股票数:    {len(stock_list)}")
    print(f"成功:        {success_count}")
    print(f"失败:        {len(stock_list) - success_count}")
    print(f"成功率:      {success_rate:.1f}%")
    print(f"平均响应:    {avg_time:.2f}s")
    print(f"{=*60}")
    
    return {
        "batch_name": batch_name,
        "total": len(stock_list),
        "success": success_count,
        "failed": len(stock_list) - success_count,
        "success_rate": round(success_rate, 1),
        "avg_time": round(avg_time, 2),
        "results": results
    }


def test_collect_function():
    """测试批量采集函数"""
    print("\n" + =*60)
    print("🧪 测试批量采集函数: collect_cn_energy_stocks()")
    print(f"{=*60}")
    
    start_time = time.time()
    try:
        stocks = collect_cn_energy_stocks()
        elapsed = time.time() - start_time
        
        print(f"\n✅ 采集完成!")
        print(f"   获取股票数: {len(stocks)}")
        print(f"   耗时: {elapsed:.2f}s")
        
        if stocks:
            print(f"\n前3只股票数据:")
            for s in stocks[:3]:
                change = s.get(change_pct, N/A)
                print(f"   {s[symbol]}: {s[close]} ({change}%)")
        
        return {
            "success": True,
            "count": len(stocks),
            "elapsed": round(elapsed, 2)
        }
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ 采集失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "elapsed": round(elapsed, 2)
        }


def main():
    """主测试函数"""
    print("\n" + "="*60)
    print("   妙想Skills稳定性测试套件")
    print("="*60)
    
    all_results = {}
    
    # 测试1: 传统能源（煤炭+油气+电力）
    all_results["传统能源"] = run_stability_test(TEST_STOCKS, "传统能源板块")
    
    # 测试2: 新能源
    all_results["新能源"] = run_stability_test(NEW_ENERGY_STOCKS, "新能源板块")
    
    # 测试3: 批量采集函数
    all_results["批量采集"] = test_collect_function()
    
    # 总结
    print("\n" + =*60)
    print("   📋 测试总结")
    print(f"{=*60}")
    
    total_stocks = all_results["传统能源"]["total"] + all_results["新能源"]["total"]
    total_success = all_results["传统能源"]["success"] + all_results["新能源"]["success"]
    overall_rate = (total_success / total_stocks) * 100
    
    print(f"总测试股票: {total_stocks}")
    print(f"总成功:     {total_success}")
    print(f"总成功率:   {overall_rate:.1f}%")
    print(f"\n判定标准:")
    print(f"  ✅ 优秀: 成功率 >= 95%")
    print(f"  ⚠️  可用: 成功率 85%-94%")
    print(f"  ❌ 不可用: 成功率 < 85%")
    print(f"\n结果判定: ", end="")
    
    if overall_rate >= 95:
        print("✅ 优秀 - 可以集成到生产环境")
    elif overall_rate >= 85:
        print("⚠️  可用 - 建议增加错误处理")
    else:
        print("❌ 不可用 - 需要排查问题")
    
    print(=*60 + \n)
    
    # 保存详细结果
    result_file = f"/tmp/mx_test_result_{datetime.now().strftime(%Y%m%d_%H%M%S)}.json"
    with open(result_file, "w") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"详细结果已保存: {result_file}")


if __name__ == "__main__":
    main()

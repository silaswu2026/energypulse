"""
EnergyPulse 采集器基类
所有采集器继承此基类，自动获得：
- 主备双通道切换
- 重试机制
- 数据验真
- 限流控制
- 日志记录
- 微信告警
"""

import time
import hashlib
import json
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Optional

from database import get_db
from config_loader import APIConfig

logger = logging.getLogger("energypulse.collector")


class BaseCollector(ABC):
    """
    采集器基类。

    子类需要实现:
    - collect_primary() -> list[dict]   主通道采集
    - collect_fallback() -> list[dict]  备用通道采集（可选）
    - validate(records) -> list[dict]   数据验真（可选，默认通过）

    使用:
        collector = MyCollector("my_source")
        collector.run()
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.db = get_db()
        self.config = APIConfig.get()
        self.retry_count = 2
        self.retry_delay = 15  # seconds
        self.timeout = 30      # request timeout
        self._session = None

    @property
    def session(self) -> requests.Session:
        """复用HTTP Session"""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "EnergyPulse/1.0 DataCollector",
                "Accept": "application/json",
            })
        return self._session

    # ── 子类必须实现 ──

    @abstractmethod
    def collect_primary(self) -> list[dict]:
        """主通道采集，返回标准化数据列表"""
        ...

    def collect_fallback(self) -> list[dict]:
        """备用通道采集，子类可选覆盖"""
        raise NotImplementedError(f"[{self.source_id}] 无备用通道")

    def validate(self, records: list[dict]) -> list[dict]:
        """数据验真，子类可覆盖增加自定义规则"""
        valid = []
        for r in records:
            if "value" in r and r["value"] is not None:
                try:
                    r["value"] = float(r["value"])
                    valid.append(r)
                except (ValueError, TypeError):
                    logger.warning(f"[{self.source_id}] 非数值记录: {r}")
            else:
                valid.append(r)
        return valid

    @abstractmethod
    def store(self, records: list[dict]):
        """存储数据到数据库，子类实现具体表写入"""
        ...

    # ── 核心执行逻辑 ──

    def run(self) -> bool:
        """
        完整采集流程：主通道 → 备用通道 → 降级。
        返回是否成功。
        """
        start = time.time()

        # 1. 尝试主通道
        for attempt in range(self.retry_count + 1):
            try:
                raw = self.collect_primary()
                valid = self.validate(raw)
                self.store(valid)
                duration = int((time.time() - start) * 1000)
                self._log("primary", "success", len(valid), duration_ms=duration)
                logger.info(f"[{self.source_id}] 主通道成功，{len(valid)}条记录，耗时{duration}ms")
                return True
            except Exception as e:
                if attempt < self.retry_count:
                    logger.warning(
                        f"[{self.source_id}] 主通道第{attempt+1}次失败: {e}，"
                        f"{self.retry_delay}s后重试"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"[{self.source_id}] 主通道全部{self.retry_count+1}次失败: {e}")

        # 2. 尝试备用通道
        try:
            raw = self.collect_fallback()
            valid = self.validate(raw)
            self.store(valid)
            duration = int((time.time() - start) * 1000)
            self._log("fallback", "success", len(valid), duration_ms=duration)
            logger.info(f"[{self.source_id}] 备用通道成功，{len(valid)}条记录")
            self._alert(f"⚠️ [{self.source_id}] 主通道失败，已切换到备用通道", level="warn")
            return True
        except NotImplementedError:
            logger.warning(f"[{self.source_id}] 无备用通道")
        except Exception as e:
            logger.error(f"[{self.source_id}] 备用通道也失败: {e}")

        # 3. 最终降级
        degraded = self._get_last_valid()
        if degraded:
            self.store(degraded)
            duration = int((time.time() - start) * 1000)
            self._log("degraded", "degraded", len(degraded), duration_ms=duration)
            logger.warning(f"[{self.source_id}] AB通道均失败，使用降级数据({len(degraded)}条)")
            self._alert(
                f"🔴 [{self.source_id}] 主备通道均失败，已使用前值降级",
                level="critical"
            )
            return False

        # 4. 完全失败
        duration = int((time.time() - start) * 1000)
        self._log("none", "failed", 0, error_message="全部通道失败且无历史数据",
                  duration_ms=duration)
        self._alert(
            f"🔴🔴 [{self.source_id}] 完全失败，该数据项缺失",
            level="critical"
        )
        return False

    # ── 辅助方法 ──

    def _get_last_valid(self) -> list[dict]:
        """获取最近一次有效数据作为降级值（子类可覆盖）"""
        return []

    def _log(self, channel: str, status: str, count: int = 0,
             error_message: str = None, duration_ms: int = 0):
        """写入采集日志表"""
        try:
            self.db.insert_log(
                collector_id=self.source_id,
                channel=channel,
                status=status,
                records_count=count,
                error_message=error_message,
                duration_ms=duration_ms
            )
        except Exception as e:
            logger.error(f"写入日志失败: {e}")

    def _alert(self, message: str, level: str = "warn"):
        """发送微信告警（在pusher模块中实现，这里仅记录）"""
        logger.warning(f"[ALERT:{level}] {message}")
        # TODO: 实际推送在pusher模块实现

    def api_get(self, api_name: str, path: str = "", params: dict = None,
                use_proxy: bool = None) -> dict:
        """
        统一API GET请求。
        自动处理：限流检查、代理包装、超时、错误码。
        """
        self.config.check_rate_limit(api_name)

        url = self.config.build_url(api_name, path, params)
        headers = {}

        # 特殊header处理
        key = self.config.get_key(api_name)
        if api_name == "tiingo":
            headers["Authorization"] = f"Token {key}"
        elif api_name == "fmp":
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}apikey={key}"
        elif api_name == "fred":
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}api_key={key}&file_type=json"

        resp = self.session.get(url, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def api_post(self, api_name: str, path: str = "", json_body: dict = None) -> dict:
        """统一API POST请求"""
        self.config.check_rate_limit(api_name)

        base_url = self.config.get_base_url(api_name)
        url = f"{base_url}{path}"

        if self.config.needs_proxy(api_name):
            url = self.config.proxy_url(url)

        resp = self.session.post(url, json=json_body, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def make_hash(data) -> str:
        """生成数据hash用于去重"""
        return hashlib.md5(
            json.dumps(data, sort_keys=True, default=str).encode()
        ).hexdigest()

    @staticmethod
    def today_str() -> str:
        return date.today().isoformat()

    @staticmethod
    def now_str() -> str:
        return datetime.utcnow().isoformat()

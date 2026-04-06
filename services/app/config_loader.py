"""
EnergyPulse API配置加载器
从YAML文件加载API密钥和数据源映射。
支持热重载、代理包装、限流控制。
"""

import os
import time
import yaml
import urllib.parse
import threading
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("energypulse.config")

CONFIG_DIR = os.environ.get("EP_CONFIG_DIR", "/app/config")


@dataclass
class APIEndpoint:
    """单个API端点配置"""
    name: str
    key: str
    base_url: str
    rate_limit: int = 60  # 默认每分钟60次
    _call_times: list = field(default_factory=list, repr=False)

    def check_rate_limit(self):
        """简单滑动窗口限流"""
        now = time.time()
        self._call_times = [t for t in self._call_times if now - t < 60]
        if len(self._call_times) >= self.rate_limit:
            wait = 60 - (now - self._call_times[0])
            logger.warning(f"[{self.name}] 限流中，等待 {wait:.1f}s")
            time.sleep(max(wait, 1))
        self._call_times.append(time.time())


class APIConfig:
    """
    统一API配置管理器（单例）。

    使用:
        config = APIConfig.get()
        key = config.get_key("fmp")
        url = config.get_base_url("fmp")
        config.check_rate_limit("fmp")
    """

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> "APIConfig":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._keys: dict = {}
        self._endpoints: dict[str, APIEndpoint] = {}
        self._proxy_config: dict = {}
        self._last_loaded: float = 0
        self.reload()

    def reload(self):
        """加载或重新加载配置文件"""
        keys_path = Path(CONFIG_DIR) / "api_keys.yaml"
        if not keys_path.exists():
            # 尝试example文件
            example_path = Path(CONFIG_DIR) / "api_keys.yaml.example"
            if example_path.exists():
                logger.warning("api_keys.yaml 不存在，使用 example 文件")
                keys_path = example_path
            else:
                raise FileNotFoundError(f"找不到API配置文件: {keys_path}")

        with open(keys_path, "r") as f:
            self._keys = yaml.safe_load(f) or {}

        # 构建端点对象
        self._endpoints = {}
        for name, cfg in self._keys.items():
            if isinstance(cfg, dict) and ("key" in cfg or "token" in cfg):
                self._endpoints[name] = APIEndpoint(
                    name=name,
                    key=cfg.get("key", cfg.get("token", "")),
                    base_url=cfg.get("base_url", ""),
                    rate_limit=cfg.get("rate_limit", cfg.get("rate_limit_daily", 60)),
                )

        # 代理配置
        self._proxy_config = self._keys.get("proxy", {})
        self._last_loaded = time.time()
        logger.info(f"API配置已加载，共 {len(self._endpoints)} 个端点")

    def get_key(self, api_name: str) -> str:
        ep = self._endpoints.get(api_name)
        if not ep:
            raise KeyError(f"未知API: {api_name}")
        return ep.key

    def get_base_url(self, api_name: str) -> str:
        ep = self._endpoints.get(api_name)
        if not ep:
            raise KeyError(f"未知API: {api_name}")
        return ep.base_url

    def check_rate_limit(self, api_name: str):
        ep = self._endpoints.get(api_name)
        if ep:
            ep.check_rate_limit()

    def needs_proxy(self, api_name: str) -> bool:
        """判断该API是否需要代理"""
        if not self._proxy_config.get("enabled", False):
            return False
        no_proxy = self._proxy_config.get("no_proxy", [])
        return api_name not in no_proxy

    def proxy_url(self, original_url: str) -> str:
        """将URL包装为Cloudflare Worker代理URL"""
        if not self._proxy_config.get("enabled", False):
            return original_url

        worker_url = self._proxy_config.get("cloudflare_worker_url", "")
        token = self._proxy_config.get("cloudflare_token", "")
        if not worker_url:
            return original_url

        return f"{worker_url}?token={token}&url={urllib.parse.quote(original_url)}"

    def build_url(self, api_name: str, path: str = "", params: dict = None) -> str:
        """构建完整的API请求URL，自动处理代理"""
        base = self.get_base_url(api_name)
        full_url = f"{base}{path}"

        if params:
            qs = urllib.parse.urlencode(params)
            full_url = f"{full_url}?{qs}" if "?" not in full_url else f"{full_url}&{qs}"

        if self.needs_proxy(api_name):
            full_url = self.proxy_url(full_url)

        return full_url

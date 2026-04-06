"""
EnergyPulse 统一AI调用客户端
支持 DeepSeek / Qwen / MiniMax 三级自动切换。
所有模型均兼容OpenAI接口格式。
"""

import time
import json
import logging
import requests
from dataclasses import dataclass
from typing import Optional

from config_loader import APIConfig

logger = logging.getLogger("energypulse.llm")


@dataclass
class ModelConfig:
    name: str
    config_key: str          # api_keys.yaml中的key名
    model_id: str
    max_tokens: int = 4096
    temperature: float = 0.3


# 任务→模型映射注册表
TASK_MODELS = {
    "report": [
        ModelConfig("DeepSeek-V3", "deepseek", "deepseek-chat", max_tokens=8192),
        ModelConfig("Qwen-Plus", "qwen", "qwen-plus", max_tokens=8192),
        ModelConfig("MiniMax", "minimax", "MiniMax-Text-01", max_tokens=8192),
    ],
    "sentiment": [
        ModelConfig("DeepSeek-V3", "deepseek", "deepseek-chat", temperature=0.1),
        ModelConfig("Qwen-Plus", "qwen", "qwen-plus", temperature=0.1),
    ],
    "chinese_nlp": [
        ModelConfig("Qwen-Plus", "qwen", "qwen-plus", temperature=0.2),
        ModelConfig("DeepSeek-V3", "deepseek", "deepseek-chat", temperature=0.2),
    ],
    "anomaly": [
        ModelConfig("DeepSeek-V3", "deepseek", "deepseek-chat", max_tokens=1024),
        ModelConfig("Qwen-Turbo", "qwen", "qwen-turbo", max_tokens=1024),
    ],
}


class LLMClient:
    """
    统一AI调用客户端。

    使用:
        client = LLMClient()
        result = client.call("report", prompt, system="你是能源分析师...")
    """

    def __init__(self):
        self.config = APIConfig.get()
        self.call_history = []

    def call(self, task: str, prompt: str, system: str = "",
             max_tokens: int = None) -> str:
        """
        调用AI，自动按优先级尝试模型列表。
        task: 'report', 'sentiment', 'chinese_nlp', 'anomaly'
        """
        models = TASK_MODELS.get(task)
        if not models:
            raise ValueError(f"未知任务类型: {task}")

        last_error = None
        for model in models:
            try:
                result = self._invoke(model, prompt, system, max_tokens)
                self._log_call(task, model.name, "success", len(result))
                return result
            except Exception as e:
                last_error = e
                self._log_call(task, model.name, "failed", 0, str(e))
                logger.warning(f"[{task}] {model.name} 失败: {e}")
                time.sleep(2)

        raise RuntimeError(f"AI调用全部失败 (task={task}): {last_error}")

    def _invoke(self, model: ModelConfig, prompt: str, system: str,
                max_tokens: int = None) -> str:
        """调用单个模型（所有模型都兼容OpenAI格式）"""
        api_key = self.config.get_key(model.config_key)
        base_url = self.config.get_base_url(model.config_key)

        # 如果需要代理
        if self.config.needs_proxy(model.config_key):
            base_url = self.config.proxy_url(base_url)

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        body = {
            "model": model.model_id,
            "messages": messages,
            "max_tokens": max_tokens or model.max_tokens,
            "temperature": model.temperature,
        }

        resp = requests.post(
            base_url, headers=headers, json=body, timeout=120
        )
        resp.raise_for_status()
        data = resp.json()

        return data["choices"][0]["message"]["content"]

    def _log_call(self, task: str, model: str, status: str,
                  output_len: int, error: str = None):
        """记录调用历史"""
        entry = {
            "time": time.time(),
            "task": task,
            "model": model,
            "status": status,
            "output_len": output_len,
            "error": error,
        }
        self.call_history.append(entry)
        # 只保留最近100条
        if len(self.call_history) > 100:
            self.call_history = self.call_history[-100:]

    def get_stats(self) -> dict:
        """获取调用统计"""
        total = len(self.call_history)
        success = sum(1 for c in self.call_history if c["status"] == "success")
        return {
            "total_calls": total,
            "success_rate": f"{success/total*100:.1f}%" if total > 0 else "N/A",
            "recent_calls": self.call_history[-5:],
        }

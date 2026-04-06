"""
天气 HDD/CDD 采集器
 Heating Degree Days / Cooling Degree Days
用于预测电力需求
"""

import logging
from datetime import date, timedelta

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.weather")

# 美国主要电力市场城市
US_CITIES = [
    {"name": "New York", "lat": 40.71, "lon": -74.01},
    {"name": "Chicago", "lat": 41.88, "lon": -87.63},
    {"name": "Houston", "lat": 29.76, "lon": -95.37},
    {"name": "Los Angeles", "lat": 34.05, "lon": -118.24},
    {"name": "Atlanta", "lat": 33.75, "lon": -84.39},
]


class WeatherHDDCDDCollector(BaseCollector):
    """天气 HDD/CDD 采集器 - 使用 Open-Meteo (免费，无需API Key)"""

    def collect_primary(self) -> list[dict]:
        records = []
        today = date.today()
        
        for city in US_CITIES:
            try:
                # Open-Meteo API (免费，无需Key)
                url = f"https://api.open-meteo.com/v1/forecast"
                params = {
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "start_date": (today - timedelta(days=7)).isoformat(),
                    "end_date": today.isoformat(),
                    "daily": "temperature_2m_mean",
                    "temperature_unit": "fahrenheit",
                    "timezone": "America/New_York",
                }
                
                data = self.session.get(url, params=params, timeout=30).json()
                
                daily = data.get("daily", {})
                dates = daily.get("time", [])
                temps = daily.get("temperature_2m_mean", [])
                
                for d, temp in zip(dates, temps):
                    if temp is not None:
                        # 计算 HDD 和 CDD
                        # 基准温度 65°F
                        hdd = max(0, 65 - temp)
                        cdd = max(0, temp - 65)
                        
                        records.append({
                            "date": d,
                            "region": city["name"],
                            "hdd": round(hdd, 2),
                            "cdd": round(cdd, 2),
                            "temp_avg_f": round(temp, 2),
                            "source": "Open-Meteo",
                        })
                
                logger.info(f"天气 {city['name']}: {len(dates)} 天数据")
                
            except Exception as e:
                logger.warning(f"天气 {city['name']} 失败: {e}")
        
        return records
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_weather(records)
            logger.info(f"天气数据写入 {len(records)} 条")

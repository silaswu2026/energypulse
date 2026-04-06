FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Create log directory
RUN mkdir -p /app/logs

# Run
CMD ["python", "main.py"]

# 安装妙想技能依赖
RUN pip install --no-cache-dir pandas openpyxl requests

# 复制技能包
COPY skills/ /app/skills/

# 设置环境变量
ENV MX_APIKEY=mkt_oto52LfYMH1rSIO6BLUgp5JbuwdYdfFsKNsfqQjeMKk

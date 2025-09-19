# Dockerfile for Online Retail II ETL Pipeline
# 基於官方 Airflow 映像，加入專案所需的 Python 套件

FROM apache/airflow:2.8.1

# 切換到 root 用戶以安裝系統套件
USER root

# 安裝系統依賴（如果需要）
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 切換回 airflow 用戶
USER airflow

# 複製並安裝 Python 依賴
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 設定工作目錄
WORKDIR /opt/airflow
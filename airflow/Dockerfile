# Базовый образ Airflow + наши зависимости.
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

# 1) Провайдеры Airflow — строго по его constraints (без пина версий),
#    иначе конфликт версий с деревом пакетов Airflow.
COPY requirements.txt /requirements.txt
RUN PYV="$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"

# 2) Клиент ClickHouse — отдельным шагом БЕЗ constraints (не входит в дерево Airflow,
#    нужен только DAG №2; ставим заранее, чтобы образ был готов к этапу 3).
RUN pip install --no-cache-dir "clickhouse-connect==0.8.15"
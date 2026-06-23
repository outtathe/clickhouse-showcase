"""
DAG №2 — pg_to_clickhouse
=========================
Ежедневная переливка PostgreSQL core -> ClickHouse с денормализацией в плоские витрины.
Идёт после raw_to_pg (тот же ds). События грузятся НАПРЯМУЮ из CSV-партиции, минуя PG.

Поток:
  check_clickhouse -> load_dims -> load_facts -> load_events -> write_etl_log

Денормализация (JOIN в PG, плоский результат в CH) повторяет логику, провалидированную
на данных: orders_fact = заказы + атрибуты юзера + items_count; order_items_fact =
позиции + товар + категория; и т.д.

Идемпотентность:
  * dim'ы -> ReplacingMergeTree (повторная вставка схлопывается по версии updated_at);
  * факты -> перед вставкой дня удаляем его срез (DELETE ... WHERE toDate(...) = ds).

Подключения:
  * PG  — Airflow Connection `pg_shop`;
  * CH  — env CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/CH_DB/CH_SECURE
          (CH на той же VM -> CH_HOST=host.docker.internal, см. docker-compose extra_hosts).
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "pg_shop"
RAW_DIR = Path(os.environ.get("DATA_RAW_DIR", "/opt/airflow/data/raw"))

CH = dict(
    host=os.environ.get("CH_HOST", "host.docker.internal"),
    port=int(os.environ.get("CH_PORT", "8123")),
    username=os.environ.get("CH_USER", "default"),
    password=os.environ.get("CH_PASSWORD", ""),
    database=os.environ.get("CH_DB", "shop_mvp"),
    secure=os.environ.get("CH_SECURE", "False").lower() == "true",
)


def _ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CH)


def _log_etl(client, task_name, ds, table, rows, seconds, status="success"):
    # source_date в CH имеет тип Date -> приводим строку/datetime к date
    from datetime import date, datetime
    if isinstance(ds, str):
        ds = datetime.strptime(ds[:10], "%Y-%m-%d").date()
    elif isinstance(ds, datetime):
        ds = ds.date()
    client.insert(
        "shop_mvp.etl_log",
        [[ds, "pg_to_clickhouse", task_name, table, int(rows), float(seconds), status]],
        column_names=["source_date", "dag_id", "task", "target_table", "rows", "seconds", "status"],
    )


# --- денормализующие SELECT'ы из PG core (плоский результат -> в CH как есть) ---

SQL_USERS = """
SELECT user_id::text, email, full_name, registration_date,
       acquisition_channel, city, age_group, is_active::int AS is_active
FROM core.users
"""

SQL_PRODUCTS = """
SELECT p.product_id::text, p.name,
       COALESCE(p.category_id, 0) AS category_id,
       COALESCE(c.name, 'Без категории') AS category_name,
       '' AS parent_category,
       p.price, p.cost_price, COALESCE(p.rating, 0) AS rating, p.is_available::int AS is_available
FROM core.products p
LEFT JOIN core.categories c ON p.category_id = c.category_id
"""

SQL_ORDERS = """
SELECT o.order_id::text, o.user_id::text, o.created_at, o.status, o.payment_method,
       o.total_amount, o.discount_amount, COALESCE(ic.cnt, 0) AS items_count,
       u.acquisition_channel, u.city AS user_city, u.age_group AS user_age_group
FROM core.orders o
JOIN core.users u ON o.user_id = u.user_id
LEFT JOIN (SELECT order_id, count(*) AS cnt FROM core.order_items GROUP BY order_id) ic
       ON o.order_id = ic.order_id
WHERE o.created_at::date = %(ds)s
"""

SQL_ITEMS = """
SELECT i.order_item_id::text, i.order_id::text, o.created_at AS order_created_at,
       o.user_id::text, i.product_id::text,
       COALESCE(p.category_id, 0) AS category_id,
       COALESCE(c.name, 'Без категории') AS category_name,
       i.quantity, i.unit_price, (i.quantity * i.unit_price) AS line_revenue
FROM core.order_items i
JOIN core.orders o ON i.order_id = o.order_id
JOIN core.products p ON i.product_id = p.product_id
LEFT JOIN core.categories c ON p.category_id = c.category_id
WHERE o.created_at::date = %(ds)s
"""

SQL_PAYMENTS = """
SELECT payment_id::text, order_id::text, amount, method, status, processed_at
FROM core.payments
WHERE processed_at::date = %(ds)s
"""


@dag(
    dag_id="pg_to_clickhouse",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["showcase", "clickhouse", "etl"],
    doc_md=__doc__,
)
def pg_to_clickhouse():

    @task
    def check_clickhouse() -> None:
        client = _ch()
        client.command("SELECT 1")
        # на всякий: убедиться, что схема CH накатана
        n = client.command("SELECT count() FROM system.tables WHERE database='shop_mvp'")
        if int(n) == 0:
            raise RuntimeError("В ClickHouse нет схемы shop_mvp — примени clickhouse/schema.sql")

    @task
    def load_dims() -> None:
        """users_dim + products_dim из PG core -> CH (ReplacingMergeTree, перезаливка целиком)."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        client = _ch()
        for table, sql in [("users_dim", SQL_USERS), ("products_dim", SQL_PRODUCTS)]:
            t = time.time()
            df = hook.get_pandas_df(sql)
            df["updated_at"] = pendulum.now("UTC").naive()
            client.insert_df(f"shop_mvp.{table}", df)
            _log_etl(client, "load_dims", pendulum.now().date(), table, len(df), time.time() - t)

    @task
    def load_facts(ds=None) -> None:
        """orders/order_items/payments за ds: удалить срез дня в CH -> вставить заново."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        client = _ch()
        plan = [
            ("orders_fact",      SQL_ORDERS,   "created_at"),
            ("order_items_fact", SQL_ITEMS,    "order_created_at"),
            ("payments_fact",    SQL_PAYMENTS, "processed_at"),
        ]
        for table, sql, date_col in plan:
            t = time.time()
            df = hook.get_pandas_df(sql, parameters={"ds": ds})
            # идемпотентность: убрать ранее загруженный срез этого дня
            client.command(f"DELETE FROM shop_mvp.{table} WHERE toDate({date_col}) = %(ds)s",
                           parameters={"ds": ds})
            if len(df):
                client.insert_df(f"shop_mvp.{table}", df)
            _log_etl(client, "load_facts", ds, table, len(df), time.time() - t)

    @task
    def load_events(ds=None) -> None:
        """События за ds: CSV-партиция -> events_fact НАПРЯМУЮ (минуя PostgreSQL)."""
        import pandas as pd
        path = RAW_DIR / ds / "events.csv"
        if not path.exists():
            raise AirflowSkipException(f"Нет events.csv за {ds}")
        client = _ch()
        t = time.time()
        client.command("DELETE FROM shop_mvp.events_fact WHERE toDate(created_at) = %(ds)s",
                       parameters={"ds": ds})
        total = 0
        # читаем чанками — на large это десятки млн строк
        for chunk in pd.read_csv(path, chunksize=500_000, parse_dates=["created_at"]):
            client.insert_df("shop_mvp.events_fact", chunk)
            total += len(chunk)
        _log_etl(client, "load_events", ds, "events_fact", total, time.time() - t)

    @task
    def write_etl_log(ds=None) -> None:
        """Финальная метка успешного прогона дня."""
        client = _ch()
        _log_etl(client, "dag_complete", ds, "ALL", 0, 0.0, "success")

    check_clickhouse() >> load_dims() >> load_facts() >> load_events() >> write_etl_log()


pg_to_clickhouse()

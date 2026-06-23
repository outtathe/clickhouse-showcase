"""
DAG №1 — raw_to_pg
==================
Ежедневная инкрементальная загрузка одной суточной партиции генератора
в PostgreSQL по слоистой модели: CSV -> staging -> core (+ карантин брака).

Поток тасков:
  check_source -> load_staging -> promote_dims -> clean_facts -> data_quality -> cleanup

Особенности:
  * catchup=True + max_active_runs=1 -> backfill идёт по дням строго по порядку,
    поэтому dim'ы (users/products) успевают накопиться в core до фактов, которые
    на них ссылаются.
  * staging — транзиентный: TRUNCATE перед загрузкой и после (cleanup, all_done).
  * идемпотентность: dims через UPSERT, факты через ON CONFLICT DO NOTHING,
    карантин за ds очищается перед повторной обработкой.
  * events НЕ грузятся в PG — они идут CSV -> ClickHouse в DAG №2 (pg_to_clickhouse).

Подключение к БД: Airflow Connection `pg_shop` (задаётся через AIRFLOW_CONN_PG_SHOP
в docker-compose). Каталог партиций: env DATA_RAW_DIR (по умолчанию /opt/airflow/data/raw).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "pg_shop"
RAW_DIR = Path(os.environ.get("DATA_RAW_DIR", "/opt/airflow/data/raw"))
SQL_DIR = Path(__file__).parent / "sql"

# Сущности, которые едут в PG (events исключены сознательно — они идут прямо в CH).
# categories грузится из корневого файла, остальное — из суточной партиции.
STAGING_TABLES = ["categories", "users", "products", "orders", "order_items", "payments"]
FACT_TABLES = ["orders", "order_items", "payments"]

# Порядок колонок в CSV генератора (для COPY)
COPY_COLS = {
    "categories":  "category_id,name,parent_id",
    "users":       "user_id,email,full_name,registration_date,acquisition_channel,city,age_group,is_active",
    "products":    "product_id,name,category_id,price,cost_price,rating,is_available,created_at",
    "orders":      "order_id,user_id,created_at,status,payment_method,total_amount,discount_amount,promo_code",
    "order_items": "order_item_id,order_id,product_id,quantity,unit_price",
    "payments":    "payment_id,order_id,amount,method,status,processed_at",
}


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


@dag(
    dag_id="raw_to_pg",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # критично: сериализация backfill -> dim'ы раньше фактов
    default_args={"retries": 1, "retry_delay": pendulum.duration(seconds=30)},
    tags=["showcase", "postgres", "etl"],
    doc_md=__doc__,
)
def raw_to_pg():

    @task
    def check_source(ds=None) -> None:
        """Партиция за ds существует? Если нет (день без данных / аномальный день) — skip."""
        part = RAW_DIR / ds
        if not part.exists():
            raise AirflowSkipException(f"Нет партиции за {ds} — пропускаем день.")

    @task
    def load_staging(ds=None) -> dict:
        """TRUNCATE staging -> COPY суточной партиции -> проставить _source_date."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        part = RAW_DIR / ds

        hook.run("TRUNCATE " + ", ".join(f"staging.{t}" for t in STAGING_TABLES) + ";")

        counts: dict[str, int] = {}
        for t in STAGING_TABLES:
            # categories — единый файл в корне; остальное — внутри партиции дня
            csv_path = (RAW_DIR / "categories.csv") if t == "categories" else (part / f"{t}.csv")
            if not csv_path.exists():
                counts[t] = 0
                continue
            hook.copy_expert(
                f"COPY staging.{t}({COPY_COLS[t]}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                str(csv_path),
            )
            counts[t] = hook.get_first(f"SELECT count(*) FROM staging.{t}")[0]

        for t in FACT_TABLES:
            hook.run(
                f"UPDATE staging.{t} SET _source_date = %(ds)s WHERE _source_date IS NULL",
                parameters={"ds": ds},
            )
        return counts

    @task
    def promote_dims() -> None:
        """Справочники staging -> core (UPSERT, идемпотентно)."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(_sql("promote_dims.sql"))

    @task
    def clean_facts(ds=None) -> None:
        """Факты staging -> core с карантином брака. Очищаем карантин ds перед прогоном."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run("DELETE FROM dq.rejected_rows WHERE source_date = %(ds)s", parameters={"ds": ds})
        hook.run(_sql("promote_facts.sql"))

    @task
    def data_quality(staging_counts: dict, ds=None) -> None:
        """Паспорт прогона в dq.load_audit + жёсткие ассерты целостности core."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        # Манифест генератора: эталон ожидаемого числа строк за день
        manifest = {}
        mpath = RAW_DIR / ds / "manifest.json"
        if mpath.exists():
            manifest = json.loads(mpath.read_text())

        # Аудит по каждой сущности: staging = promoted + rejected (доказанное тождество)
        for t in FACT_TABLES:
            rejected = hook.get_first(
                "SELECT count(*) FROM dq.rejected_rows WHERE source_table=%s AND source_date=%s",
                parameters=(t, ds),
            )[0]
            staged = staging_counts.get(t, 0)
            promoted = staged - rejected
            hook.run(
                """
                INSERT INTO dq.load_audit
                    (source_date, source_table, rows_staging, rows_promoted,
                     rows_rejected, rows_manifest, status)
                VALUES (%(ds)s, %(t)s, %(staged)s, %(promoted)s, %(rejected)s, %(manifest)s, 'success')
                ON CONFLICT (source_date, source_table) DO UPDATE
                   SET rows_staging  = EXCLUDED.rows_staging,
                       rows_promoted = EXCLUDED.rows_promoted,
                       rows_rejected = EXCLUDED.rows_rejected,
                       rows_manifest = EXCLUDED.rows_manifest,
                       finished_at   = now(),
                       status        = 'success';
                """,
                parameters={"ds": ds, "t": t, "staged": staged, "promoted": promoted,
                            "rejected": rejected, "manifest": manifest.get(t)},
            )

        # Ассерт 1: целостность core (FK / CHECK / NOT NULL) — всё должно быть 0
        orphan_items, orphan_prod, nonpos_qty, null_user = hook.get_first(_sql("dq_assert.sql"))
        if any((orphan_items, orphan_prod, nonpos_qty, null_user)):
            raise ValueError(
                f"DQ FAIL целостности core: orphan_items={orphan_items}, "
                f"orphan_product={orphan_prod}, nonpositive_qty={nonpos_qty}, null_user={null_user}"
            )

        # Ассерт 2: потеря данных — в staging должно прийти не меньше, чем в манифесте
        for t in FACT_TABLES:
            expected = manifest.get(t)
            if expected is not None and staging_counts.get(t, 0) < expected:
                raise ValueError(
                    f"DQ FAIL потери данных в {t}: staging={staging_counts.get(t, 0)} < manifest={expected}"
                )

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        """staging транзиентный — чистим после прогона независимо от исхода."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(
            "TRUNCATE " + ", ".join(f"staging.{t}" for t in STAGING_TABLES) + ";"
        )

    counts = load_staging()
    dq = data_quality(counts)
    check_source() >> counts >> promote_dims() >> clean_facts() >> dq >> cleanup()


raw_to_pg()

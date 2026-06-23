-- =====================================================================
--  ClickHouse — аналитическая схема V2
--  Слои: dim (справочники) / fact (факты) / agg (витрины через MV) / ops
--  Движки: MergeTree, ReplacingMergeTree, AggregatingMergeTree, SummingMergeTree
--  Плюс: TTL на events, словарь из PostgreSQL, агрегатные состояния.
-- =====================================================================
CREATE DATABASE IF NOT EXISTS shop_mvp;

-- ============ DIM (ReplacingMergeTree: дедуп по ключу, версия updated_at) ============

CREATE TABLE IF NOT EXISTS shop_mvp.users_dim (
    user_id              UUID,
    email                String,
    full_name            String,
    registration_date    Date,
    acquisition_channel  LowCardinality(String),
    city                 LowCardinality(String),
    age_group            LowCardinality(String),
    is_active            UInt8,
    updated_at           DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS shop_mvp.products_dim (
    product_id       UUID,
    name             String,
    category_id      UInt16,
    category_name    LowCardinality(String),   -- денормализовано из categories
    parent_category  LowCardinality(String),
    price            Decimal(12,2),
    cost_price       Decimal(12,2),
    rating           Decimal(3,2),
    is_available     UInt8,
    updated_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

-- ============ FACT (MergeTree, помесячное партиционирование) ============

-- Витрина заказов: атрибуты пользователя «вшиты» -> аналитика без JOIN
CREATE TABLE IF NOT EXISTS shop_mvp.orders_fact (
    order_id            UUID,
    user_id             UUID,
    created_at          DateTime,
    status              LowCardinality(String),
    payment_method      LowCardinality(String),
    total_amount        Decimal(12,2),
    discount_amount     Decimal(12,2),
    items_count         UInt16,
    acquisition_channel LowCardinality(String),
    user_city           LowCardinality(String),
    user_age_group      LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, user_id);

-- Позиции заказов: товар + категория денормализованы -> разрезы по категориям, market basket
CREATE TABLE IF NOT EXISTS shop_mvp.order_items_fact (
    order_item_id    UUID,
    order_id         UUID,
    order_created_at DateTime,
    user_id          UUID,
    product_id       UUID,
    category_id      UInt16,
    category_name    LowCardinality(String),
    quantity         Int16,
    unit_price       Decimal(12,2),
    line_revenue     Decimal(18,2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_created_at)
ORDER BY (order_created_at, order_id);

-- Поведенческие события: TTL гасит данные старше 24 мес (управление жизненным циклом)
CREATE TABLE IF NOT EXISTS shop_mvp.events_fact (
    user_id      UUID,
    session_id   UUID,
    product_id   UUID,
    event_type   LowCardinality(String),
    created_at   DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, user_id, session_id)
TTL created_at + INTERVAL 24 MONTH;

CREATE TABLE IF NOT EXISTS shop_mvp.payments_fact (
    payment_id   UUID,
    order_id     UUID,
    amount       Decimal(12,2),
    method       LowCardinality(String),
    status       LowCardinality(String),
    processed_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(processed_at)
ORDER BY (processed_at, order_id);

-- ============ AGG (витрины через Materialized Views) ============

-- (AggregatingMergeTree) GMV/заказы/уникальные покупатели по дням — агрегатные состояния
CREATE TABLE IF NOT EXISTS shop_mvp.daily_gmv (
    day           Date,
    gmv_state     AggregateFunction(sum, Decimal(12,2)),
    orders_state  AggregateFunction(count),
    buyers_state  AggregateFunction(uniq, UUID)
) ENGINE = AggregatingMergeTree()
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS shop_mvp.mv_daily_gmv TO shop_mvp.daily_gmv AS
SELECT toDate(created_at)        AS day,
       sumState(total_amount)    AS gmv_state,
       countState()              AS orders_state,
       uniqState(user_id)        AS buyers_state
FROM shop_mvp.orders_fact
WHERE status = 'completed'
GROUP BY day;

-- (SummingMergeTree) выручка/штуки по категории и дню
CREATE TABLE IF NOT EXISTS shop_mvp.category_revenue_daily (
    day           Date,
    category_id   UInt16,
    category_name LowCardinality(String),
    revenue       Decimal(18,2),
    items_sold    UInt64
) ENGINE = SummingMergeTree()
ORDER BY (day, category_id, category_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS shop_mvp.mv_category_revenue TO shop_mvp.category_revenue_daily AS
SELECT toDate(order_created_at) AS day,
       category_id,
       category_name,
       sum(line_revenue)        AS revenue,
       sum(quantity)            AS items_sold
FROM shop_mvp.order_items_fact
GROUP BY day, category_id, category_name;

-- (SummingMergeTree) воронка по дням: счётчики событий схлопываются суммированием
CREATE TABLE IF NOT EXISTS shop_mvp.funnel_daily (
    day        Date,
    views      UInt64,
    carts      UInt64,
    purchases  UInt64
) ENGINE = SummingMergeTree()
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS shop_mvp.mv_funnel_daily TO shop_mvp.funnel_daily AS
SELECT toDate(created_at)                 AS day,
       countIf(event_type = 'view')       AS views,
       countIf(event_type = 'cart')       AS carts,
       countIf(event_type = 'purchase')   AS purchases
FROM shop_mvp.events_fact
GROUP BY day;

-- (AggregatingMergeTree) первая покупка пользователя в категории (аналог first-buys)
CREATE TABLE IF NOT EXISTS shop_mvp.user_category_first_buy (
    user_id         UUID,
    category_id     UInt16,
    first_buy_state AggregateFunction(min, DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY (user_id, category_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS shop_mvp.mv_user_category_first_buy TO shop_mvp.user_category_first_buy AS
SELECT user_id,
       category_id,
       minState(order_created_at) AS first_buy_state
FROM shop_mvp.order_items_fact
GROUP BY user_id, category_id;

-- ============ OPS (лог переливки — источник для Grafana) ============
CREATE TABLE IF NOT EXISTS shop_mvp.etl_log (
    ts            DateTime DEFAULT now(),
    dag_id        LowCardinality(String),
    task          LowCardinality(String),
    source_date   Date,
    target_table  LowCardinality(String),
    rows          UInt64,
    seconds       Float32,
    status        LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY ts;

-- ============ DICTIONARY (живая интеграция CH <- PostgreSQL, без ETL) ============
-- Создаётся НА КЛАСТЕРЕ (нужен доступ к PG). Параметры host/password — из окружения.
-- CREATE DICTIONARY shop_mvp.dict_categories (
--     category_id UInt64,
--     name        String,
--     parent_id   Nullable(UInt64)
-- ) PRIMARY KEY category_id
-- SOURCE(POSTGRESQL(
--     host 'postgres' port 5432 user 'postgres' password 'CHANGE_ME'
--     db 'shop_mvp' query 'SELECT category_id, name, parent_id FROM core.categories'))
-- LAYOUT(HASHED())
-- LIFETIME(MIN 300 MAX 600);
-- Использование:  dictGet('shop_mvp.dict_categories', 'name', toUInt64(category_id))

-- =====================================================================
--  PostgreSQL — слоистая схема V2
--  staging  : landing-слой (bronze). Всё TEXT, без констрейнтов, UNLOGGED.
--             Принимает «грязные» данные как есть — никогда не отклоняет строку.
--  core     : чистая 3НФ (silver). PK / FK / CHECK / индексы.
--             Только транзакционные сущности модерируемого объёма.
--  dq       : data-quality слой. Карантин отклонённых строк + аудит загрузок.
--
--  ВАЖНО: высокообъёмные поведенческие events НЕ нормализуются в core —
--  они живут в staging.events и оттуда переливаются прямо в ClickHouse.
--  Загонять десятки млн append-only фактов в констрейнтную 3НФ нереалистично.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS dq;

-- =====================================================================
--  STAGING (landing): permissive, UNLOGGED для быстрой пере-загрузки.
--  Метаколонки _source_date / _loaded_at / _batch_id — трассировка партиции.
-- =====================================================================

CREATE UNLOGGED TABLE staging.categories (
    category_id  TEXT,
    name         TEXT,
    parent_id    TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.users (
    user_id              TEXT,
    email                TEXT,
    full_name            TEXT,
    registration_date    TEXT,
    acquisition_channel  TEXT,
    city                 TEXT,
    age_group            TEXT,
    is_active            TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.products (
    product_id   TEXT,
    name         TEXT,
    category_id  TEXT,
    price        TEXT,
    cost_price   TEXT,
    rating       TEXT,
    is_available TEXT,
    created_at   TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.orders (
    order_id         TEXT,
    user_id          TEXT,
    created_at       TEXT,
    status           TEXT,
    payment_method   TEXT,
    total_amount     TEXT,
    discount_amount  TEXT,
    promo_code       TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.order_items (
    order_item_id TEXT,
    order_id      TEXT,
    product_id    TEXT,
    quantity      TEXT,
    unit_price    TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.payments (
    payment_id   TEXT,
    order_id     TEXT,
    amount       TEXT,
    method       TEXT,
    status       TEXT,
    processed_at TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.events (
    user_id      TEXT,
    session_id   TEXT,
    product_id   TEXT,
    event_type   TEXT,
    created_at   TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

-- =====================================================================
--  CORE (clean 3НФ). Констрейнты подобраны так, чтобы ИМЕННО грязь
--  генератора отсеивалась при промоушене staging → core:
--    NULL user_id в orders            -> NOT NULL  (hard reject)
--    NULL / orphan product_id в items -> NOT NULL + FK (hard reject)
--    quantity < 0 в items             -> CHECK > 0 (hard reject)
--    дубликат order_id                -> PRIMARY KEY (hard reject)
--    товар без категории              -> category_id NULLABLE (soft flag, не reject)
-- =====================================================================

CREATE TABLE core.categories (
    category_id  INTEGER PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    parent_id    INTEGER REFERENCES core.categories(category_id)
);

CREATE TABLE core.users (
    user_id              UUID PRIMARY KEY,
    email                VARCHAR(255) NOT NULL,
    full_name            VARCHAR(255),
    registration_date    DATE NOT NULL,
    acquisition_channel  VARCHAR(50),
    city                 VARCHAR(100),
    age_group            VARCHAR(20),
    is_active            BOOLEAN DEFAULT TRUE
);

CREATE TABLE core.products (
    product_id    UUID PRIMARY KEY,
    name          VARCHAR(255),
    category_id   INTEGER REFERENCES core.categories(category_id) ON DELETE RESTRICT,  -- NULL = uncategorized (soft-flag)
    price         NUMERIC(12,2) CHECK (price >= 0),
    cost_price    NUMERIC(12,2),
    rating        NUMERIC(3,2),
    is_available  BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP
);

CREATE TABLE core.orders (
    order_id         UUID PRIMARY KEY,
    user_id          UUID NOT NULL REFERENCES core.users(user_id) ON DELETE RESTRICT,
    created_at       TIMESTAMP NOT NULL,
    status           VARCHAR(20),
    payment_method   VARCHAR(30),
    total_amount     NUMERIC(12,2) CHECK (total_amount >= 0),
    discount_amount  NUMERIC(12,2) DEFAULT 0,
    promo_code       VARCHAR(50)
);

CREATE TABLE core.order_items (
    order_item_id  UUID PRIMARY KEY,
    order_id       UUID NOT NULL REFERENCES core.orders(order_id) ON DELETE RESTRICT,
    product_id     UUID NOT NULL REFERENCES core.products(product_id) ON DELETE RESTRICT,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE core.payments (
    payment_id   UUID PRIMARY KEY,
    order_id     UUID NOT NULL UNIQUE REFERENCES core.orders(order_id) ON DELETE RESTRICT,  -- 1:1 к заказу
    amount       NUMERIC(12,2),
    method       VARCHAR(30),
    status       VARCHAR(20),
    processed_at TIMESTAMP
);

-- Индексы по FK и по датам — под аналитические выборки и проверки целостности
CREATE INDEX idx_products_category   ON core.products(category_id);
CREATE INDEX idx_products_created    ON core.products(created_at);
CREATE INDEX idx_users_reg_date      ON core.users(registration_date);
CREATE INDEX idx_orders_user_id      ON core.orders(user_id);
CREATE INDEX idx_orders_created_at   ON core.orders(created_at);
CREATE INDEX idx_items_order_id      ON core.order_items(order_id);
CREATE INDEX idx_items_product_id    ON core.order_items(product_id);
CREATE INDEX idx_payments_order_id   ON core.payments(order_id);

-- =====================================================================
--  DQ: карантин брака + аудит загрузок
-- =====================================================================

-- Каждая строка, отклонённая при промоушене staging -> core, с причиной
-- и полным исходным payload (JSONB) для разбора.
CREATE TABLE dq.rejected_rows (
    id            BIGSERIAL PRIMARY KEY,
    source_table  TEXT        NOT NULL,           -- 'orders' / 'order_items' / ...
    source_date   DATE,                            -- ds партиции
    reason        TEXT        NOT NULL,           -- 'null_user_id' / 'orphan_product' / 'negative_quantity' / 'duplicate_pk' / ...
    raw_data      JSONB,                           -- исходная строка как есть
    rejected_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_rejected_table_date ON dq.rejected_rows(source_table, source_date);
CREATE INDEX idx_rejected_reason     ON dq.rejected_rows(reason);

-- Паспорт каждого прогона DAG: сколько строк пришло / промотировано / отклонено.
-- Аналог project-run-summary: один SELECT показывает здоровье пайплайна,
-- источник для вкладки Data Quality в DataLens и для Grafana.
CREATE TABLE dq.load_audit (
    source_date       DATE        NOT NULL,        -- ds партиции
    source_table      TEXT        NOT NULL,
    run_id            TEXT,                          -- {dag_run_id} Airflow (последний прогон)
    rows_staging      BIGINT      DEFAULT 0,       -- прочитано из CSV в staging
    rows_promoted     BIGINT      DEFAULT 0,       -- доехало в core
    rows_rejected     BIGINT      DEFAULT 0,       -- ушло в карантин
    rows_manifest     BIGINT,                      -- ожидалось по manifest.json генератора
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ DEFAULT now(),
    status            VARCHAR(20) DEFAULT 'success',
    PRIMARY KEY (source_date, source_table)
);
CREATE INDEX idx_audit_date ON dq.load_audit(source_date);

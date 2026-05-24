CREATE TABLE shop_mvp.users_dim (
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

CREATE TABLE shop_mvp.orders_fact (
    order_id             UUID,
    user_id              UUID,
    created_at           DateTime,
    status               LowCardinality(String),
    payment_method       LowCardinality(String),
    total_amount         Decimal(12,2),
    discount_amount      Decimal(10,2),
    items_count          UInt16,
    acquisition_channel  LowCardinality(String),
    user_city            LowCardinality(String),
    user_age_group       LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, user_id);
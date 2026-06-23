CREATE SCHEMA IF NOT EXISTS shop_mvp;

CREATE TABLE shop_mvp.categories (
    category_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    parent_id     INTEGER REFERENCES shop_mvp.categories(category_id)
);

CREATE TABLE shop_mvp.users (
    user_id              UUID PRIMARY KEY,
    email                VARCHAR(255) UNIQUE NOT NULL,
    full_name            VARCHAR(255),
    registration_date    DATE NOT NULL,
    acquisition_channel  VARCHAR(50),
    city                 VARCHAR(100),
    age_group            VARCHAR(20),
    is_active            BOOLEAN DEFAULT TRUE
);

CREATE TABLE shop_mvp.products (
    product_id    UUID PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    category_id   INTEGER REFERENCES shop_mvp.categories(category_id) ON DELETE RESTRICT,
    price         NUMERIC(12,2) NOT NULL,
    cost_price    NUMERIC(12,2),
    rating        NUMERIC(3,2),
    is_available  BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE shop_mvp.orders (
    order_id         UUID PRIMARY KEY,
    user_id          UUID REFERENCES shop_mvp.users(user_id) ON DELETE RESTRICT,
    created_at       TIMESTAMP NOT NULL,
    status           VARCHAR(20),
    payment_method   VARCHAR(30),
    total_amount     NUMERIC(12,2),
    discount_amount  NUMERIC(12,2) DEFAULT 0,
    promo_code       VARCHAR(50)
);

CREATE TABLE shop_mvp.order_items (
    order_item_id  UUID PRIMARY KEY,
    order_id       UUID REFERENCES shop_mvp.orders(order_id) ON DELETE RESTRICT,
    product_id     UUID REFERENCES shop_mvp.products(product_id) ON DELETE RESTRICT,
    quantity       INTEGER NOT NULL,
    unit_price     NUMERIC(12,2) NOT NULL
);

CREATE INDEX idx_orders_user_id    ON shop_mvp.orders(user_id);
CREATE INDEX idx_orders_created_at ON shop_mvp.orders(created_at);
CREATE INDEX idx_items_order_id    ON shop_mvp.order_items(order_id);
CREATE INDEX idx_items_product_id  ON shop_mvp.order_items(product_id);
-- Факты staging -> core с маршрутизацией брака в dq.rejected_rows.
-- Карантин за этот ds очищается DAG'ом ДО запуска этого скрипта (идемпотентность).

-- ============ ORDERS ============
-- (1) NULL user_id -> карантин
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'orders', _source_date, 'null_user_id', to_jsonb(s)
FROM staging.orders s WHERE s.user_id IS NULL;

-- (2) user_id есть, но такого юзера нет в core.users -> карантин (orphan_user).
--     ФИКС: без этого заказ «осиротевшего» юзера падал на FK orders_user_id_fkey.
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'orders', _source_date, 'orphan_user', to_jsonb(s)
FROM staging.orders s
WHERE s.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM core.users u WHERE u.user_id::text = s.user_id);

-- (3) дубликаты order_id среди валидных -> карантин (оставляем первый)
WITH valid AS (
  SELECT s.* FROM staging.orders s
  WHERE s.user_id IS NOT NULL
    AND EXISTS (SELECT 1 FROM core.users u WHERE u.user_id::text = s.user_id)),
ranked AS (
  SELECT v.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn FROM valid v)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'orders', _source_date, 'duplicate_pk', to_jsonb(r) - 'rn'
FROM ranked r WHERE rn > 1;

-- (4) выжившие -> core
WITH valid AS (
  SELECT s.* FROM staging.orders s
  WHERE s.user_id IS NOT NULL
    AND EXISTS (SELECT 1 FROM core.users u WHERE u.user_id::text = s.user_id)),
ranked AS (
  SELECT v.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn FROM valid v)
INSERT INTO core.orders(order_id, user_id, created_at, status, payment_method,
                        total_amount, discount_amount, promo_code)
SELECT order_id::uuid, user_id::uuid, created_at::timestamp, status, payment_method,
       total_amount::numeric, COALESCE(NULLIF(discount_amount,''),'0')::numeric, NULLIF(promo_code,'')
FROM ranked WHERE rn = 1
ON CONFLICT (order_id) DO NOTHING;

-- ============ ORDER_ITEMS ============
WITH cls AS (
  SELECT i.*,
    CASE WHEN i.product_id IS NULL          THEN 'null_product_id'
         WHEN p.product_id IS NULL          THEN 'orphan_product'
         WHEN i.quantity::int <= 0          THEN 'negative_quantity'
         WHEN o.order_id  IS NULL           THEN 'orphan_order'
         ELSE NULL END AS reject_reason
  FROM staging.order_items i
  LEFT JOIN core.products p ON i.product_id = p.product_id::text
  LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'order_items', _source_date, reject_reason, to_jsonb(c) - 'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

WITH cls AS (
  SELECT i.*,
    CASE WHEN i.product_id IS NULL          THEN 'null_product_id'
         WHEN p.product_id IS NULL          THEN 'orphan_product'
         WHEN i.quantity::int <= 0          THEN 'negative_quantity'
         WHEN o.order_id  IS NULL           THEN 'orphan_order'
         ELSE NULL END AS reject_reason
  FROM staging.order_items i
  LEFT JOIN core.products p ON i.product_id = p.product_id::text
  LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text)
INSERT INTO core.order_items(order_item_id, order_id, product_id, quantity, unit_price)
SELECT order_item_id::uuid, order_id::uuid, product_id::uuid, quantity::int, unit_price::numeric
FROM cls WHERE reject_reason IS NULL
ON CONFLICT (order_item_id) DO NOTHING;

-- ============ PAYMENTS ============ (платёж отклонённого заказа -> карантин)
WITH cls AS (
  SELECT s.*, CASE WHEN o.order_id IS NULL THEN 'orphan_order' ELSE NULL END AS reject_reason
  FROM staging.payments s LEFT JOIN core.orders o ON s.order_id = o.order_id::text)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'payments', _source_date, reject_reason, to_jsonb(c) - 'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

WITH cls AS (
  SELECT s.*, CASE WHEN o.order_id IS NULL THEN 'orphan_order' ELSE NULL END AS reject_reason
  FROM staging.payments s LEFT JOIN core.orders o ON s.order_id = o.order_id::text)
INSERT INTO core.payments(payment_id, order_id, amount, method, status, processed_at)
SELECT payment_id::uuid, order_id::uuid, amount::numeric, method, status, processed_at::timestamp
FROM cls WHERE reject_reason IS NULL
ON CONFLICT (payment_id) DO NOTHING;

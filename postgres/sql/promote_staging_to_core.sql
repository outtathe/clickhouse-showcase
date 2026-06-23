-- ============ DIMS: staging -> core (с кастами типов) ============
INSERT INTO core.categories(category_id,name,parent_id)
SELECT category_id::int, name, NULLIF(parent_id,'')::int FROM staging.categories;

INSERT INTO core.users(user_id,email,full_name,registration_date,acquisition_channel,city,age_group,is_active)
SELECT user_id::uuid,email,full_name,registration_date::date,acquisition_channel,city,age_group,is_active::boolean
FROM staging.users;

INSERT INTO core.products(product_id,name,category_id,price,cost_price,rating,is_available,created_at)
SELECT product_id::uuid,name,NULLIF(category_id,'')::int,price::numeric,cost_price::numeric,
       rating::numeric,is_available::boolean,created_at::timestamp
FROM staging.products;

-- ============ ORDERS: чистим + карантиним ============
-- (1) NULL user_id -> карантин
INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'orders',_source_date,'null_user_id',to_jsonb(s)
FROM staging.orders s WHERE s.user_id IS NULL;

-- (2) дубликаты order_id (оставляем первый) -> карантин
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'orders',_source_date,'duplicate_pk',to_jsonb(r)-'rn'
FROM ranked r WHERE rn > 1;

-- (3) выжившие -> core
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO core.orders(order_id,user_id,created_at,status,payment_method,total_amount,discount_amount,promo_code)
SELECT order_id::uuid,user_id::uuid,created_at::timestamp,status,payment_method,
       total_amount::numeric,COALESCE(NULLIF(discount_amount,''),'0')::numeric,NULLIF(promo_code,'')
FROM ranked WHERE rn = 1;

-- ============ ORDER_ITEMS: классификация по приоритету причин ============
CREATE TEMP TABLE cls AS
SELECT i.*,
  CASE
    WHEN i.product_id IS NULL                THEN 'null_product_id'
    WHEN p.product_id IS NULL                THEN 'orphan_product'
    WHEN i.quantity::int <= 0                THEN 'negative_quantity'
    WHEN o.order_id  IS NULL                 THEN 'orphan_order'      -- позиция отклонённого заказа
    ELSE NULL
  END AS reject_reason
FROM staging.order_items i
LEFT JOIN core.products p ON i.product_id = p.product_id::text
LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text;

INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'order_items',_source_date,reject_reason,to_jsonb(c)-'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

INSERT INTO core.order_items(order_item_id,order_id,product_id,quantity,unit_price)
SELECT order_item_id::uuid,order_id::uuid,product_id::uuid,quantity::int,unit_price::numeric
FROM cls WHERE reject_reason IS NULL;

-- ============ АУДИТ прогона ============
INSERT INTO dq.load_audit(run_id,source_date,source_table,rows_staging,rows_promoted,rows_rejected,started_at)
SELECT 'manual-test', (SELECT _source_date FROM staging.orders LIMIT 1), 'orders',
       (SELECT count(*) FROM staging.orders),(SELECT count(*) FROM core.orders),
       (SELECT count(*) FROM dq.rejected_rows WHERE source_table='orders'), now()
UNION ALL
SELECT 'manual-test', (SELECT _source_date FROM staging.order_items LIMIT 1), 'order_items',
       (SELECT count(*) FROM staging.order_items),(SELECT count(*) FROM core.order_items),
       (SELECT count(*) FROM dq.rejected_rows WHERE source_table='order_items'), now();

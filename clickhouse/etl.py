import os
import pandas as pd
import psycopg2
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

pg = psycopg2.connect(
    host=os.getenv('PG_HOST'), port=os.getenv('PG_PORT'),
    dbname=os.getenv('PG_DB'), user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
)

ch = clickhouse_connect.get_client(
    host=os.getenv('CH_HOST'),
    user=os.getenv('CH_USER'),
    password=os.getenv('CH_PASSWORD'),
    secure=True,
    database='shop_mvp',
)

# --- users_dim ---
users = pd.read_sql('SELECT user_id, email, full_name, registration_date, '
                    'acquisition_channel, city, age_group, is_active::int '
                    'FROM shop_mvp.users', pg)
ch.insert_df('users_dim', users)
print(f'users_dim: {len(users)} rows')

# --- orders_fact (денормализация на стороне PG) ---
orders = pd.read_sql("""
    SELECT
        o.order_id,
        o.user_id,
        o.created_at,
        o.status,
        o.payment_method,
        o.total_amount,
        COALESCE(o.discount_amount, 0) AS discount_amount,
        COUNT(oi.order_item_id)::int   AS items_count,
        u.acquisition_channel,
        u.city                          AS user_city,
        u.age_group                     AS user_age_group
    FROM shop_mvp.orders o
    JOIN shop_mvp.users u ON o.user_id = u.user_id
    LEFT JOIN shop_mvp.order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, u.user_id
""", pg)
ch.insert_df('orders_fact', orders)
print(f'orders_fact: {len(orders)} rows')

# --- Проверка ---
for t in ['users_dim', 'orders_fact']:
    n = ch.query(f'SELECT count() FROM {t}').result_set[0][0]
    print(f'  CH {t}: {n}')

pg.close()
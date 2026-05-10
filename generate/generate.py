import os
import uuid
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker('ru_RU')
Faker.seed(SEED)

OUT_DIR = './data'
os.makedirs(OUT_DIR, exist_ok=True)

# --- categories ---
categories = pd.DataFrame([
    {'category_id': 1, 'name': 'Электроника', 'parent_id': None},
    {'category_id': 2, 'name': 'Одежда',       'parent_id': None},
    {'category_id': 3, 'name': 'Книги',        'parent_id': None},
    {'category_id': 4, 'name': 'Дом и сад',    'parent_id': None},
    {'category_id': 5, 'name': 'Спорт',        'parent_id': None},
])
categories.to_csv(f'{OUT_DIR}/categories.csv', index=False)

# --- users ---
users = pd.DataFrame([{
    'user_id': str(uuid.uuid4()),
    'email': fake.unique.email(),
    'full_name': fake.name(),
    'registration_date': fake.date_between(start_date='-18M', end_date='today'),
    'acquisition_channel': random.choice(['organic','paid_search','social','referral']),
    'city': fake.city(),
    'age_group': random.choice(['18-24','25-34','35-44','45+']),
    'is_active': random.random() < 0.85,
} for _ in range(1000)])
users.to_csv(f'{OUT_DIR}/users.csv', index=False)

# --- products ---
products = pd.DataFrame([{
    'product_id': str(uuid.uuid4()),
    'name': fake.catch_phrase(),
    'category_id': random.randint(1, 5),
    'price': round(np.random.lognormal(mean=7.3, sigma=0.8), 2),
    'cost_price': None,  # заполним ниже
    'rating': round(random.uniform(1, 5), 2),
    'is_available': random.random() < 0.92,
    'created_at': fake.date_time_between(start_date='-18M', end_date='now'),
} for _ in range(200)])
products['cost_price'] = (products['price'] * np.random.uniform(0.4, 0.7, len(products))).round(2)
products.to_csv(f'{OUT_DIR}/products.csv', index=False)

# --- orders + order_items ---
orders, items = [], []
user_ids = users['user_id'].tolist()
product_ids = products['product_id'].tolist()
product_prices = dict(zip(products['product_id'], products['price']))

for _ in range(5000):
    oid = str(uuid.uuid4())
    n_items = random.randint(1, 4)
    item_rows = []
    for _ in range(n_items):
        pid = random.choice(product_ids)
        qty = random.randint(1, 3)
        item_rows.append({
            'order_item_id': str(uuid.uuid4()),
            'order_id': oid,
            'product_id': pid,
            'quantity': qty,
            'unit_price': product_prices[pid],
        })
    total = sum(r['quantity'] * r['unit_price'] for r in item_rows)
    orders.append({
        'order_id': oid,
        'user_id': random.choice(user_ids),
        'created_at': fake.date_time_between(start_date='-18M', end_date='now'),
        'status': random.choices(['completed','cancelled','returned'], weights=[0.85,0.10,0.05])[0],
        'payment_method': random.choice(['card','sbp','cash_on_delivery']),
        'total_amount': round(total, 2),
        'discount_amount': 0,
        'promo_code': None,
    })
    items.extend(item_rows)

pd.DataFrame(orders).to_csv(f'{OUT_DIR}/orders.csv', index=False)
pd.DataFrame(items).to_csv(f'{OUT_DIR}/order_items.csv', index=False)

print('Done. Files:')
for f in os.listdir(OUT_DIR):
    print(' ', f, len(pd.read_csv(f'{OUT_DIR}/{f}')), 'rows')
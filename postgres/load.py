import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

from urllib.parse import quote_plus

DSN = (f"postgresql+psycopg2://{os.getenv('PG_USER')}:{quote_plus(os.getenv('PG_PASSWORD'))}"
       f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}")
print('DSN:', DSN.replace(os.getenv('PG_PASSWORD', ''), '***'))
print('CWD:', os.getcwd())

engine = create_engine(DSN)

# Порядок важен: справочники → факты
order = ['categories', 'users', 'products', 'orders', 'order_items']

for name in order:
    df = pd.read_csv(f'./data/{name}.csv')
    df.to_sql(name, engine, schema='shop_mvp', if_exists='append', index=False)
    print(f'{name}: {len(df)} rows')

# Проверка
with engine.connect() as conn:
    for name in order:
        from sqlalchemy import text
        n = conn.execute(text(f'SELECT count(*) FROM shop_mvp.{name}')).scalar()
        print(f'  PG {name}: {n}')
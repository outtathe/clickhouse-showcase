import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT'),
        dbname=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
    )
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print('PG version:', cur.fetchone()[0])
    conn.close()
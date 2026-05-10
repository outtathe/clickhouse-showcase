import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()  # читает .env из корня проекта
if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host=os.getenv('CH_HOST'),
        user=os.getenv('CH_USER'),
        password=os.getenv('CH_PASSWORD'),
        secure=os.getenv('CH_SECURE', 'True').lower() == 'true',
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])
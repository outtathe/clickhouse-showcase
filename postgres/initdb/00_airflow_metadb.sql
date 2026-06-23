-- Отдельная БД метаданных Airflow в том же инстансе Postgres.
-- Выполняется ДО business-схемы (алфавитный порядок init-скриптов).
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

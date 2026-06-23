-- S3 (фирменный). Приближённые vs точные уникальные: на больших данных uniqCombined
-- и uniqHLL12 считают почти точно, но во много раз дешевле по памяти, чем uniqExact.
SELECT
    uniqExact(user_id)    AS exact,      -- точное (дорого)
    uniqCombined(user_id) AS combined,   -- адаптивный (по умолчанию в uniq())
    uniqHLL12(user_id)    AS hll12       -- HyperLogLog, минимум памяти
FROM shop_mvp.events_fact;

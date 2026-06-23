-- Q3. Когортный retention: матрица «месяц регистрации × смещение в месяцах × сколько
-- пользователей сделали заказ». Длинный формат — DataLens разворачивает в heatmap.
WITH
  uc AS (SELECT user_id, toStartOfMonth(registration_date) AS cohort FROM shop_mvp.users_dim FINAL),
  ua AS (SELECT DISTINCT user_id, toStartOfMonth(created_at) AS active_month
         FROM shop_mvp.orders_fact WHERE status = 'completed')
SELECT
    cohort,
    dateDiff('month', cohort, ua.active_month) AS month_offset,
    uniqExact(uc.user_id)                       AS users
FROM uc INNER JOIN ua USING(user_id)
WHERE ua.active_month >= cohort
GROUP BY cohort, month_offset
ORDER BY cohort, month_offset;

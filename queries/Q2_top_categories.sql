-- Q2. Топ категорий по выручке. Источник — category_revenue_daily (SummingMergeTree):
-- счётчики уже схлопнуты суммированием, остаётся финальная агрегация по категории.
SELECT
    category_name,
    round(sum(revenue), 0) AS revenue,
    sum(items_sold)        AS items
FROM shop_mvp.category_revenue_daily
GROUP BY category_name
ORDER BY revenue DESC
LIMIT 10;

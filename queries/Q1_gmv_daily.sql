-- Q1. GMV / заказы / уникальные покупатели по дням.
-- Источник — витрина daily_gmv (AggregatingMergeTree): читаем агрегатные состояния
-- через *Merge. Это в разы дешевле, чем считать sum/uniq по сырому orders_fact.
SELECT
    day,
    sumMerge(gmv_state)                                   AS gmv,
    countMerge(orders_state)                              AS orders,
    uniqMerge(buyers_state)                               AS buyers,
    round(sumMerge(gmv_state) / countMerge(orders_state), 2) AS aov
FROM shop_mvp.daily_gmv
GROUP BY day
ORDER BY day;

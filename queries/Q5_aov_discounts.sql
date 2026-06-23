-- Q5. Средний чек и влияние промокодов: сегментируем заказы на «с промокодом»
-- (discount_amount > 0) и «без», сравниваем AOV, GMV и среднюю скидку.
SELECT
    if(discount_amount > 0, 'промокод', 'без промо') AS segment,
    count()                       AS orders,
    round(avg(total_amount), 2)   AS aov,
    round(sum(total_amount), 0)   AS gmv,
    round(avg(discount_amount), 2) AS avg_discount
FROM shop_mvp.orders_fact
WHERE status = 'completed'
GROUP BY segment;

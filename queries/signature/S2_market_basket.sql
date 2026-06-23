-- S2 (фирменный). Market basket: какие категории чаще покупают вместе в одном заказе.
-- Self-join order_items_fact по order_id с условием cat_a < cat_b (пары без повторов).
-- E-commerce-аналог графовой аналитики «что с чем связано».
SELECT
    i1.category_name AS cat_a,
    i2.category_name AS cat_b,
    count()          AS together
FROM shop_mvp.order_items_fact i1
INNER JOIN shop_mvp.order_items_fact i2
    ON i1.order_id = i2.order_id
   AND i1.category_name < i2.category_name
GROUP BY cat_a, cat_b
ORDER BY together DESC
LIMIT 15;

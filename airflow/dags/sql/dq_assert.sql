-- После загрузки core обязан быть целостным. Все четыре числа = 0, иначе DAG падает.
SELECT
  (SELECT count(*) FROM core.order_items i LEFT JOIN core.orders   o USING(order_id)   WHERE o.order_id   IS NULL) AS orphan_order_items,
  (SELECT count(*) FROM core.order_items i LEFT JOIN core.products p USING(product_id) WHERE p.product_id IS NULL) AS orphan_product_items,
  (SELECT count(*) FROM core.order_items WHERE quantity <= 0)                                                       AS nonpositive_qty,
  (SELECT count(*) FROM core.orders WHERE user_id IS NULL)                                                          AS null_user_orders;

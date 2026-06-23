-- Q4. Воронка view -> cart -> purchase по сессиям через windowFunnel.
-- windowFunnel(окно_сек)(время, шаг1, шаг2, шаг3) возвращает макс. длину достигнутой
-- цепочки в пределах окна (24ч). level=k -> сессия дошла до шага k.
SELECT
    level,
    count() AS sessions
FROM (
    SELECT
        session_id,
        windowFunnel(86400)(
            created_at,
            event_type = 'view',
            event_type = 'cart',
            event_type = 'purchase'
        ) AS level
    FROM shop_mvp.events_fact
    GROUP BY session_id
)
GROUP BY level
ORDER BY level;

-- Справочники staging -> core с UPSERT: повторный прогон дня не ломается,
-- dim'ы накапливаются инкрементально (новые users/products каждого дня).
-- ФИКС: id-поля кастуются через ::numeric::int — генератор пишет их как float
-- ("1.0"), и прямой ::int падает на InvalidTextRepresentation.

INSERT INTO core.categories(category_id, name, parent_id)
SELECT category_id::numeric::int, name, NULLIF(parent_id,'')::numeric::int
FROM staging.categories
ON CONFLICT (category_id) DO NOTHING;

INSERT INTO core.users(user_id, email, full_name, registration_date,
                       acquisition_channel, city, age_group, is_active)
SELECT user_id::uuid, email, full_name, registration_date::date,
       acquisition_channel, city, age_group, is_active::boolean
FROM staging.users
ON CONFLICT (user_id) DO UPDATE
   SET email = EXCLUDED.email,
       full_name = EXCLUDED.full_name,
       is_active = EXCLUDED.is_active;

INSERT INTO core.products(product_id, name, category_id, price, cost_price,
                          rating, is_available, created_at)
SELECT product_id::uuid, name, NULLIF(category_id,'')::numeric::int, price::numeric,
       cost_price::numeric, rating::numeric, is_available::boolean, created_at::timestamp
FROM staging.products
ON CONFLICT (product_id) DO UPDATE
   SET price = EXCLUDED.price,
       rating = EXCLUDED.rating,
       is_available = EXCLUDED.is_available;

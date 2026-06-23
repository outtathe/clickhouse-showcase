#!/usr/bin/env bash
#
# migrate_to_v2.sh — превращает ТЕКУЩИЙ репозиторий clickhouse-showcase в V2 НА МЕСТЕ.
# Запускать из КОРНЯ репозитория:  bash migrate_to_v2.sh
#
# Что делает:
#   * проверяет, что ты в корне репо;
#   * заменяемые файлы копирует в .migration_backup_<дата>/ перед перезаписью;
#   * создаёт новые каталоги и файлы V2;
#   * НЕ трогает: .env, .git, clickhouse/, queries/, README.md, requirements.txt,
#     postgres/load.py, postgres/check_connection.py.
# Безопасно запускать повторно (бэкап создаётся с новой меткой времени).

set -euo pipefail

# --- 0. Защита: убедиться, что это корень репозитория clickhouse-showcase ---
if [[ ! -f generate/generate.py || ! -d postgres ]]; then
  echo "ОШИБКА: запускать из корня репозитория clickhouse-showcase (где есть generate/ и postgres/)." >&2
  exit 1
fi

BACKUP=".migration_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP"
echo "Бэкап заменяемых файлов -> $BACKUP/"

# --- 1. Создать новые каталоги ---
mkdir -p "postgres/initdb"
mkdir -p "postgres/sql"
mkdir -p "airflow/dags/sql"
mkdir -p "airflow/plugins"
mkdir -p "docs/img"
echo "Каталоги созданы."

# --- 2. Записать файлы V2 (заменяемые предварительно бэкапятся) ---
# ---- generate/generate.py  [REPLACE] ----
if [[ -f "generate/generate.py" ]]; then cp -p "generate/generate.py" "$BACKUP/$(echo "generate/generate.py" | tr "/" "_")"; fi
mkdir -p "generate"
cat > "generate/generate.py" << '__V2_FILE_EOF__'
#!/usr/bin/env python3
"""
Генератор синтетических данных интернет-магазина — v2.

Что нового относительно v1:
  * CLI: --scale / --seed / --output / --months / --dirty / --partition-by-day
  * Сущности events (view/cart/purchase) и payments
  * Сезонность: месячная (Black Friday +40%, декабрь +25%, февраль −15%),
    недельная (выходные), суточная (вечерний пик) + линейный рост бизнеса
  * Один аномальный день с нулевыми продажами (имитация инцидента)
  * Промокоды и скидки (~15% заказов)
  * Причинная воронка событий: view → cart → purchase, привязанная к заказам
  * Режим «грязных» данных (--dirty): дубликаты заказов, NULL в ключах,
    отрицательные количества, продукты-сироты, товары без категории
  * Режим выгрузки по дням (--partition-by-day) под Airflow-DAG:
    data/raw/YYYY-MM-DD/{users,products,orders,order_items,events,payments}.csv
    + manifest.json с контрольными счётчиками для data quality проверок
  * Векторизованная генерация фактов (numpy) — large scale за разумное время

Примеры запуска:
  python generate.py --scale small --seed 42 --output ./data
  python generate.py --scale medium --dirty --partition-by-day --output ./data/raw
  python generate.py --scale large --dirty --partition-by-day
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
import time

import numpy as np
import pandas as pd
from faker import Faker

try:
    from tqdm import tqdm
except ImportError:  # tqdm — Should Have, без него просто нет прогресс-бара
    def tqdm(iterable, **kwargs):
        return iterable

# ----------------------------------------------------------------------------
# Конфигурация
# ----------------------------------------------------------------------------

SCALES = {
    "small":  dict(users=10_000,  products=1_000,  orders=80_000),
    "medium": dict(users=50_000,  products=5_000,  orders=400_000),
    "large":  dict(users=200_000, products=10_000, orders=2_000_000),
}

# Месячная сезонность (множитель спроса)
MONTH_MULT = {1: 0.95, 2: 0.85, 3: 0.95, 4: 1.0, 5: 1.0, 6: 0.95,
              7: 0.9, 8: 0.95, 9: 1.0, 10: 1.05, 11: 1.40, 12: 1.25}

# Недельная сезонность: Пн..Вс
DOW_MULT = np.array([0.92, 0.88, 0.90, 0.95, 1.08, 1.22, 1.15])

# Суточный профиль (24 часа): ночь — тишина, пик 19–22
HOUR_W = np.array([
    0.3, 0.2, 0.15, 0.1, 0.1, 0.2, 0.5, 0.9, 1.3, 1.6, 1.8, 2.0,
    2.2, 2.1, 2.0, 1.9, 2.0, 2.3, 2.8, 3.2, 3.4, 3.0, 2.2, 1.2,
])
HOUR_P = HOUR_W / HOUR_W.sum()

# Рост бизнеса за период: от 1.0 в начале до GROWTH_END в конце (линейно)
GROWTH_END = 1.35

# Воронка: целевые конверсии между ступенями
VIEW_TO_CART = 0.30      # доля просмотров, дошедших до корзины
CART_TO_PURCHASE = 0.45  # доля корзин, дошедших до покупки

ORDER_STATUSES = ["completed", "cancelled", "returned"]
STATUS_P = [0.85, 0.10, 0.05]
PAYMENT_METHODS = ["card", "sbp", "cash_on_delivery"]
PAYMENT_P = [0.45, 0.40, 0.15]
CHANNELS = ["organic", "paid_search", "social", "referral"]
CHANNEL_P = [0.40, 0.30, 0.20, 0.10]
AGE_GROUPS = ["18-24", "25-34", "35-44", "45+"]
AGE_P = [0.20, 0.40, 0.25, 0.15]

PROMO_SHARE = 0.15  # доля заказов с промокодом
PROMO_CODES = ["WELCOME10", "BLACKFRIDAY", "SUMMER5", "VIP15", "PUSH7"]

# Иерархия категорий: 5 верхнеуровневых × 4 подкатегории = 25 строк
CATEGORY_TREE = {
    "Электроника": ["Смартфоны", "Ноутбуки", "Аудио", "Аксессуары"],
    "Одежда": ["Мужская", "Женская", "Детская", "Обувь"],
    "Книги": ["Художественные", "Бизнес", "Детские книги", "Комиксы"],
    "Дом и сад": ["Кухня", "Мебель", "Декор", "Сад"],
    "Спорт": ["Тренажёры", "Одежда для спорта", "Туризм", "Питание"],
}


@dataclass
class DirtConfig:
    """Доли «грязи» от объёма соответствующей сущности (применяются при --dirty)."""
    duplicate_orders: float = 0.02       # дубликаты заказов по order_id
    null_user_in_orders: float = 0.01    # NULL user_id в заказах
    null_product_in_items: float = 0.01  # NULL product_id в позициях
    negative_quantity: float = 0.01      # отрицательное количество в позициях
    orphan_product_in_items: float = 0.01  # несуществующий product_id
    null_category_products: float = 0.02   # товары без категории
    duplicate_events: float = 0.005      # дубликаты событий


@dataclass
class Stats:
    """Счётчики для финальной сводки."""
    rows: dict = field(default_factory=dict)
    dirt: dict = field(default_factory=dict)
    gmv_by_day: dict = field(default_factory=dict)
    events_by_type: dict = field(default_factory=lambda: {"view": 0, "cart": 0, "purchase": 0})

    def add_rows(self, name: str, n: int):
        self.rows[name] = self.rows.get(name, 0) + n

    def add_dirt(self, name: str, n: int):
        self.dirt[name] = self.dirt.get(name, 0) + n


# ----------------------------------------------------------------------------
# Вспомогательные функции
# ----------------------------------------------------------------------------

def rand_uuids(n: int, rng: np.random.Generator) -> list[str]:
    """UUID v4 без uuid4() в цикле: два 63-битных числа на строку, формат через uuid.UUID.
    На ~5 млн строк работает за секунды — для наших объёмов достаточно."""
    a = rng.integers(0, 2**63, size=n, dtype=np.uint64)
    b = rng.integers(0, 2**63, size=n, dtype=np.uint64)
    return [str(uuid.UUID(int=(int(x) << 64) | int(y), version=4))
            for x, y in zip(a, b)]


def day_weights(days: list[date], rng: np.random.Generator) -> tuple[np.ndarray, date]:
    """Вес каждого дня = месячная × недельная сезонность × рост × шум.
    Возвращает нормированные веса и выбранный аномальный день (вес 0)."""
    n = len(days)
    w = np.empty(n)
    for i, d in enumerate(days):
        growth = 1.0 + (GROWTH_END - 1.0) * i / max(n - 1, 1)
        w[i] = MONTH_MULT[d.month] * DOW_MULT[d.weekday()] * growth
    w *= rng.normal(1.0, 0.07, n).clip(0.7, 1.3)  # дневной шум ±7%

    # Аномальный день: будний, в средней трети периода, чтобы был заметен
    lo, hi = n // 3, 2 * n // 3
    candidates = [i for i in range(lo, hi) if days[i].weekday() < 5]
    anomaly_idx = int(rng.choice(candidates))
    w[anomaly_idx] = 0.0
    return w / w.sum(), days[anomaly_idx]


def timestamps_for_day(d: date, n: int, rng: np.random.Generator) -> np.ndarray:
    """n меток времени внутри дня d с реалистичным суточным профилем."""
    hours = rng.choice(24, size=n, p=HOUR_P)
    seconds = hours * 3600 + rng.integers(0, 3600, size=n)
    base = np.datetime64(datetime(d.year, d.month, d.day))
    return base + seconds.astype("timedelta64[s]")


# ----------------------------------------------------------------------------
# Справочники (генерируются один раз)
# ----------------------------------------------------------------------------

def gen_categories() -> pd.DataFrame:
    rows, cid = [], 1
    pending = []
    for parent, subs in CATEGORY_TREE.items():
        rows.append({"category_id": cid, "name": parent, "parent_id": None})
        pending.extend((s, cid) for s in subs)
        cid += 1
    for name, parent_id in pending:
        rows.append({"category_id": cid, "name": name, "parent_id": parent_id})
        cid += 1
    return pd.DataFrame(rows)


def gen_users(n: int, start: date, end: date, fake: Faker,
              rng: np.random.Generator) -> pd.DataFrame:
    """Имена берутся из пула (Faker медленный на больших n),
    уникальность email гарантируется числовым суффиксом."""
    pool = min(n, 20_000)
    names = [fake.name() for _ in range(pool)]
    handles = [fake.user_name() for _ in range(pool)]
    cities = [fake.city() for _ in range(min(n, 2_000))]
    domains = ["example.com", "example.org", "example.net", "mail.example"]

    idx = rng.integers(0, pool, size=n)
    city_idx = rng.integers(0, len(cities), size=n)
    span = (end - start).days
    reg_offsets = rng.integers(0, span + 1, size=n)

    return pd.DataFrame({
        "user_id": rand_uuids(n, rng),
        "email": [f"{handles[i]}.{k}@{domains[k % 4]}" for k, i in enumerate(idx)],
        "full_name": [names[i] for i in idx],
        "registration_date": [start + timedelta(days=int(o)) for o in reg_offsets],
        "acquisition_channel": rng.choice(CHANNELS, size=n, p=CHANNEL_P),
        "city": [cities[i] for i in city_idx],
        "age_group": rng.choice(AGE_GROUPS, size=n, p=AGE_P),
        "is_active": rng.random(n) < 0.85,
    })


def gen_products(n: int, categories: pd.DataFrame, start: date, end: date,
                 fake: Faker, rng: np.random.Generator) -> pd.DataFrame:
    sub_ids = categories.loc[categories["parent_id"].notna(), "category_id"].to_numpy()
    pool = min(n, 5_000)
    names = [fake.catch_phrase() for _ in range(pool)]
    idx = rng.integers(0, pool, size=n)
    span = (end - start).days
    prices = np.round(rng.lognormal(mean=7.3, sigma=0.8, size=n), 2)  # медиана ~1500 ₽

    return pd.DataFrame({
        "product_id": rand_uuids(n, rng),
        "name": [names[i] for i in idx],
        "category_id": rng.choice(sub_ids, size=n),
        "price": prices,
        "cost_price": np.round(prices * rng.uniform(0.4, 0.7, size=n), 2),
        "rating": np.round(rng.uniform(1.0, 5.0, size=n), 2),
        "is_available": rng.random(n) < 0.92,
        "created_at": [datetime.combine(start + timedelta(days=int(o)),
                                        datetime.min.time())
                       + timedelta(seconds=int(s))
                       for o, s in zip(rng.integers(0, span + 1, size=n),
                                       rng.integers(0, 86_400, size=n))],
    })


# ----------------------------------------------------------------------------
# Факты одного дня (векторизованно)
# ----------------------------------------------------------------------------

def gen_day(d: date, n_orders: int, users_sorted: pd.DataFrame,
            reg_days: np.ndarray, propensity: np.ndarray, tau: np.ndarray,
            products: pd.DataFrame, product_p: np.ndarray,
            rng: np.random.Generator, stats: Stats):
    """Возвращает (orders, order_items, payments, events) за день d."""
    empty = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    # Покупать могут только зарегистрированные к этому дню пользователи
    k = int(np.searchsorted(reg_days, np.datetime64(d), side="right"))
    if n_orders == 0 or k == 0:
        return empty

    uid_all = users_sorted["user_id"].to_numpy()
    pid = products["product_id"].to_numpy()
    price = products["price"].to_numpy()

    # Вес пользователя сегодня = склонность × затухание от возраста регистрации.
    # Floor 0.03 — фоновая вероятность «проснуться» даже для давно остывших.
    age = (np.datetime64(d) - reg_days[:k]) / np.timedelta64(1, "D")
    user_w = propensity[:k] * (np.exp(-age / tau[:k]) + 0.03)
    user_p = user_w / user_w.sum()

    # --- orders ---------------------------------------------------------------
    order_ids = np.array(rand_uuids(n_orders, rng))
    user_idx = rng.choice(k, size=n_orders, p=user_p)
    order_ts = timestamps_for_day(d, n_orders, rng)
    statuses = rng.choice(ORDER_STATUSES, size=n_orders, p=STATUS_P)
    pay_methods = rng.choice(PAYMENT_METHODS, size=n_orders, p=PAYMENT_P)

    # --- order_items ------------------------------------------------------------
    n_items_per_order = rng.integers(1, 5, size=n_orders)  # 1–4 позиции
    total_items = int(n_items_per_order.sum())
    item_order_idx = np.repeat(np.arange(n_orders), n_items_per_order)
    item_pid_idx = rng.choice(len(pid), size=total_items, p=product_p)
    item_qty = rng.integers(1, 4, size=total_items)
    item_price = price[item_pid_idx]

    gross = np.zeros(n_orders)
    np.add.at(gross, item_order_idx, item_qty * item_price)

    # Промокоды и скидки
    has_promo = rng.random(n_orders) < PROMO_SHARE
    discount = np.where(has_promo,
                        np.round(gross * rng.uniform(0.05, 0.20, n_orders), 2), 0.0)
    total = np.round(gross - discount, 2)
    promo = np.where(has_promo, rng.choice(PROMO_CODES, size=n_orders), None)

    orders = pd.DataFrame({
        "order_id": order_ids,
        "user_id": uid_all[user_idx],
        "created_at": order_ts,
        "status": statuses,
        "payment_method": pay_methods,
        "total_amount": total,
        "discount_amount": discount,
        "promo_code": promo,
    })

    items = pd.DataFrame({
        "order_item_id": rand_uuids(total_items, rng),
        "order_id": order_ids[item_order_idx],
        "product_id": pid[item_pid_idx],
        "quantity": item_qty,
        "unit_price": item_price,
    })

    # --- payments: completed/returned — успешный платёж; cancelled — fail или нет
    pay_mask = np.ones(n_orders, dtype=bool)
    cancelled = statuses == "cancelled"
    pay_mask[cancelled] = rng.random(int(cancelled.sum())) < 0.7  # 30% отмен без платежа
    n_pay = int(pay_mask.sum())
    pay_status = np.select(
        [statuses[pay_mask] == "completed", statuses[pay_mask] == "returned"],
        ["succeeded", "refunded"], default="failed",
    )
    payments = pd.DataFrame({
        "payment_id": rand_uuids(n_pay, rng),
        "order_id": order_ids[pay_mask],
        "amount": total[pay_mask],
        "method": pay_methods[pay_mask],
        "status": pay_status,
        "processed_at": order_ts[pay_mask]
                        + rng.integers(5, 600, size=n_pay).astype("timedelta64[s]"),
    })

    # --- events: воронка view → cart → purchase --------------------------------
    # Purchase: одно событие на позицию не-отменённого заказа (товар известен),
    # session_id общий на заказ — так воронку можно считать и через windowFunnel.
    purch_order_mask = statuses != "cancelled"
    purch_item_mask = purch_order_mask[item_order_idx]
    n_purch = int(purch_item_mask.sum())

    order_sessions = np.array(rand_uuids(n_orders, rng))
    p_user = uid_all[user_idx][item_order_idx][purch_item_mask]
    p_sess = order_sessions[item_order_idx][purch_item_mask]
    p_pid = pid[item_pid_idx][purch_item_mask]
    p_ts = order_ts[item_order_idx][purch_item_mask]

    # Cart: покупки + брошенные корзины (чтобы cart→purchase ≈ CART_TO_PURCHASE)
    n_cart_extra = max(int(n_purch / CART_TO_PURCHASE) - n_purch, 0)
    c_user_idx = rng.choice(k, size=n_cart_extra, p=user_p) if n_cart_extra else np.empty(0, dtype=int)
    c_pid_idx = rng.choice(len(pid), size=n_cart_extra, p=product_p)
    cart_user = np.concatenate([p_user, uid_all[c_user_idx]])
    cart_sess = np.concatenate([p_sess, np.array(rand_uuids(max(n_cart_extra, 1), rng)[:n_cart_extra])]) \
        if n_cart_extra else p_sess.copy()
    cart_pid = np.concatenate([p_pid, pid[c_pid_idx]])
    cart_ts = np.concatenate([
        p_ts - rng.integers(120, 3600, size=n_purch).astype("timedelta64[s]"),
        timestamps_for_day(d, n_cart_extra, rng),
    ])
    n_cart = len(cart_user)

    # View: каждой корзине предшествовал просмотр + «холостые» просмотры
    n_view_extra = max(int(n_cart / VIEW_TO_CART) - n_cart, 0)
    v_user_idx = rng.choice(k, size=n_view_extra, p=user_p) if n_view_extra else np.empty(0, dtype=int)
    v_pid_idx = rng.choice(len(pid), size=n_view_extra, p=product_p)
    view_user = np.concatenate([cart_user, uid_all[v_user_idx]])
    # Холостые просмотры группируем в сессии по ~3 события
    n_vs = max(n_view_extra // 3, 1)
    extra_sess = np.array(rand_uuids(n_vs, rng))
    view_sess = np.concatenate(
        [cart_sess, extra_sess[rng.integers(0, n_vs, size=n_view_extra)]])
    view_pid = np.concatenate([cart_pid, pid[v_pid_idx]])
    view_ts = np.concatenate([
        cart_ts - rng.integers(60, 1800, size=n_cart).astype("timedelta64[s]"),
        timestamps_for_day(d, n_view_extra, rng),
    ])

    events = pd.DataFrame({
        "user_id": np.concatenate([view_user, cart_user, p_user]),
        "session_id": np.concatenate([view_sess, cart_sess, p_sess]),
        "product_id": np.concatenate([view_pid, cart_pid, p_pid]),
        "event_type": np.concatenate([
            np.full(len(view_user), "view"),
            np.full(n_cart, "cart"),
            np.full(n_purch, "purchase"),
        ]),
        "created_at": np.concatenate([view_ts, cart_ts, p_ts]),
    })
    # Вычитание интервалов могло увести время во вчера — прижимаем к началу дня
    day_start = np.datetime64(datetime(d.year, d.month, d.day))
    events["created_at"] = events["created_at"].clip(lower=day_start)
    events.sort_values("created_at", inplace=True, kind="stable")
    events.reset_index(drop=True, inplace=True)

    stats.events_by_type["view"] += len(view_user)
    stats.events_by_type["cart"] += n_cart
    stats.events_by_type["purchase"] += n_purch
    completed = statuses == "completed"
    stats.gmv_by_day[d.isoformat()] = float(total[completed].sum())

    return orders, items, payments, events


# ----------------------------------------------------------------------------
# «Грязь»
# ----------------------------------------------------------------------------

def apply_dirt_day(orders: pd.DataFrame, items: pd.DataFrame, events: pd.DataFrame,
                   dirt: DirtConfig, rng: np.random.Generator, stats: Stats):
    """Портит дневные фреймы. Возвращает изменённые копии."""
    if len(orders) == 0:
        return orders, items, events

    # Дубликаты заказов: тот же order_id, время сдвинуто на 1–30 мин (ретрай источника)
    n_dup = int(len(orders) * dirt.duplicate_orders)
    if n_dup:
        dup = orders.sample(n=n_dup, random_state=int(rng.integers(0, 2**31))).copy()
        dup["created_at"] = dup["created_at"] + pd.to_timedelta(
            rng.integers(60, 1800, size=n_dup), unit="s")
        orders = pd.concat([orders, dup], ignore_index=True)
        stats.add_dirt("orders: дубликаты order_id", n_dup)

    # NULL user_id (гостевой заказ / потерянный идентификатор)
    n_null_u = int(len(orders) * dirt.null_user_in_orders)
    if n_null_u:
        idx = rng.choice(orders.index, size=n_null_u, replace=False)
        orders.loc[idx, "user_id"] = None
        stats.add_dirt("orders: NULL user_id", n_null_u)

    # NULL product_id в позициях
    n_null_p = int(len(items) * dirt.null_product_in_items)
    if n_null_p:
        idx = rng.choice(items.index, size=n_null_p, replace=False)
        items.loc[idx, "product_id"] = None
        stats.add_dirt("order_items: NULL product_id", n_null_p)

    # Отрицательные количества (ошибка интеграции)
    n_neg = int(len(items) * dirt.negative_quantity)
    if n_neg:
        idx = rng.choice(items.index, size=n_neg, replace=False)
        items.loc[idx, "quantity"] *= -1
        stats.add_dirt("order_items: количество < 0", n_neg)

    # Сироты: product_id, которого нет в справочнике
    n_orph = int(len(items) * dirt.orphan_product_in_items)
    if n_orph:
        idx = rng.choice(items.index, size=n_orph, replace=False)
        items.loc[idx, "product_id"] = rand_uuids(n_orph, rng)
        stats.add_dirt("order_items: product_id-сирота", n_orph)

    # Дубликаты событий (повторная доставка из очереди)
    n_dup_e = int(len(events) * dirt.duplicate_events)
    if n_dup_e:
        dup = events.sample(n=n_dup_e, random_state=int(rng.integers(0, 2**31)))
        events = pd.concat([events, dup], ignore_index=True)
        stats.add_dirt("events: дубликаты", n_dup_e)

    return orders, items, events


# ----------------------------------------------------------------------------
# Запись
# ----------------------------------------------------------------------------

class Writer:
    """Два режима: монолитные CSV (как в v1) или партиции по дням под Airflow."""

    def __init__(self, out_dir: Path, partitioned: bool):
        self.out = out_dir
        self.partitioned = partitioned
        self.out.mkdir(parents=True, exist_ok=True)
        self._monolith_started: set[str] = set()

    def write_dimension(self, name: str, df: pd.DataFrame):
        """categories — всегда одним файлом. users/products в партиционном режиме
        нарезаются по дате появления (день регистрации / добавления товара)."""
        if not self.partitioned or name == "categories":
            df.to_csv(self.out / f"{name}.csv", index=False)
            return
        key = "registration_date" if name == "users" else "created_at"
        ds = pd.to_datetime(df[key]).dt.date
        for day, chunk in df.groupby(ds):
            p = self.out / day.isoformat()
            p.mkdir(exist_ok=True)
            chunk.to_csv(p / f"{name}.csv", index=False)
            self._update_manifest(p, {name: len(chunk)})

    @staticmethod
    def _update_manifest(p: Path, counts: dict):
        mpath = p / "manifest.json"
        existing = json.loads(mpath.read_text()) if mpath.exists() else {}
        existing.update(counts)
        mpath.write_text(json.dumps(existing, ensure_ascii=False, indent=2))

    def write_day(self, d: date, frames: dict[str, pd.DataFrame]):
        if self.partitioned:
            p = self.out / d.isoformat()
            manifest = {}
            for name, df in frames.items():
                if len(df) == 0:
                    continue
                p.mkdir(exist_ok=True)
                df.to_csv(p / f"{name}.csv", index=False)
                manifest[name] = len(df)
            if not manifest:
                return
            self._update_manifest(p, manifest)
        else:
            for name, df in frames.items():
                if len(df) == 0:
                    continue
                path = self.out / f"{name}.csv"
                first = name not in self._monolith_started
                df.to_csv(path, index=False, mode="w" if first else "a", header=first)
                self._monolith_started.add(name)


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Генератор данных интернет-магазина v2")
    p.add_argument("--scale", choices=SCALES, default="small")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", type=Path, default=Path("./data"))
    p.add_argument("--months", type=int, default=18, help="Глубина истории в месяцах")
    p.add_argument("--dirty", action="store_true",
                   help="Включить «грязные» данные (дубликаты, NULL, сироты)")
    p.add_argument("--partition-by-day", action="store_true",
                   help="Писать партиции <output>/<YYYY-MM-DD>/*.csv + manifest.json")
    return p


def main() -> None:
    args = build_parser().parse_args()
    cfg = SCALES[args.scale]
    rng = np.random.default_rng(args.seed)
    fake = Faker("ru_RU")
    Faker.seed(args.seed)
    t0 = time.time()
    stats = Stats()
    dirt = DirtConfig()

    end = date.today()
    start = end - timedelta(days=args.months * 30)
    reg_start = start - timedelta(days=60)  # стартовая когорта появляется чуть раньше
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    print(f"Scale={args.scale}  seed={args.seed}  период {start} … {end} "
          f"({len(days)} дней)  dirty={args.dirty}  partitioned={args.partition_by_day}")

    # --- справочники ------------------------------------------------------------
    categories = gen_categories()
    users = gen_users(cfg["users"], reg_start, end, fake, rng)
    products = gen_products(cfg["products"], categories, reg_start, end, fake, rng)

    if args.dirty:
        n_nc = int(len(products) * dirt.null_category_products)
        idx = rng.choice(products.index, size=n_nc, replace=False)
        products.loc[idx, "category_id"] = None
        # после NaN колонка стала float — возвращаем nullable int для чистого CSV
        products["category_id"] = products["category_id"].astype("Int64")
        stats.add_dirt("products: NULL category_id", n_nc)

    # Пользователи, отсортированные по дате регистрации — для выборки «кто уже есть»
    users_sorted = users.sort_values("registration_date").reset_index(drop=True)
    reg_days = pd.to_datetime(users_sorted["registration_date"]).to_numpy()

    # --- Поведенческая модель пользователя (Монте-Карло «снизу») -----------------
    # У каждого пользователя два персистентных свойства, разыгранных один раз:
    #  1) propensity — базовая склонность к покупке (гамма-распределение):
    #     появляются «киты» и одноразовые покупатели, число заказов на
    #     пользователя получает реалистичный тяжёлый правый хвост;
    #  2) tau — характерное время затухания активности после регистрации.
    #     ~25% пользователей «лояльные» (tau=400 дней, почти не затухают),
    #     остальные остывают с tau=60 дней. Вместе с floor=0.03 это даёт
    #     честные retention-кривые: пик в M+0, спад к M+1..M+2, длинный хвост.
    propensity = rng.gamma(1.2, 1.0, size=len(users_sorted))
    loyal = rng.random(len(users_sorted)) < 0.25
    tau = np.where(loyal, 400.0, 60.0)

    # Популярность товаров: степенное распределение (немногие товары — хиты)
    pop = rng.pareto(1.5, size=len(products)) + 1
    product_p = pop / pop.sum()

    # --- распределение заказов по дням -------------------------------------------
    w, anomaly_day = day_weights(days, rng)
    orders_per_day = rng.multinomial(cfg["orders"], w)
    print(f"Аномальный день (нулевые продажи): {anomaly_day}")

    writer = Writer(args.output, args.partition_by_day)
    writer.write_dimension("categories", categories)
    writer.write_dimension("users", users)
    writer.write_dimension("products", products)
    stats.add_rows("categories", len(categories))
    stats.add_rows("users", len(users))
    stats.add_rows("products", len(products))

    # --- факты по дням ------------------------------------------------------------
    for i, d in enumerate(tqdm(days, desc="Генерация дней", unit="день")):
        orders, items, payments, events = gen_day(
            d, int(orders_per_day[i]), users_sorted, reg_days,
            propensity, tau, products, product_p, rng, stats)
        if args.dirty:
            orders, items, events = apply_dirt_day(orders, items, events,
                                                   dirt, rng, stats)
        writer.write_day(d, {"orders": orders, "order_items": items,
                             "payments": payments, "events": events})
        for name, df in [("orders", orders), ("order_items", items),
                         ("payments", payments), ("events", events)]:
            stats.add_rows(name, len(df))

    # --- сводка --------------------------------------------------------------------
    el = time.time() - t0
    print(f"\nГотово за {el:.1f} c. Строк по сущностям:")
    for name, n in stats.rows.items():
        print(f"  {name:12s} {n:>12,}".replace(",", " "))
    ev = stats.events_by_type
    if ev["view"]:
        print(f"\nВоронка: view={ev['view']:,} -> cart={ev['cart']:,} "
              f"({ev['cart']/ev['view']:.1%}) -> purchase={ev['purchase']:,} "
              f"({ev['purchase']/ev['cart']:.1%})".replace(",", " "))
    if stats.dirt:
        print("\n«Грязь» в данных:")
        for name, n in stats.dirt.items():
            print(f"  {name}: {n}")
    print(f"\nАномальный день: {anomaly_day} (заказы и события = 0)")


if __name__ == "__main__":
    main()
__V2_FILE_EOF__

# ---- postgres/init.sql  [REPLACE] ----
if [[ -f "postgres/init.sql" ]]; then cp -p "postgres/init.sql" "$BACKUP/$(echo "postgres/init.sql" | tr "/" "_")"; fi
mkdir -p "postgres"
cat > "postgres/init.sql" << '__V2_FILE_EOF__'
-- =====================================================================
--  PostgreSQL — слоистая схема V2
--  staging  : landing-слой (bronze). Всё TEXT, без констрейнтов, UNLOGGED.
--             Принимает «грязные» данные как есть — никогда не отклоняет строку.
--  core     : чистая 3НФ (silver). PK / FK / CHECK / индексы.
--             Только транзакционные сущности модерируемого объёма.
--  dq       : data-quality слой. Карантин отклонённых строк + аудит загрузок.
--
--  ВАЖНО: высокообъёмные поведенческие events НЕ нормализуются в core —
--  они живут в staging.events и оттуда переливаются прямо в ClickHouse.
--  Загонять десятки млн append-only фактов в констрейнтную 3НФ нереалистично.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS dq;

-- =====================================================================
--  STAGING (landing): permissive, UNLOGGED для быстрой пере-загрузки.
--  Метаколонки _source_date / _loaded_at / _batch_id — трассировка партиции.
-- =====================================================================

CREATE UNLOGGED TABLE staging.categories (
    category_id  TEXT,
    name         TEXT,
    parent_id    TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.users (
    user_id              TEXT,
    email                TEXT,
    full_name            TEXT,
    registration_date    TEXT,
    acquisition_channel  TEXT,
    city                 TEXT,
    age_group            TEXT,
    is_active            TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.products (
    product_id   TEXT,
    name         TEXT,
    category_id  TEXT,
    price        TEXT,
    cost_price   TEXT,
    rating       TEXT,
    is_available TEXT,
    created_at   TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.orders (
    order_id         TEXT,
    user_id          TEXT,
    created_at       TEXT,
    status           TEXT,
    payment_method   TEXT,
    total_amount     TEXT,
    discount_amount  TEXT,
    promo_code       TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.order_items (
    order_item_id TEXT,
    order_id      TEXT,
    product_id    TEXT,
    quantity      TEXT,
    unit_price    TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.payments (
    payment_id   TEXT,
    order_id     TEXT,
    amount       TEXT,
    method       TEXT,
    status       TEXT,
    processed_at TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

CREATE UNLOGGED TABLE staging.events (
    user_id      TEXT,
    session_id   TEXT,
    product_id   TEXT,
    event_type   TEXT,
    created_at   TEXT,
    _source_date DATE,
    _loaded_at   TIMESTAMPTZ DEFAULT now(),
    _batch_id    TEXT
);

-- =====================================================================
--  CORE (clean 3НФ). Констрейнты подобраны так, чтобы ИМЕННО грязь
--  генератора отсеивалась при промоушене staging → core:
--    NULL user_id в orders            -> NOT NULL  (hard reject)
--    NULL / orphan product_id в items -> NOT NULL + FK (hard reject)
--    quantity < 0 в items             -> CHECK > 0 (hard reject)
--    дубликат order_id                -> PRIMARY KEY (hard reject)
--    товар без категории              -> category_id NULLABLE (soft flag, не reject)
-- =====================================================================

CREATE TABLE core.categories (
    category_id  INTEGER PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    parent_id    INTEGER REFERENCES core.categories(category_id)
);

CREATE TABLE core.users (
    user_id              UUID PRIMARY KEY,
    email                VARCHAR(255) NOT NULL,
    full_name            VARCHAR(255),
    registration_date    DATE NOT NULL,
    acquisition_channel  VARCHAR(50),
    city                 VARCHAR(100),
    age_group            VARCHAR(20),
    is_active            BOOLEAN DEFAULT TRUE
);

CREATE TABLE core.products (
    product_id    UUID PRIMARY KEY,
    name          VARCHAR(255),
    category_id   INTEGER REFERENCES core.categories(category_id) ON DELETE RESTRICT,  -- NULL = uncategorized (soft-flag)
    price         NUMERIC(12,2) CHECK (price >= 0),
    cost_price    NUMERIC(12,2),
    rating        NUMERIC(3,2),
    is_available  BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP
);

CREATE TABLE core.orders (
    order_id         UUID PRIMARY KEY,
    user_id          UUID NOT NULL REFERENCES core.users(user_id) ON DELETE RESTRICT,
    created_at       TIMESTAMP NOT NULL,
    status           VARCHAR(20),
    payment_method   VARCHAR(30),
    total_amount     NUMERIC(12,2) CHECK (total_amount >= 0),
    discount_amount  NUMERIC(12,2) DEFAULT 0,
    promo_code       VARCHAR(50)
);

CREATE TABLE core.order_items (
    order_item_id  UUID PRIMARY KEY,
    order_id       UUID NOT NULL REFERENCES core.orders(order_id) ON DELETE RESTRICT,
    product_id     UUID NOT NULL REFERENCES core.products(product_id) ON DELETE RESTRICT,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE core.payments (
    payment_id   UUID PRIMARY KEY,
    order_id     UUID NOT NULL UNIQUE REFERENCES core.orders(order_id) ON DELETE RESTRICT,  -- 1:1 к заказу
    amount       NUMERIC(12,2),
    method       VARCHAR(30),
    status       VARCHAR(20),
    processed_at TIMESTAMP
);

-- Индексы по FK и по датам — под аналитические выборки и проверки целостности
CREATE INDEX idx_products_category   ON core.products(category_id);
CREATE INDEX idx_products_created    ON core.products(created_at);
CREATE INDEX idx_users_reg_date      ON core.users(registration_date);
CREATE INDEX idx_orders_user_id      ON core.orders(user_id);
CREATE INDEX idx_orders_created_at   ON core.orders(created_at);
CREATE INDEX idx_items_order_id      ON core.order_items(order_id);
CREATE INDEX idx_items_product_id    ON core.order_items(product_id);
CREATE INDEX idx_payments_order_id   ON core.payments(order_id);

-- =====================================================================
--  DQ: карантин брака + аудит загрузок
-- =====================================================================

-- Каждая строка, отклонённая при промоушене staging -> core, с причиной
-- и полным исходным payload (JSONB) для разбора.
CREATE TABLE dq.rejected_rows (
    id            BIGSERIAL PRIMARY KEY,
    source_table  TEXT        NOT NULL,           -- 'orders' / 'order_items' / ...
    source_date   DATE,                            -- ds партиции
    reason        TEXT        NOT NULL,           -- 'null_user_id' / 'orphan_product' / 'negative_quantity' / 'duplicate_pk' / ...
    raw_data      JSONB,                           -- исходная строка как есть
    rejected_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_rejected_table_date ON dq.rejected_rows(source_table, source_date);
CREATE INDEX idx_rejected_reason     ON dq.rejected_rows(reason);

-- Паспорт каждого прогона DAG: сколько строк пришло / промотировано / отклонено.
-- Аналог project-run-summary: один SELECT показывает здоровье пайплайна,
-- источник для вкладки Data Quality в DataLens и для Grafana.
CREATE TABLE dq.load_audit (
    source_date       DATE        NOT NULL,        -- ds партиции
    source_table      TEXT        NOT NULL,
    run_id            TEXT,                          -- {dag_run_id} Airflow (последний прогон)
    rows_staging      BIGINT      DEFAULT 0,       -- прочитано из CSV в staging
    rows_promoted     BIGINT      DEFAULT 0,       -- доехало в core
    rows_rejected     BIGINT      DEFAULT 0,       -- ушло в карантин
    rows_manifest     BIGINT,                      -- ожидалось по manifest.json генератора
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ DEFAULT now(),
    status            VARCHAR(20) DEFAULT 'success',
    PRIMARY KEY (source_date, source_table)
);
CREATE INDEX idx_audit_date ON dq.load_audit(source_date);
__V2_FILE_EOF__

# ---- docker-compose.yml  [REPLACE] ----
if [[ -f "docker-compose.yml" ]]; then cp -p "docker-compose.yml" "$BACKUP/$(echo "docker-compose.yml" | tr "/" "_")"; fi
cat > "docker-compose.yml" << '__V2_FILE_EOF__'
# ClickHouse Data Engineering Showcase — локальный стек: PostgreSQL + Apache Airflow.
# ClickHouse живёт отдельно (кластер в Яндекс Облаке), сюда не входит.
#
# Запуск:
#   1) cp .env.example .env  &&  отредактировать пароли
#   2) echo "AIRFLOW_UID=$(id -u)" >> .env      # чтобы логи/dags не были root-only
#   3) docker compose up airflow-init           # один раз: миграции + admin-пользователь
#   4) docker compose up -d                      # поднять весь стек
#   5) UI:  http://localhost:8080  (admin / из .env)

x-airflow-common: &airflow-common
  build:
    context: ./airflow
    args:
      AIRFLOW_VERSION: ${AIRFLOW_VERSION:-2.10.5}
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${PG_USER}:${PG_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}
    # Подключение к business-БД для DAG'ов (PostgresHook conn_id = pg_shop)
    AIRFLOW_CONN_PG_SHOP: postgresql://${PG_USER}:${PG_PASSWORD}@postgres:5432/${PG_DB}
    # Где DAG ищет суточные партиции генератора
    DATA_RAW_DIR: /opt/airflow/data/raw
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./airflow/logs:/opt/airflow/logs
    - ./airflow/plugins:/opt/airflow/plugins
    - ./data:/opt/airflow/data:ro          # партиции генератора (read-only)
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:16
    container_name: showcase-postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: ${PG_DB}                 # business-БД (shop_mvp)
    ports:
      - "${PG_PORT:-5433}:5432"             # наружу 5433, внутри 5432
    volumes:
      - pg_data:/var/lib/postgresql/data
      # init-скрипты выполняются по алфавиту: сначала метабаза Airflow, потом business-схема
      - ./postgres/initdb/00_airflow_metadb.sql:/docker-entrypoint-initdb.d/00_airflow_metadb.sql:ro
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/10_business_schema.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d ${PG_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Один раз: применить миграции метабазы и создать admin-пользователя UI
  airflow-init:
    <<: *airflow-common
    container_name: showcase-airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username "${AIRFLOW_ADMIN_USER:-admin}" \
          --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
          --firstname Admin --lastname User --role Admin \
          --email admin@example.com || true
    restart: on-failure

  airflow-scheduler:
    <<: *airflow-common
    container_name: showcase-airflow-scheduler
    command: scheduler
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5

  airflow-webserver:
    <<: *airflow-common
    container_name: showcase-airflow-webserver
    command: webserver
    restart: unless-stopped
    ports:
      - "${AIRFLOW_PORT:-8080}:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  pg_data:
__V2_FILE_EOF__

# ---- .env.example  [REPLACE] ----
if [[ -f ".env.example" ]]; then cp -p ".env.example" "$BACKUP/$(echo ".env.example" | tr "/" "_")"; fi
cat > ".env.example" << '__V2_FILE_EOF__'
# ===== PostgreSQL (локальный, в docker-compose) =====
PG_HOST=localhost
PG_PORT=5433
PG_DB=shop_mvp
PG_USER=postgres
PG_PASSWORD=__CHANGE_ME__

# ===== Apache Airflow =====
AIRFLOW_VERSION=2.10.5
AIRFLOW_PORT=8080
AIRFLOW_UID=50000                 # перезапиши: echo "AIRFLOW_UID=$(id -u)" >> .env
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=__CHANGE_ME__
AIRFLOW_FERNET_KEY=               # сгенерировать: python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"

# ===== ClickHouse (кластер в Яндекс Облаке, self-hosted на VM) =====
CH_HOST=__YOUR_VM_IP__
CH_PORT=8123                      # HTTP-интерфейс (native — 9000)
CH_DB=shop_mvp
CH_USER=default
CH_PASSWORD=__CHANGE_ME__
CH_SECURE=False                   # на self-hosted VM без TLS
__V2_FILE_EOF__

# ---- .gitignore  [REPLACE] ----
if [[ -f ".gitignore" ]]; then cp -p ".gitignore" "$BACKUP/$(echo ".gitignore" | tr "/" "_")"; fi
cat > ".gitignore" << '__V2_FILE_EOF__'
.env
.venv/
__pycache__/
*.pyc
data/
airflow/logs/
airflow/plugins/__pycache__/
__V2_FILE_EOF__

# ---- Makefile  [NEW] ----
cat > "Makefile" << '__V2_FILE_EOF__'
.PHONY: gen gen-large up init down logs psql ps

# Сгенерировать суточные партиции (грязные) под Airflow
gen:
	python generate/generate.py --scale small --dirty --partition-by-day --output ./data/raw

gen-large:
	python generate/generate.py --scale large --dirty --partition-by-day --output ./data/raw

init:            ## один раз: миграции метабазы + admin-пользователь
	docker compose up airflow-init

up:              ## поднять PostgreSQL + Airflow
	docker compose up -d

down:            ## остановить стек (данные сохраняются в volume)
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

ps:
	docker compose ps

psql:            ## psql в business-БД
	docker compose exec postgres psql -U postgres -d shop_mvp
__V2_FILE_EOF__

# ---- postgres/initdb/00_airflow_metadb.sql  [NEW] ----
mkdir -p "postgres/initdb"
cat > "postgres/initdb/00_airflow_metadb.sql" << '__V2_FILE_EOF__'
-- Отдельная БД метаданных Airflow в том же инстансе Postgres.
-- Выполняется ДО business-схемы (алфавитный порядок init-скриптов).
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
__V2_FILE_EOF__

# ---- postgres/sql/promote_staging_to_core.sql  [NEW] ----
mkdir -p "postgres/sql"
cat > "postgres/sql/promote_staging_to_core.sql" << '__V2_FILE_EOF__'
-- ============ DIMS: staging -> core (с кастами типов) ============
INSERT INTO core.categories(category_id,name,parent_id)
SELECT category_id::int, name, NULLIF(parent_id,'')::int FROM staging.categories;

INSERT INTO core.users(user_id,email,full_name,registration_date,acquisition_channel,city,age_group,is_active)
SELECT user_id::uuid,email,full_name,registration_date::date,acquisition_channel,city,age_group,is_active::boolean
FROM staging.users;

INSERT INTO core.products(product_id,name,category_id,price,cost_price,rating,is_available,created_at)
SELECT product_id::uuid,name,NULLIF(category_id,'')::int,price::numeric,cost_price::numeric,
       rating::numeric,is_available::boolean,created_at::timestamp
FROM staging.products;

-- ============ ORDERS: чистим + карантиним ============
-- (1) NULL user_id -> карантин
INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'orders',_source_date,'null_user_id',to_jsonb(s)
FROM staging.orders s WHERE s.user_id IS NULL;

-- (2) дубликаты order_id (оставляем первый) -> карантин
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'orders',_source_date,'duplicate_pk',to_jsonb(r)-'rn'
FROM ranked r WHERE rn > 1;

-- (3) выжившие -> core
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO core.orders(order_id,user_id,created_at,status,payment_method,total_amount,discount_amount,promo_code)
SELECT order_id::uuid,user_id::uuid,created_at::timestamp,status,payment_method,
       total_amount::numeric,COALESCE(NULLIF(discount_amount,''),'0')::numeric,NULLIF(promo_code,'')
FROM ranked WHERE rn = 1;

-- ============ ORDER_ITEMS: классификация по приоритету причин ============
CREATE TEMP TABLE cls AS
SELECT i.*,
  CASE
    WHEN i.product_id IS NULL                THEN 'null_product_id'
    WHEN p.product_id IS NULL                THEN 'orphan_product'
    WHEN i.quantity::int <= 0                THEN 'negative_quantity'
    WHEN o.order_id  IS NULL                 THEN 'orphan_order'      -- позиция отклонённого заказа
    ELSE NULL
  END AS reject_reason
FROM staging.order_items i
LEFT JOIN core.products p ON i.product_id = p.product_id::text
LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text;

INSERT INTO dq.rejected_rows(source_table,source_date,reason,raw_data)
SELECT 'order_items',_source_date,reject_reason,to_jsonb(c)-'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

INSERT INTO core.order_items(order_item_id,order_id,product_id,quantity,unit_price)
SELECT order_item_id::uuid,order_id::uuid,product_id::uuid,quantity::int,unit_price::numeric
FROM cls WHERE reject_reason IS NULL;

-- ============ АУДИТ прогона ============
INSERT INTO dq.load_audit(run_id,source_date,source_table,rows_staging,rows_promoted,rows_rejected,started_at)
SELECT 'manual-test', (SELECT _source_date FROM staging.orders LIMIT 1), 'orders',
       (SELECT count(*) FROM staging.orders),(SELECT count(*) FROM core.orders),
       (SELECT count(*) FROM dq.rejected_rows WHERE source_table='orders'), now()
UNION ALL
SELECT 'manual-test', (SELECT _source_date FROM staging.order_items LIMIT 1), 'order_items',
       (SELECT count(*) FROM staging.order_items),(SELECT count(*) FROM core.order_items),
       (SELECT count(*) FROM dq.rejected_rows WHERE source_table='order_items'), now();
__V2_FILE_EOF__

# ---- airflow/Dockerfile  [NEW] ----
mkdir -p "airflow"
cat > "airflow/Dockerfile" << '__V2_FILE_EOF__'
# Базовый официальный образ Airflow + наши зависимости для DAG'ов.
# AIRFLOW_VERSION приходит build-аргументом из docker-compose.
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

# Ставим Python-зависимости с constraints самого Airflow,
# чтобы не словить конфликт версий с его собственным деревом пакетов.
ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
RUN PYV="$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"
__V2_FILE_EOF__

# ---- airflow/requirements.txt  [NEW] ----
mkdir -p "airflow"
cat > "airflow/requirements.txt" << '__V2_FILE_EOF__'
# Провайдеры и драйверы для DAG'ов (поверх базового образа apache/airflow)
apache-airflow-providers-postgres==5.13.1
clickhouse-connect==0.8.15
__V2_FILE_EOF__

# ---- airflow/dags/raw_to_pg.py  [NEW] ----
mkdir -p "airflow/dags"
cat > "airflow/dags/raw_to_pg.py" << '__V2_FILE_EOF__'
"""
DAG №1 — raw_to_pg
==================
Ежедневная инкрементальная загрузка одной суточной партиции генератора
в PostgreSQL по слоистой модели: CSV -> staging -> core (+ карантин брака).

Поток тасков:
  check_source -> load_staging -> promote_dims -> clean_facts -> data_quality -> cleanup

Особенности:
  * catchup=True + max_active_runs=1 -> backfill идёт по дням строго по порядку,
    поэтому dim'ы (users/products) успевают накопиться в core до фактов, которые
    на них ссылаются.
  * staging — транзиентный: TRUNCATE перед загрузкой и после (cleanup, all_done).
  * идемпотентность: dims через UPSERT, факты через ON CONFLICT DO NOTHING,
    карантин за ds очищается перед повторной обработкой.
  * events НЕ грузятся в PG — они идут CSV -> ClickHouse в DAG №2 (pg_to_clickhouse).

Подключение к БД: Airflow Connection `pg_shop` (задаётся через AIRFLOW_CONN_PG_SHOP
в docker-compose). Каталог партиций: env DATA_RAW_DIR (по умолчанию /opt/airflow/data/raw).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "pg_shop"
RAW_DIR = Path(os.environ.get("DATA_RAW_DIR", "/opt/airflow/data/raw"))
SQL_DIR = Path(__file__).parent / "sql"

# Сущности, которые едут в PG (events исключены сознательно — они идут прямо в CH).
# categories грузится из корневого файла, остальное — из суточной партиции.
STAGING_TABLES = ["categories", "users", "products", "orders", "order_items", "payments"]
FACT_TABLES = ["orders", "order_items", "payments"]

# Порядок колонок в CSV генератора (для COPY)
COPY_COLS = {
    "categories":  "category_id,name,parent_id",
    "users":       "user_id,email,full_name,registration_date,acquisition_channel,city,age_group,is_active",
    "products":    "product_id,name,category_id,price,cost_price,rating,is_available,created_at",
    "orders":      "order_id,user_id,created_at,status,payment_method,total_amount,discount_amount,promo_code",
    "order_items": "order_item_id,order_id,product_id,quantity,unit_price",
    "payments":    "payment_id,order_id,amount,method,status,processed_at",
}


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


@dag(
    dag_id="raw_to_pg",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # критично: сериализация backfill -> dim'ы раньше фактов
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["showcase", "postgres", "etl"],
    doc_md=__doc__,
)
def raw_to_pg():

    @task
    def check_source(ds=None) -> None:
        """Партиция за ds существует? Если нет (день без данных / аномальный день) — skip."""
        part = RAW_DIR / ds
        if not part.exists():
            raise AirflowSkipException(f"Нет партиции за {ds} — пропускаем день.")

    @task
    def load_staging(ds=None) -> dict:
        """TRUNCATE staging -> COPY суточной партиции -> проставить _source_date."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        part = RAW_DIR / ds

        hook.run("TRUNCATE " + ", ".join(f"staging.{t}" for t in STAGING_TABLES) + ";")

        counts: dict[str, int] = {}
        for t in STAGING_TABLES:
            # categories — единый файл в корне; остальное — внутри партиции дня
            csv_path = (RAW_DIR / "categories.csv") if t == "categories" else (part / f"{t}.csv")
            if not csv_path.exists():
                counts[t] = 0
                continue
            hook.copy_expert(
                f"COPY staging.{t}({COPY_COLS[t]}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                str(csv_path),
            )
            counts[t] = hook.get_first(f"SELECT count(*) FROM staging.{t}")[0]

        for t in FACT_TABLES:
            hook.run(
                f"UPDATE staging.{t} SET _source_date = %(ds)s WHERE _source_date IS NULL",
                parameters={"ds": ds},
            )
        return counts

    @task
    def promote_dims() -> None:
        """Справочники staging -> core (UPSERT, идемпотентно)."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(_sql("promote_dims.sql"))

    @task
    def clean_facts(ds=None) -> None:
        """Факты staging -> core с карантином брака. Очищаем карантин ds перед прогоном."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run("DELETE FROM dq.rejected_rows WHERE source_date = %(ds)s", parameters={"ds": ds})
        hook.run(_sql("promote_facts.sql"))

    @task
    def data_quality(staging_counts: dict, ds=None) -> None:
        """Паспорт прогона в dq.load_audit + жёсткие ассерты целостности core."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        # Манифест генератора: эталон ожидаемого числа строк за день
        manifest = {}
        mpath = RAW_DIR / ds / "manifest.json"
        if mpath.exists():
            manifest = json.loads(mpath.read_text())

        # Аудит по каждой сущности: staging = promoted + rejected (доказанное тождество)
        for t in FACT_TABLES:
            rejected = hook.get_first(
                "SELECT count(*) FROM dq.rejected_rows WHERE source_table=%s AND source_date=%s",
                parameters=(t, ds),
            )[0]
            staged = staging_counts.get(t, 0)
            promoted = staged - rejected
            hook.run(
                """
                INSERT INTO dq.load_audit
                    (source_date, source_table, rows_staging, rows_promoted,
                     rows_rejected, rows_manifest, status)
                VALUES (%(ds)s, %(t)s, %(staged)s, %(promoted)s, %(rejected)s, %(manifest)s, 'success')
                ON CONFLICT (source_date, source_table) DO UPDATE
                   SET rows_staging  = EXCLUDED.rows_staging,
                       rows_promoted = EXCLUDED.rows_promoted,
                       rows_rejected = EXCLUDED.rows_rejected,
                       rows_manifest = EXCLUDED.rows_manifest,
                       finished_at   = now(),
                       status        = 'success';
                """,
                parameters={"ds": ds, "t": t, "staged": staged, "promoted": promoted,
                            "rejected": rejected, "manifest": manifest.get(t)},
            )

        # Ассерт 1: целостность core (FK / CHECK / NOT NULL) — всё должно быть 0
        orphan_items, orphan_prod, nonpos_qty, null_user = hook.get_first(_sql("dq_assert.sql"))
        if any((orphan_items, orphan_prod, nonpos_qty, null_user)):
            raise ValueError(
                f"DQ FAIL целостности core: orphan_items={orphan_items}, "
                f"orphan_product={orphan_prod}, nonpositive_qty={nonpos_qty}, null_user={null_user}"
            )

        # Ассерт 2: потеря данных — в staging должно прийти не меньше, чем в манифесте
        for t in FACT_TABLES:
            expected = manifest.get(t)
            if expected is not None and staging_counts.get(t, 0) < expected:
                raise ValueError(
                    f"DQ FAIL потери данных в {t}: staging={staging_counts.get(t, 0)} < manifest={expected}"
                )

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        """staging транзиентный — чистим после прогона независимо от исхода."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(
            "TRUNCATE " + ", ".join(f"staging.{t}" for t in STAGING_TABLES) + ";"
        )

    counts = load_staging()
    dq = data_quality(counts)
    check_source() >> counts >> promote_dims() >> clean_facts() >> dq >> cleanup()


raw_to_pg()
__V2_FILE_EOF__

# ---- airflow/dags/sql/promote_dims.sql  [NEW] ----
mkdir -p "airflow/dags/sql"
cat > "airflow/dags/sql/promote_dims.sql" << '__V2_FILE_EOF__'
-- Справочники staging -> core с UPSERT: повторный прогон дня не ломается,
-- dim'ы накапливаются инкрементально (новые users/products каждого дня).

INSERT INTO core.categories(category_id, name, parent_id)
SELECT category_id::int, name, NULLIF(parent_id,'')::int
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
SELECT product_id::uuid, name, NULLIF(category_id,'')::int, price::numeric,
       cost_price::numeric, rating::numeric, is_available::boolean, created_at::timestamp
FROM staging.products
ON CONFLICT (product_id) DO UPDATE
   SET price = EXCLUDED.price,
       rating = EXCLUDED.rating,
       is_available = EXCLUDED.is_available;
__V2_FILE_EOF__

# ---- airflow/dags/sql/promote_facts.sql  [NEW] ----
mkdir -p "airflow/dags/sql"
cat > "airflow/dags/sql/promote_facts.sql" << '__V2_FILE_EOF__'
-- Факты staging -> core с маршрутизацией брака в dq.rejected_rows.
-- Карантин за этот ds очищается DAG'ом ДО запуска этого скрипта (идемпотентность).

-- ============ ORDERS ============
-- (1) NULL user_id -> карантин
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'orders', _source_date, 'null_user_id', to_jsonb(s)
FROM staging.orders s WHERE s.user_id IS NULL;

-- (2) дубликаты order_id (оставляем первый) -> карантин
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'orders', _source_date, 'duplicate_pk', to_jsonb(r) - 'rn'
FROM ranked r WHERE rn > 1;

-- (3) выжившие -> core
WITH ranked AS (
  SELECT s.*, row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at) rn
  FROM staging.orders s WHERE s.user_id IS NOT NULL)
INSERT INTO core.orders(order_id, user_id, created_at, status, payment_method,
                        total_amount, discount_amount, promo_code)
SELECT order_id::uuid, user_id::uuid, created_at::timestamp, status, payment_method,
       total_amount::numeric, COALESCE(NULLIF(discount_amount,''),'0')::numeric, NULLIF(promo_code,'')
FROM ranked WHERE rn = 1
ON CONFLICT (order_id) DO NOTHING;

-- ============ ORDER_ITEMS ============
WITH cls AS (
  SELECT i.*,
    CASE WHEN i.product_id IS NULL          THEN 'null_product_id'
         WHEN p.product_id IS NULL          THEN 'orphan_product'
         WHEN i.quantity::int <= 0          THEN 'negative_quantity'
         WHEN o.order_id  IS NULL           THEN 'orphan_order'
         ELSE NULL END AS reject_reason
  FROM staging.order_items i
  LEFT JOIN core.products p ON i.product_id = p.product_id::text
  LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'order_items', _source_date, reject_reason, to_jsonb(c) - 'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

WITH cls AS (
  SELECT i.*,
    CASE WHEN i.product_id IS NULL          THEN 'null_product_id'
         WHEN p.product_id IS NULL          THEN 'orphan_product'
         WHEN i.quantity::int <= 0          THEN 'negative_quantity'
         WHEN o.order_id  IS NULL           THEN 'orphan_order'
         ELSE NULL END AS reject_reason
  FROM staging.order_items i
  LEFT JOIN core.products p ON i.product_id = p.product_id::text
  LEFT JOIN core.orders   o ON i.order_id   = o.order_id::text)
INSERT INTO core.order_items(order_item_id, order_id, product_id, quantity, unit_price)
SELECT order_item_id::uuid, order_id::uuid, product_id::uuid, quantity::int, unit_price::numeric
FROM cls WHERE reject_reason IS NULL
ON CONFLICT (order_item_id) DO NOTHING;

-- ============ PAYMENTS ============ (платёж отклонённого заказа -> карантин)
WITH cls AS (
  SELECT s.*, CASE WHEN o.order_id IS NULL THEN 'orphan_order' ELSE NULL END AS reject_reason
  FROM staging.payments s LEFT JOIN core.orders o ON s.order_id = o.order_id::text)
INSERT INTO dq.rejected_rows(source_table, source_date, reason, raw_data)
SELECT 'payments', _source_date, reject_reason, to_jsonb(c) - 'reject_reason'
FROM cls c WHERE reject_reason IS NOT NULL;

WITH cls AS (
  SELECT s.*, CASE WHEN o.order_id IS NULL THEN 'orphan_order' ELSE NULL END AS reject_reason
  FROM staging.payments s LEFT JOIN core.orders o ON s.order_id = o.order_id::text)
INSERT INTO core.payments(payment_id, order_id, amount, method, status, processed_at)
SELECT payment_id::uuid, order_id::uuid, amount::numeric, method, status, processed_at::timestamp
FROM cls WHERE reject_reason IS NULL
ON CONFLICT (payment_id) DO NOTHING;
__V2_FILE_EOF__

# ---- airflow/dags/sql/dq_assert.sql  [NEW] ----
mkdir -p "airflow/dags/sql"
cat > "airflow/dags/sql/dq_assert.sql" << '__V2_FILE_EOF__'
-- После загрузки core обязан быть целостным. Все четыре числа = 0, иначе DAG падает.
SELECT
  (SELECT count(*) FROM core.order_items i LEFT JOIN core.orders   o USING(order_id)   WHERE o.order_id   IS NULL) AS orphan_order_items,
  (SELECT count(*) FROM core.order_items i LEFT JOIN core.products p USING(product_id) WHERE p.product_id IS NULL) AS orphan_product_items,
  (SELECT count(*) FROM core.order_items WHERE quantity <= 0)                                                       AS nonpositive_qty,
  (SELECT count(*) FROM core.orders WHERE user_id IS NULL)                                                          AS null_user_orders;
__V2_FILE_EOF__

# ---- docs/REPORT.md  [NEW] ----
mkdir -p "docs"
cat > "docs/REPORT.md" << '__V2_FILE_EOF__'
# ClickHouse Data Engineering Showcase — пояснительная записка

> Версия: v2 (в работе) · Дата: 12.06.2026 · Автор: <ФИО>
> Репозиторий: `clickhouse-showcase`, ветка `main`

---

## 1. Тема проекта

Сквозной демонстрационный data-пайплайн интернет-магазина:
**генерация синтетических данных → PostgreSQL (3НФ, операционный слой) → ClickHouse (денормализованный аналитический слой) → дашборд Yandex DataLens**, с оркестрацией загрузки через Apache Airflow и мониторингом пайплайна в Grafana (фаза v2).

Предметная область — маркетплейс: пользователи, товары, заказы, позиции заказов, платежи и поведенческие события (просмотр → корзина → покупка). Легенда выбрана за универсальную понятность: сущности «пользователь / товар / заказ» не требуют объяснений слушателю без технического бэкграунда.

## 2. Цель и задачи

**Цель** — продемонстрировать и закрепить компетенции в области Data Engineering и администрирования аналитической инфраструктуры: проектирование схем хранения (нормализованной и денормализованной), построение ETL/ELT-процессов, развёртывание и сопровождение ClickHouse-кластера в Яндекс Облаке, оркестрация и контроль качества данных.

**Задачи:**

1. Реализовать воспроизводимый генератор реалистичных синтетических данных с управляемыми статистическими свойствами (сезонность, поведенческая модель пользователя, контролируемая доля «грязных» записей).
2. Спроектировать схему PostgreSQL в 3НФ и процесс инкрементальной загрузки сырых данных с карантином брака.
3. Развернуть собственный кластер ClickHouse в Яндекс Облаке и реализовать ETL-переливку PostgreSQL → ClickHouse в денормализованные витрины.
4. Оркестровать пайплайн в Apache Airflow (ежедневные DAG'и, backfill исторических данных, data quality проверки).
5. Реализовать набор аналитических SQL-запросов, использующих специфичные возможности ClickHouse (`windowFunnel`, `retention`, approx-агрегаты).
6. Построить публичный аналитический дашборд в DataLens и operations-дашборд пайплайна в Grafana.

## 3. Основная часть

### 3.1 Архитектура решения (целевая, v2)

```
generate.py ──CSV по дням──▶ Airflow DAG №1 ──▶ PostgreSQL (3НФ + карантин)
                                                    │
                                              Airflow DAG №2
                                                    ▼
                                       ClickHouse (Яндекс Облако)
                                        факты + витрины + MV
                                          │               │
                                          ▼               ▼
                                      DataLens         Grafana
                                   (бизнес-аналитика) (мониторинг ETL)
```

Разделы 3.2+ (PostgreSQL, Airflow, ClickHouse, дашборды) дополняются по мере реализации соответствующих этапов.

---

## 4. Исходные данные. Генератор данных

Источником данных служит собственный генератор [`generate/generate.py`](../generate/generate.py) — Python-скрипт (~620 строк, numpy + pandas + Faker), создающий согласованный набор из семи сущностей: `categories`, `users`, `products`, `orders`, `order_items`, `payments`, `events`.

### 4.1 Интерфейс и режимы запуска

```bash
# монолитные CSV (совместимо с load.py из МВП)
python generate.py --scale small --seed 42 --output ./data

# партиции по дням + «грязные» данные — режим для Airflow
python generate.py --scale medium --dirty --partition-by-day --output ./data/raw
```

Масштабы заданы таблицей [`SCALES`](../generate/generate.py#L50):

| scale | users | products | orders | events (факт.) |
|---|---|---|---|---|
| small | 10 000 | 1 000 | 80 000 | ~1.9 млн |
| medium | 50 000 | 5 000 | 400 000 | ~9.5 млн |
| large | 200 000 | 10 000 | 2 000 000 | ~48 млн |

Воспроизводимость: все источники случайности подчинены одному сиду (`--seed`); два запуска с одинаковыми параметрами дают **побайтово идентичные файлы** (проверено сравнением md5).

### 4.2 Статистическая модель данных (метод Монте-Карло)

Генератор построен по гибридной схеме Монте-Карло: целевые объёмы фиксируются «сверху», а структура данных разыгрывается «снизу» из вероятностных распределений.

**Спрос и сезонность.** Суммарное число заказов раскладывается по дням мультиномиальным распределением, где вес дня ([`day_weights`](../generate/generate.py#L139)) — произведение четырёх факторов: месячной сезонности ([`MONTH_MULT`](../generate/generate.py#L57): ноябрь ×1.40 — Black Friday, декабрь ×1.25, февраль ×0.85), недельной ([`DOW_MULT`](../generate/generate.py#L61): пик в субботу), линейного роста бизнеса +35% за период ([`GROWTH_END`](../generate/generate.py#L71)) и нормального шума ±7%. Внутри дня метки времени следуют суточному профилю ([`HOUR_W`](../generate/generate.py#L64)) с вечерним пиком 19–22 ч. Один будний день в средней трети периода получает вес 0 — **аномальный день** с полным отвалом продаж (имитация инцидента; при `seed=42` это 2025‑07‑30).

![GMV по дням: сезонность и аномальный день](img/gmv_seasonality.png)

**Поведенческая модель пользователя.** Покупатель дня выбирается не равномерно, а пропорционально индивидуальному весу ([строка 256](../generate/generate.py#L256)):

```
w(user, t) = propensity × ( e^(−age/τ) + 0.03 )
```

где `propensity ~ Gamma(1.2, 1.0)` — постоянная склонность к покупке ([строка 566](../generate/generate.py#L566)), `age` — дней с момента регистрации, а τ задаёт остывание интереса: у ~25% «лояльных» пользователей τ = 400 дней, у остальных τ = 60; слагаемое 0.03 — фоновая вероятность вернуться. Эта модель даёт два свойства, отличающих данные от «равномерной» синтетики:

- тяжёлый правый хвост числа заказов на покупателя (медиана 5, p99 = 63, максимум 204 — есть «киты»);
- реалистичные retention-кривые: спад от ~70% в M+1 до ~27% в M+6 с плато лояльного ядра — когортная матрица на дашборде выглядит как у живого продукта.

![Когортные retention-кривые](img/retention_curves.png)

**Цены и популярность товаров.** Цены — логнормальное распределение `LogN(7.3, 0.8)` (медиана ≈ 1500 ₽, [`gen_products`](../generate/generate.py#L209)); себестоимость 40–70% цены. Популярность товаров — распределение Парето (α = 1.5): небольшая доля товаров-«хитов» собирает значимую долю продаж.

**Согласованность сущностей.** Сумма заказа вычисляется из его позиций (`total = Σ qty×price − discount`); промокод есть у ~15% заказов ([`PROMO_SHARE`](../generate/generate.py#L86)) со скидкой 5–20%. Платёж ([строка 303](../generate/generate.py#L303)) связан с заказом 1:1 и статусно согласован: `completed → succeeded`, `returned → refunded`, `cancelled → failed` либо без платежа (30%). Проверка на сгенерированных данных: тождество суммы выполняется в 100% строк.

**Воронка событий.** События строятся причинно, от покупки вверх ([строки 322–360](../generate/generate.py#L322)): на каждую позицию неотменённого заказа создаётся `purchase`-событие (общий `session_id` на заказ), к ним достраиваются брошенные корзины до целевой конверсии cart→purchase = 45% ([строка 336](../generate/generate.py#L336)) и «холостые» просмотры до view→cart = 30% ([строка 350](../generate/generate.py#L350)). Временные метки упорядочены: view → cart (через 1–30 мин) → purchase (через 2–60 мин). Фактические конверсии в сгенерированных данных совпадают с целевыми с точностью до 0.1 п.п., что делает осмысленным расчёт воронки в ClickHouse через `windowFunnel()` по `session_id`.

### 4.3 «Грязные» данные

Флаг `--dirty` включает контролируемую порчу данных ([`DirtConfig`](../generate/generate.py#L100), [`apply_dirt_day`](../generate/generate.py#L395)) — имитацию дефектов реальных источников, которые обязан перехватывать ETL-слой:

| Дефект | Доля | Легенда |
|---|---|---|
| Дубликаты заказов (тот же `order_id`, время +1–30 мин) | 2% | Ретрай интеграции / двойной клик |
| `NULL user_id` в заказах | 1% | Гостевой заказ, потерянный идентификатор |
| `NULL product_id` в позициях | 1% | Брак выгрузки источника |
| Отрицательное `quantity` | 1% | Возвраты, записанные в ту же таблицу |
| `product_id`-сирота (нет в справочнике) | 1% | Рассинхронизация справочника |
| Товары без категории | 2% | Незаполненная карточка |
| Дубликаты событий | 0.5% | Повторная доставка из очереди |

Все объёмы внесённой грязи печатаются в итоговой сводке запуска — это эталон для проверки полноты карантина на этапе DQ.

### 4.4 Партиционирование под Airflow

Режим `--partition-by-day` ([`Writer`](../generate/generate.py#L452)) раскладывает данные в структуру «один день — одна партиция»:

```
data/raw/
├── categories.csv                  # справочник целиком
├── 2025-07-29/
│   ├── users.csv                   # пользователи, зарегистрированные в этот день
│   ├── products.csv                # товары, добавленные в этот день
│   ├── orders.csv / order_items.csv / payments.csv / events.csv
│   └── manifest.json               # контрольные счётчики строк по каждому файлу
└── 2025-07-31/ ...                 # 2025-07-30 отсутствует — аномальный день
```

Справочники нарезаются по дате появления записи (день регистрации пользователя / добавления товара), то есть растут инкрементально — это создаёт естественный поток обновлений для `ReplacingMergeTree` в ClickHouse. [`manifest.json`](../generate/generate.py#L476) фиксирует ожидаемое число строк по каждой сущности дня и служит контрольной суммой для data quality шага DAG'а: расхождение «загружено в PG» против манифеста — сигнал потери данных.

### 4.5 Производительность и итоги тестовых прогонов

| Метрика | Значение |
|---|---|
| Время генерации, scale=small | ~23 с (лимит ТЗ — 120 с) |
| Сезонность (заказов в месяц) | ~4 000 базово → 6 193 в ноябре → 3 070 в феврале |
| Аномальный день | 0 заказов и 0 событий |
| Воронка view→cart→purchase | 30.0% / 45.0% (= целевым) |
| Медиана цены товара | 1 490 ₽ (цель ~1 500 ₽) |
| Тождество `total = Σ items − discount` | 100% заказов |
| Воспроизводимость по seed | md5 файлов идентичны между запусками |

Ключевое решение по производительности: генерация фактов полностью векторизована (numpy) и идёт дневными батчами; Faker используется только для пулов имён/городов справочников. Это даёт линейное масштабирование: large (~2 млн заказов, ~48 млн событий) генерируется за ~10–15 минут при постоянном потреблении памяти (события пишутся на диск по дням, а не копятся в RAM).

---

## 5. Слой PostgreSQL: трёхслойная архитектура загрузки

Загрузка в PostgreSQL построена по слоистой модели (схема [`postgres/init.sql`](../postgres/init.sql)): сырые CSV проходят путь **landing → 3НФ → карантин**, а не грузятся напрямую в нормализованные таблицы. Это вынужденное и осознанное решение: генератор намеренно отдаёт «грязные» данные (NULL в ключах, дубликаты PK, отрицательные количества, сироты FK), которые физически не вставятся в таблицы с `PRIMARY KEY` и `FOREIGN KEY` — прямая загрузка просто упала бы. Слоистость превращает этот конфликт в управляемый процесс очистки с прослеживаемостью брака.

### 5.1 Три схемы

| Схема | Роль | Типы / констрейнты | Аналогия |
|---|---|---|---|
| [`staging`](../postgres/init.sql#L14) | Landing-слой: принимает CSV как есть, никогда не отклоняет строку | Все колонки `TEXT`, без констрейнтов, таблицы `UNLOGGED` | bronze |
| [`core`](../postgres/init.sql#L15) | Чистая 3НФ: транзакционные сущности модерируемого объёма | `PK` / `FK` / `CHECK` / индексы | silver / core |
| [`dq`](../postgres/init.sql#L16) | Data quality: карантин отклонённых строк + аудит прогонов | — | observability |

**Staging — `UNLOGGED`.** Таблицы landing-слоя объявлены `UNLOGGED` ([`init.sql`](../postgres/init.sql#L23)): они не пишут WAL, что ускоряет массовую загрузку, а durability им не нужна — staging пересоздаётся из исходных CSV при каждом прогоне. Метаколонки `_source_date` / `_loaded_at` / `_batch_id` обеспечивают трассировку партиции, из которой пришла строка.

**Решение по events.** Высокообъёмные поведенческие события (`events`, на large-scale ~48 млн строк) **не нормализуются в `core`**. Они остаются в [`staging.events`](../postgres/init.sql#L97) и оттуда переливаются прямо в ClickHouse. Загонять десятки миллионов append-only фактов в констрейнтную 3НФ нереалистично и убивает производительность загрузки; в `core` живут только транзакционные сущности, которым нужны целостность и точечные обновления (это тот же принцип разделения OLTP-сущностей и raw-фактов, что применяется в зрелых ClickHouse-проектах).

### 5.2 Констрейнты core как контракт качества

Констрейнты в `core` подобраны так, чтобы **именно дефекты генератора** отсеивались при промоушене `staging → core`. Каждому типу грязи соответствует свой механизм отбраковки:

| Дефект в данных | Механизм core | Действие |
|---|---|---|
| `NULL user_id` в заказах | [`core.orders.user_id NOT NULL`](../postgres/init.sql#L146) | hard reject → карантин |
| `NULL` / сирота `product_id` в позициях | [`core.order_items.product_id NOT NULL + FK`](../postgres/init.sql#L157) | hard reject → карантин |
| `quantity < 0` в позициях | `CHECK (quantity > 0)` | hard reject → карантин |
| Дубликат `order_id` | `PRIMARY KEY` | hard reject → карантин (остаётся первая строка) |
| Позиция отклонённого заказа | `FK → core.orders` | hard reject → карантин (`orphan_order`, каскад) |
| Товар без категории | `category_id` NULLABLE | soft flag: загружается, помечается в DQ |

Деление на hard-reject и soft-flag принципиально: отсутствие категории — это не повод терять товар (в реальных каталогах есть некатегоризированные позиции), поэтому такая строка попадает в `core` и лишь учитывается как DQ-метрика, а не отбрасывается.

### 5.3 DQ-слой: карантин и паспорт прогона

Таблица [`dq.rejected_rows`](../postgres/init.sql#L190) хранит каждую отклонённую строку с причиной (`reason`) и полным исходным payload в `JSONB` — это позволяет разобрать любой инцидент постфактум и питает вкладку Data Quality в дашборде. Таблица [`dq.load_audit`](../postgres/init.sql#L204) — паспорт каждого прогона: сколько строк пришло в staging, доехало в core и ушло в карантин по каждой сущности и партиции. Один `SELECT` из неё показывает здоровье всего пайплайна (аналог project-run-summary), и она же служит источником для Grafana.

### 5.4 Проверка на реальном PostgreSQL

Схема и логика промоушена проверены на живом PostgreSQL 16 на грязной партиции (`scale=small`, один день, 4 067 заказов / 10 035 позиций). Результаты:

- **Staging принял всю грязь** без потерь (98 строк с NULL product_id, 100 с отрицательным количеством легли в `staging.order_items`).
- **Промоушен отбраковал ровно дефектные строки** с разбивкой по причинам: `null_user_id`, `duplicate_pk`, `null_product_id`, `orphan_product`, `negative_quantity`, `orphan_order`.
- **Сходимость подтверждена:** `staging = promoted + rejected` для каждой сущности (orders: 4067 = 3949 + 118; order_items: 10035 = 9655 + 380).
- **Целостность `core` после загрузки:** 0 сирот по FK, 0 строк с неположительным количеством — нормализованный слой чист.

| Сущность | staging | → core | → карантин |
|---|---|---|---|
| orders | 4 067 | 3 949 | 118 |
| order_items | 10 035 | 9 655 | 380 |

Логика очистки, проверенная здесь как единый SQL-скрипт, на следующем этапе оборачивается в Airflow-таск `clean_transform` без изменения сути.

## 6. Оркестрация Apache Airflow

Загрузка `CSV → PostgreSQL` оркеструется Apache Airflow (LocalExecutor) в локальном docker-compose-стеке рядом с PostgreSQL. ClickHouse в этот стек не входит — он развёрнут отдельным кластером в Яндекс Облаке. Выбран Airflow 2.x: для одномашинного демо-стенда он стабильнее и легче по ресурсам, чем 3.x с его раздельными api-server и dag-processor.

### 6.1 Топология стека

Стек поднимается одним docker-compose ([`docker-compose.yml`](../docker-compose.yml)) и состоит из четырёх сервисов:

| Сервис | Образ | Роль |
|---|---|---|
| `postgres` | postgres:16 | business-БД `shop_mvp` (staging/core/dq) **и** метабаза `airflow` в одном инстансе |
| `airflow-init` | собственный (Dockerfile) | разовый: миграции метабазы + создание admin-пользователя |
| `airflow-scheduler` | собственный | планировщик + LocalExecutor (исполняет таски) |
| `airflow-webserver` | собственный | UI на `localhost:8080` |

Метаданные Airflow хранятся отдельной базой `airflow` в том же Postgres (создаётся init-скриптом [`00_airflow_metadb.sql`](../postgres/initdb/00_airflow_metadb.sql)) — это экономит контейнер относительно официального паттерна с выделенным Postgres. Образ Airflow собирается из [`airflow/Dockerfile`](../airflow/Dockerfile): базовый `apache/airflow` + наши зависимости (`apache-airflow-providers-postgres`, `clickhouse-connect`), устанавливаемые с constraints самого Airflow во избежание конфликтов версий. Суточные партиции генератора монтируются в контейнеры read-only (`./data → /opt/airflow/data`), подключение к business-БД задаётся переменной `AIRFLOW_CONN_PG_SHOP` (Airflow Connection `pg_shop`).

### 6.2 DAG `raw_to_pg`

DAG [`airflow/dags/raw_to_pg.py`](../airflow/dags/raw_to_pg.py) написан на TaskFlow API и реализует ежедневную инкрементальную загрузку одной суточной партиции:

```
check_source → load_staging → promote_dims → clean_facts → data_quality → cleanup
```

| Таск | Что делает |
|---|---|
| `check_source` | партиция за `ds` существует? нет (день без данных / аномалия) → `AirflowSkipException` |
| `load_staging` | `TRUNCATE staging` → `COPY` CSV партиции в staging → проставить `_source_date` |
| `promote_dims` | справочники staging → core (`UPSERT`, накопление по дням) |
| `clean_facts` | факты staging → core с маршрутизацией брака в `dq.rejected_rows` |
| `data_quality` | запись паспорта в `dq.load_audit` + жёсткие ассерты целостности |
| `cleanup` | `TRUNCATE staging` (`trigger_rule=all_done`) — staging транзиентный |

Ключевые свойства, заложенные в DAG:

- **Backfill по порядку.** `catchup=True` + `max_active_runs=1` заставляют исторические прогоны идти строго по дням слева направо. Это обязательно: `dim`-сущности (users/products) должны успеть накопиться в `core` до фактов, которые на них ссылаются по FK. Команда `airflow dags backfill` переваривает все 18 месяцев истории по партициям.
- **Идемпотентность.** Повторный прогон дня не ломается и не задваивает данные: справочники грузятся через `ON CONFLICT DO UPDATE`, факты — через `ON CONFLICT DO NOTHING`, карантин за `ds` очищается перед повторной обработкой. Проверено на реальном Postgres: второй прогон даёт идентичные счётчики `core`.
- **events в обход PG.** Высокообъёмные события не грузятся в Postgres — они пойдут напрямую `CSV → ClickHouse` в DAG №2.
- **Data quality как гейт.** Таск `data_quality` не просто пишет метрики, а **роняет DAG** при нарушении целостности (`orphan FK`, отрицательные количества, NULL user в core) или при потере данных (строк в staging меньше, чем заявлено в `manifest.json` генератора). Сверка с манифестом — детектор тихой потери строк при загрузке.

### 6.3 Слой data quality на выходе

После прогона `dq.load_audit` содержит паспорт каждого дня (`rows_staging` / `rows_promoted` / `rows_rejected` / `rows_manifest`), а `dq.rejected_rows` — каждую отклонённую строку с причиной и исходным payload. На грязной партиции одного дня (4 067 заказов / 10 035 позиций) DAG-овые SQL отбраковали в карантин: 40 заказов с NULL user, 78 дубликатов, 98 позиций с NULL product, 100 сирот-товаров, 95 с отрицательным количеством, 87 позиций и 36 платежей отклонённых заказов — при нулевой ошибке целостности `core`. Эти две таблицы — источник вкладки Data Quality в DataLens и дашборда в Grafana.

### 6.4 Развёртывание

```bash
cp .env.example .env                      # отредактировать пароли
echo "AIRFLOW_UID=$(id -u)" >> .env       # права на logs/dags
make gen                                  # сгенерировать партиции в ./data/raw
docker compose up airflow-init            # разово: миграции + admin
docker compose up -d                      # поднять стек
# UI: http://localhost:8080 → включить DAG raw_to_pg → Trigger / backfill
```

## 7. Слой ClickHouse *(заполняется на этапе 3–4)*

## 8. Дашборды: DataLens и Grafana *(заполняется на этапе 5–6)*

## 9. Выводы *(заполняется по завершении)*
__V2_FILE_EOF__

touch airflow/plugins/.gitkeep

# --- 3. Итог ---
echo
echo "================ ГОТОВО ================"
echo "Заменены (старые версии в $BACKUP/):"
echo "   generate/generate.py, postgres/init.sql, docker-compose.yml, .env.example, .gitignore"
echo "Добавлены:"
echo "   Makefile, postgres/initdb/, postgres/sql/, airflow/ (Dockerfile, requirements.txt, dags/), docs/REPORT.md"
echo "Не тронуты:"
echo "   .env, clickhouse/, queries/, README.md, requirements.txt, postgres/load.py"
echo
echo "Дальше:"
echo "   1) скопировать графики docs/img/*.png (2 файла, опционально — для отчёта)"
echo "   2) сверить .env с .env.example (добавились AIRFLOW_*, выправлены CH_*)"
echo "   3) git status  &&  git add -A  &&  git commit -m "V2: layered PG + Airflow""
echo "======================================="
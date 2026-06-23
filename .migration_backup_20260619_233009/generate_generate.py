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
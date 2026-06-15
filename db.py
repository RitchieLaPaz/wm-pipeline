"""
PostgreSQL operations for the WM pipeline.
Hosted on Railway Postgres. Mode Analytics connects as a native data source.

Tables:
  wm_orders       — order summaries (from CSV + detail page)
  wm_customers    — email + phone keyed by WMID
  wm_order_items  — product line items per order

Views (ready for Mode):
  wm_orders_enriched  — orders + customer contact joined
  wm_daily_summary    — revenue by store by day
  wm_product_sales    — product/brand performance
"""

import json
import logging
import os

import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]  # auto-injected by Railway Postgres plugin


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def ensure_tables():
    """Create tables and views if they don't exist. Safe to run on every startup."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""

                CREATE TABLE IF NOT EXISTS wm_orders (
                    id                        SERIAL PRIMARY KEY,
                    order_id                  TEXT          NOT NULL UNIQUE,
                    order_uuid                TEXT,
                    order_datetime            TIMESTAMPTZ,
                    order_date                DATE,
                    status                    TEXT,
                    order_type                TEXT,
                    source                    TEXT,
                    store_name                TEXT,
                    listing_name              TEXT,
                    wmid                      TEXT,
                    customer_name             TEXT,
                    item_quantity             INTEGER,
                    order_items_total         NUMERIC(10,2),
                    excise_tax                NUMERIC(10,2),
                    sales_tax                 NUMERIC(10,2),
                    service_fee               NUMERIC(10,2),
                    delivery_fee              NUMERIC(10,2),
                    discount_total            NUMERIC(10,2),
                    gross_sales               NUMERIC(10,2),
                    payment_type              TEXT,
                    discount_code             TEXT,
                    customer_notes            TEXT,
                    last_order_note           TEXT,
                    last_delivery_note        TEXT,
                    fulfillment_status        TEXT,
                    fulfillment_status_reason TEXT,
                    dispatch_status           TEXT,
                    dispatch_status_reason    TEXT,
                    raw_data                  JSONB,
                    synced_at                 TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_wm_orders_date   ON wm_orders (order_date);
                CREATE INDEX IF NOT EXISTS idx_wm_orders_store  ON wm_orders (store_name);
                CREATE INDEX IF NOT EXISTS idx_wm_orders_wmid   ON wm_orders (wmid);
                CREATE INDEX IF NOT EXISTS idx_wm_orders_uuid   ON wm_orders (order_uuid);

                CREATE TABLE IF NOT EXISTS wm_customers (
                    id            SERIAL PRIMARY KEY,
                    wmid          TEXT        NOT NULL UNIQUE,
                    customer_name TEXT,
                    email         TEXT,
                    phone         TEXT,
                    synced_at     TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_wm_customers_wmid  ON wm_customers (wmid);
                CREATE INDEX IF NOT EXISTS idx_wm_customers_email ON wm_customers (email);

                CREATE TABLE IF NOT EXISTS wm_order_items (
                    id           SERIAL PRIMARY KEY,
                    order_uuid   TEXT,
                    order_id     TEXT,
                    product_name TEXT,
                    brand        TEXT,
                    category     TEXT,
                    weight       TEXT,
                    unit_price   NUMERIC(10,2),
                    qty          INTEGER,
                    item_total   NUMERIC(10,2),
                    synced_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (order_uuid, product_name, qty)
                );

                CREATE INDEX IF NOT EXISTS idx_wm_items_order    ON wm_order_items (order_id);
                CREATE INDEX IF NOT EXISTS idx_wm_items_product  ON wm_order_items (product_name);
                CREATE INDEX IF NOT EXISTS idx_wm_items_brand    ON wm_order_items (brand);
                CREATE INDEX IF NOT EXISTS idx_wm_items_category ON wm_order_items (category);

                CREATE OR REPLACE VIEW wm_orders_enriched AS
                    SELECT o.*, c.email, c.phone
                    FROM wm_orders o
                    LEFT JOIN wm_customers c USING (wmid)
                    ORDER BY o.order_date DESC;

                CREATE OR REPLACE VIEW wm_daily_summary AS
                    SELECT
                        order_date,
                        store_name,
                        COUNT(*)                    AS total_orders,
                        SUM(gross_sales)            AS gross_revenue,
                        SUM(order_items_total)      AS items_revenue,
                        SUM(excise_tax + sales_tax) AS total_tax,
                        SUM(discount_total)         AS total_discounts,
                        AVG(gross_sales)            AS avg_order_value
                    FROM wm_orders
                    WHERE status = 'Complete'
                    GROUP BY order_date, store_name
                    ORDER BY order_date DESC, store_name;

                CREATE OR REPLACE VIEW wm_product_sales AS
                    SELECT
                        i.product_name,
                        i.brand,
                        i.category,
                        i.weight,
                        COUNT(DISTINCT i.order_id) AS order_count,
                        SUM(i.qty)                 AS units_sold,
                        SUM(i.item_total)          AS total_revenue,
                        AVG(i.unit_price)          AS avg_unit_price,
                        MIN(o.order_date)          AS first_sold,
                        MAX(o.order_date)          AS last_sold
                    FROM wm_order_items i
                    LEFT JOIN wm_orders o USING (order_id)
                    GROUP BY i.product_name, i.brand, i.category, i.weight
                    ORDER BY total_revenue DESC;

            """)
        conn.commit()
    log.info("Tables and views verified")


def get_known_wmids() -> set:
    """Return WMIDs already enriched with email — skip re-scraping."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT wmid FROM wm_customers WHERE email IS NOT NULL")
            return {row[0] for row in cur.fetchall()}


def upsert_orders(rows: list[dict]):
    if not rows:
        return
    sql = """
        INSERT INTO wm_orders (
            order_id, order_uuid, order_datetime, order_date, status,
            order_type, source, store_name, listing_name, wmid, customer_name,
            item_quantity, order_items_total, excise_tax, sales_tax,
            service_fee, delivery_fee, discount_total, gross_sales,
            payment_type, discount_code, customer_notes, last_order_note,
            last_delivery_note, fulfillment_status, fulfillment_status_reason,
            dispatch_status, dispatch_status_reason, raw_data
        ) VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            status                    = EXCLUDED.status,
            order_uuid                = COALESCE(EXCLUDED.order_uuid, wm_orders.order_uuid),
            fulfillment_status        = EXCLUDED.fulfillment_status,
            fulfillment_status_reason = EXCLUDED.fulfillment_status_reason,
            gross_sales               = EXCLUDED.gross_sales,
            discount_total            = EXCLUDED.discount_total,
            raw_data                  = EXCLUDED.raw_data,
            synced_at                 = NOW()
    """
    values = [(
        r["order_id"], r.get("order_uuid"), r["order_datetime"], r["order_date"],
        r["status"], r["order_type"], r["source"], r["store_name"],
        r["listing_name"], r["wmid"], r["customer_name"], r["item_quantity"],
        r["order_items_total"], r["excise_tax"], r["sales_tax"],
        r["service_fee"], r["delivery_fee"], r["discount_total"],
        r["gross_sales"], r["payment_type"], r.get("discount_code"),
        r.get("customer_notes"), r.get("last_order_note"),
        r.get("last_delivery_note"), r.get("fulfillment_status"),
        r.get("fulfillment_status_reason"), r.get("dispatch_status"),
        r.get("dispatch_status_reason"), r.get("raw_data"),
    ) for r in rows]

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
    log.info(f"Upserted {len(values)} orders")


def upsert_order_uuid(order: dict):
    if not order.get("order_id") or not order.get("order_uuid"):
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wm_orders (order_id, order_uuid)
                VALUES (%s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    order_uuid = EXCLUDED.order_uuid
            """, (order["order_id"], order["order_uuid"]))
        conn.commit()


def upsert_customer(customer: dict):
    if not customer.get("wmid"):
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wm_customers (wmid, customer_name, email, phone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (wmid) DO UPDATE SET
                    customer_name = COALESCE(EXCLUDED.customer_name, wm_customers.customer_name),
                    email         = COALESCE(EXCLUDED.email,         wm_customers.email),
                    phone         = COALESCE(EXCLUDED.phone,         wm_customers.phone),
                    synced_at     = NOW()
            """, (
                customer["wmid"], customer.get("customer_name"),
                customer.get("email"), customer.get("phone"),
            ))
        conn.commit()


def upsert_order_items(items: list[dict]):
    if not items:
        return
    sql = """
        INSERT INTO wm_order_items
            (order_uuid, order_id, product_name, brand, category,
             weight, unit_price, qty, item_total)
        VALUES %s
        ON CONFLICT (order_uuid, product_name, qty) DO UPDATE SET
            brand      = EXCLUDED.brand,
            category   = EXCLUDED.category,
            unit_price = EXCLUDED.unit_price,
            item_total = EXCLUDED.item_total,
            synced_at  = NOW()
    """
    values = [(
        i["order_uuid"], i["order_id"], i["product_name"], i.get("brand"),
        i.get("category"), i.get("weight"), i["unit_price"], i["qty"], i["item_total"],
    ) for i in items]

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
    log.info(f"Upserted {len(values)} line items")

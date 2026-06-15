"""
Weedmaps Admin → PostgreSQL Pipeline
Runs daily via Railway cron.

Flow:
  1. Login at weedmaps.com/login
  2. Navigate to admin.weedmaps.com/orders → All Orders
  3. Apply date filter, collect all order UUIDs from list
  4. For each UUID → scrape detail page → email, phone, line items
  5. Skip customers already enriched in DB
  6. Upsert everything to Postgres for Mode Analytics
"""

import asyncio
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

import db

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

WM_EMAIL      = os.environ["WM_EMAIL"]
WM_PASSWORD   = os.environ["WM_PASSWORD"]
LOGIN_URL     = "https://weedmaps.com/login"
ADMIN_URL     = "https://admin.weedmaps.com"

# Date range
# Weekly cron:       leave WM_START_DATE/WM_END_DATE unset → uses DAYS_BACK=7
# One-time backfill: set WM_START_DATE=04/01/2026, WM_END_DATE=06/15/2026
_start_env = os.environ.get("WM_START_DATE")
_end_env   = os.environ.get("WM_END_DATE")

if _start_env and _end_env:
    START_DATE = datetime.strptime(_start_env, "%m/%d/%Y").date()
    END_DATE   = datetime.strptime(_end_env,   "%m/%d/%Y").date()
    DAYS_BACK  = (END_DATE - START_DATE).days + 1
else:
    DAYS_BACK  = int(os.environ.get("WM_DAYS_BACK", "7"))
    START_DATE = date.today() - timedelta(days=DAYS_BACK)
    END_DATE   = date.today() - timedelta(days=1)

MAX_ORDERS = int(os.environ.get("WM_MAX_ORDERS", "5000" if DAYS_BACK > 7 else "1000"))

UUID_PATTERN  = re.compile(
    r"/orders/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)


# ── Login ─────────────────────────────────────────────────────────────────────

async def login(page):
    log.info("Logging in at weedmaps.com/login...")
    await page.goto(LOGIN_URL, wait_until="networkidle")

    await page.get_by_role("textbox", name="Email or username").fill(WM_EMAIL)
    await page.get_by_role("textbox", name="Password").fill(WM_PASSWORD)
    await page.get_by_role("button", name="Log in").click()

    # Wait for redirect to admin portal
    await page.wait_for_url(f"{ADMIN_URL}/**", timeout=15_000)
    log.info(f"Logged in — landed at {page.url}")


# ── Step 1: Navigate to All Orders ───────────────────────────────────────────

async def go_to_orders(page):
    log.info("Navigating to All Orders...")
    await page.locator("#side-nav-orders").click()
    await page.get_by_test_id("orders-sidenav-link-active-false") \
              .get_by_role("link", name="All Orders").click()
    await page.wait_for_load_state("networkidle", timeout=10_000)
    log.info("On All Orders page")


# ── Step 2: Apply date filter ─────────────────────────────────────────────────

async def apply_date_filter(page):
    """
    Apply date range filter: START_DATE → END_DATE.
    Daily run:  yesterday → yesterday  (DAYS_BACK=1)
    Weekly run: 7 days ago → yesterday (DAYS_BACK=7)
    TODO: Update selectors if date filter isn't applying correctly.
    """
    start_str = START_DATE.strftime("%m/%d/%Y")
    end_str   = END_DATE.strftime("%m/%d/%Y")
    log.info(f"Applying date filter: {start_str} → {end_str}")

    try:
        await page.click(
            '[data-testid="date-filter"], [data-test-id="date-filter"], '
            'button:has-text("Date"), .date-range-picker',
            timeout=5_000
        )
        await page.fill('input[placeholder*="start"], input[name="start"]', start_str)
        await page.fill('input[placeholder*="end"], input[name="end"]', end_str)
        await page.click('button:has-text("Apply"), button:has-text("Search")')
        await page.wait_for_load_state("networkidle", timeout=8_000)
        log.info("Date filter applied")
    except PWTimeout:
        log.warning("Date filter not found — scraping without date filter (will rely on DB dedup)")


# ── Step 3: Collect order UUIDs from list ────────────────────────────────────

async def get_order_uuids(page) -> list[str]:
    """
    Collect all order detail UUIDs from the orders list page.
    Order links are rendered as "#46520154" but href contains the UUID.
    """
    uuids = []
    page_num = 1

    while len(uuids) < MAX_ORDERS:
        await page.wait_for_load_state("networkidle", timeout=8_000)

        # Grab all hrefs from order links (display: "#12345", href: /orders/{uuid})
        hrefs = await page.eval_on_selector_all(
            'a[href*="/orders/"]',
            "els => els.map(e => e.getAttribute('href'))"
        )

        new_found = 0
        for href in hrefs:
            m = UUID_PATTERN.search(href or "")
            if m and m.group(1) not in uuids:
                uuids.append(m.group(1))
                new_found += 1

        log.info(f"Page {page_num}: found {new_found} orders (total: {len(uuids)})")

        # Pagination
        try:
            next_btn = page.get_by_role("button", name="Next").or_(
                page.locator('[aria-label="Next page"], [data-testid="next-page"]')
            )
            if await next_btn.is_visible(timeout=2_000) and await next_btn.is_enabled():
                await next_btn.click()
                page_num += 1
                await asyncio.sleep(0.5)
            else:
                break
        except PWTimeout:
            break

    log.info(f"Collected {len(uuids)} order UUIDs")
    return uuids[:MAX_ORDERS]


# ── Step 4: Scrape order detail page ─────────────────────────────────────────

async def scrape_order_detail(page, uuid: str) -> dict | None:
    url = f"{ADMIN_URL}/orders/{uuid}"
    try:
        await page.goto(url, wait_until="networkidle", timeout=20_000)
    except PWTimeout:
        log.warning(f"Timeout loading {uuid}")
        return None

    try:
        # ── Order ID (numeric) ────────────────────────────────────────────────
        # Heading shows "Order #46520154"
        heading = await _safe_text(page, 'h1')
        order_id_match = re.search(r"#(\d+)", heading)
        order_id = order_id_match.group(1) if order_id_match else None

        # ── Customer name ─────────────────────────────────────────────────────
        customer_name = await _safe_text(page, '[data-test-id="dc-name"], h2')

        # ── Phone ─────────────────────────────────────────────────────────────
        # Phone is displayed as text near the "Copy phone number" button
        # Try tel: link first, then regex scan the page text
        phone = None
        try:
            tel_link = page.locator('a[href^="tel:"]')
            if await tel_link.is_visible(timeout=2_000):
                href = await tel_link.get_attribute("href")
                phone = href.replace("tel:", "").strip() if href else None
        except PWTimeout:
            pass

        if not phone:
            # Fallback: scan visible text for phone pattern
            body_text = await page.locator("body").text_content() or ""
            phone_match = re.search(r"\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4}", body_text)
            phone = phone_match.group(0).strip() if phone_match else None

        # ── Email ─────────────────────────────────────────────────────────────
        email = None
        try:
            mailto = page.locator('a[href^="mailto:"]')
            if await mailto.is_visible(timeout=2_000):
                href = await mailto.get_attribute("href")
                email = href.replace("mailto:", "").strip() if href else None
        except PWTimeout:
            pass

        if not email:
            # Fallback: scan for email pattern in page text
            body_text = body_text if 'body_text' in dir() else \
                        await page.locator("body").text_content() or ""
            email_match = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", body_text)
            email = email_match.group(0).strip() if email_match else None

        # ── WMID ──────────────────────────────────────────────────────────────
        # WMID label is visible on page — grab the value after it
        wmid = None
        try:
            wmid_label = page.get_by_text("WMID", exact=True)
            if await wmid_label.is_visible(timeout=2_000):
                # Get the sibling/following element with the actual WMID value
                wmid_container = page.locator(
                    '[data-test-id="wmid-value"], '
                    'text="WMID" >> xpath=following-sibling::*[1]'
                )
                wmid = await _safe_text(page, wmid_container) or None

                # Fallback: scan body for numeric WMID near the label
                if not wmid:
                    body_text = await page.locator("body").text_content() or ""
                    wmid_match = re.search(r"WMID\D{0,10}(\d{7,12})", body_text)
                    wmid = wmid_match.group(1) if wmid_match else None
        except PWTimeout:
            pass

        # ── Order line items ──────────────────────────────────────────────────
        line_items = await scrape_line_items(page, uuid, order_id)

        log.info(
            f"Order {order_id}: customer={customer_name}, "
            f"email={'✓' if email else '✗'}, "
            f"phone={'✓' if phone else '✗'}, "
            f"items={len(line_items)}"
        )

        return {
            "order":      {"order_uuid": uuid, "order_id": order_id},
            "customer":   {"wmid": wmid, "customer_name": customer_name,
                           "phone": phone, "email": email},
            "line_items": line_items,
        }

    except Exception as e:
        log.error(f"Failed scraping {uuid}: {e}")
        return None


# ── Line item scraper ─────────────────────────────────────────────────────────

async def scrape_line_items(page, uuid: str, order_id: str) -> list[dict]:
    """
    Scrape product line items from the right panel of the order detail page.
    From the screenshot: product name, brand, category, weight, unit price, qty, total.

    TODO: If items aren't parsing correctly, run codegen on the detail page
    and click each product element to capture the exact selectors.
    """
    items = []

    try:
        # Try to find item rows — common patterns for order item tables
        item_rows = await page.query_selector_all(
            '[data-test-id="order-item"], '
            '[data-testid="order-item"], '
            '.order-item-row, '
            '[class*="OrderItem"], '
            '[class*="order-item"]'
        )

        for row in item_rows:
            try:
                # Product name (e.g. "Moonland - Tropicana Cherry Pre-Roll 1g")
                product_name = await _safe_text(row,
                    '[data-test-id="product-name"], .product-name, h3, h4, strong'
                )
                # Brand (e.g. "by Moonland" → strip "by ")
                brand_raw = await _safe_text(row,
                    '[data-test-id="brand"], .brand, [class*="brand"]'
                )
                brand = brand_raw.replace("by ", "").strip() if brand_raw else None

                # Category (e.g. "Joints", "Flower")
                category = await _safe_text(row,
                    '[data-test-id="category"], .category, [class*="category"]'
                )
                # Weight (e.g. "1 g", "1.25 g")
                weight = await _safe_text(row,
                    '[data-test-id="weight"], .weight, [class*="weight"]'
                )
                # Financials
                unit_price = _parse_money(await _safe_text(row,
                    '[data-test-id="unit-price"], .unit-price, [class*="price"]'
                ))
                qty = _parse_int(await _safe_text(row,
                    '[data-test-id="qty"], .qty, [class*="quantity"]'
                ))
                item_total = _parse_money(await _safe_text(row,
                    '[data-test-id="total"], .total, [class*="total"]'
                ))

                if product_name:
                    items.append({
                        "order_uuid":   uuid,
                        "order_id":     order_id,
                        "product_name": product_name,
                        "brand":        brand,
                        "category":     category,
                        "weight":       weight or None,
                        "unit_price":   unit_price,
                        "qty":          qty or 1,
                        "item_total":   item_total,
                    })
            except Exception as e:
                log.warning(f"Skipping item row: {e}")

    except Exception as e:
        log.warning(f"Line item scrape failed for {uuid}: {e}")

    return items


# ── Utilities ─────────────────────────────────────────────────────────────────

async def _safe_text(parent, selector) -> str:
    try:
        if isinstance(selector, str):
            el = await parent.query_selector(selector)
        else:
            el = selector  # already a Locator
        if el is None:
            return ""
        text = await el.text_content()
        return (text or "").strip()
    except Exception:
        return ""

def _parse_money(val: str) -> float:
    try:
        return float(str(val).replace("$", "").replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        return 0.0

def _parse_int(val: str) -> int:
    try:
        return int(str(val).strip() or 0)
    except (ValueError, TypeError):
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info(f"=== WM Pipeline starting — {START_DATE} → {END_DATE} (DAYS_BACK={DAYS_BACK}) ==="))
    db.ensure_tables()

    known_wmids = db.get_known_wmids()
    log.info(f"{len(known_wmids)} customers already enriched — will skip")

    stats = {"orders": 0, "customers": 0, "items": 0, "skipped": 0, "errors": 0}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            await login(page)
            await go_to_orders(page)
            await apply_date_filter(page)

            uuids = await get_order_uuids(page)

            for i, uuid in enumerate(uuids, 1):
                log.info(f"[{i}/{len(uuids)}] {uuid}")

                result = await scrape_order_detail(page, uuid)
                if not result:
                    stats["errors"] += 1
                    continue

                db.upsert_order_uuid(result["order"])
                stats["orders"] += 1

                customer = result["customer"]
                wmid = customer.get("wmid")

                if wmid and wmid not in known_wmids:
                    db.upsert_customer(customer)
                    known_wmids.add(wmid)
                    stats["customers"] += 1
                else:
                    stats["skipped"] += 1

                if result["line_items"]:
                    db.upsert_order_items(result["line_items"])
                    stats["items"] += len(result["line_items"])

                await asyncio.sleep(0.75)  # polite delay

        finally:
            await browser.close()

    log.info(
        f"=== Done — orders: {stats['orders']}, "
        f"new customers: {stats['customers']}, "
        f"items: {stats['items']}, "
        f"skipped: {stats['skipped']}, "
        f"errors: {stats['errors']} ==="
    )
    if stats["errors"] > stats["orders"] / 2:
        sys.exit(1)  # fail loudly if majority errored


if __name__ == "__main__":
    asyncio.run(main())

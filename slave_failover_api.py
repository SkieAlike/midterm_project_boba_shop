import asyncio
import os
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from datetime import date, timedelta

import asyncpg
from fastapi import FastAPI, HTTPException, Query


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("boba_failover_api")


# ============================================================
# CONFIG
# ============================================================

MASTER_DSN = os.getenv(
    "MASTER_DSN",
    "postgresql://postgres:gvantsa@127.0.0.1:5432/postgres"
)
SLAVE_DSN = os.getenv(
    "SLAVE_DSN",
    "postgresql://postgres:gvantsa@127.0.0.1:5433/postgres"
)

MIN_POOL_SIZE = 1
MAX_POOL_SIZE = 20
SLAVE_RETRY_SECONDS = 15
STATEMENT_TIMEOUT_MS = 900

app = FastAPI(
    title="BobaShop Analytics API (Failover)",
    description="Analytics endpoints with automatic read failover.",
    version="1.0.0",
)


# ============================================================
# LIFESPAN STARTUP / SHUTDOWN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.master_pool = None
    app.state.slave_pool = None
    app.state.current_read = "slave"
    app.state.lock = asyncio.Lock()
    app.state.health_task = None

    async def create_pool(dsn: str, name: str):
        logger.info(f"Connecting to {name.upper()}: {dsn}")
        return await asyncpg.create_pool(
            dsn, min_size=MIN_POOL_SIZE, max_size=MAX_POOL_SIZE
        )

    async def slave_health_checker():
        logger.info("Slave health checker started")
        while True:
            try:
                pool = app.state.slave_pool
                if pool is None:
                    try:
                        app.state.slave_pool = await create_pool(SLAVE_DSN, "slave")
                        pool = app.state.slave_pool
                        logger.info("SLAVE pool created.")
                    except Exception as e:
                        logger.warning(f"SLAVE unavailable: {e}")
                        async with app.state.lock:
                            app.state.current_read = "master"
                        await asyncio.sleep(SLAVE_RETRY_SECONDS)
                        continue

                async with pool.acquire() as conn:
                    await conn.execute("SET LOCAL statement_timeout = 200")
                    await conn.fetchval("SELECT 1")

                async with app.state.lock:
                    if app.state.current_read == "master":
                        logger.info("Slave back online -> switching to SLAVE")
                        app.state.current_read = "slave"

            except Exception as e:
                logger.warning(f"Slave health check failed: {e}")
                async with app.state.lock:
                    if app.state.current_read != "master":
                        logger.info("Switching reads to MASTER")
                    app.state.current_read = "master"

            await asyncio.sleep(SLAVE_RETRY_SECONDS)

    # Startup: create master + slave
    try:
        app.state.master_pool = await create_pool(MASTER_DSN, "master")
        logger.info("MASTER pool created.")
    except Exception as e:
        logger.error(f"Failed to create MASTER pool: {e}")
        raise

    try:
        app.state.slave_pool = await create_pool(SLAVE_DSN, "slave")
        logger.info("SLAVE pool created.")
    except Exception as e:
        logger.warning(f"Could not create SLAVE at startup: {e}")
        app.state.slave_pool = None
        async with app.state.lock:
            app.state.current_read = "master"

    # Background health checker
    app.state.health_task = asyncio.create_task(slave_health_checker())

    yield

    # Shutdown cleanup
    logger.info("Shutting down API...")
    if app.state.health_task:
        app.state.health_task.cancel()
        try:
            await app.state.health_task
        except asyncio.CancelledError:
            pass

    if app.state.slave_pool:
        await app.state.slave_pool.close()
    if app.state.master_pool:
        await app.state.master_pool.close()

    logger.info("Shutdown complete.")


app.router.lifespan_context = lifespan


# ============================================================
# DB HELPERS
# ============================================================

async def set_statement_timeout(conn, ms=STATEMENT_TIMEOUT_MS):
    await conn.execute(f"SET LOCAL statement_timeout = {ms}")

async def try_query_with_pool(pool, sql, *args):
    async with pool.acquire() as conn:
        await set_statement_timeout(conn)
        return await conn.fetch(sql, *args)

async def try_query_with_pool_row(pool, sql, *args):
    async with pool.acquire() as conn:
        await set_statement_timeout(conn)
        return await conn.fetchrow(sql, *args)

async def get_read_pool():
    async with app.state.lock:
        pref = app.state.current_read
    return app.state.slave_pool if pref == "slave" and app.state.slave_pool else app.state.master_pool

async def run_read_query(sql, *args):
    pool = await get_read_pool()
    try:
        return await try_query_with_pool(pool, sql, *args)
    except Exception as e:
        logger.warning(f"Query failed on {app.state.current_read}: {e}. Switching to MASTER.")
        async with app.state.lock:
            app.state.current_read = "master"
        return await try_query_with_pool(app.state.master_pool, sql, *args)

async def run_read_query_row(sql, *args):
    pool = await get_read_pool()
    try:
        return await try_query_with_pool_row(pool, sql, *args)
    except Exception as e:
        logger.warning(f"QueryRow failed on {app.state.current_read}: {e}. Switching to MASTER.")
        async with app.state.lock:
            app.state.current_read = "master"
        return await try_query_with_pool_row(app.state.master_pool, sql, *args)


# ============================================================
# UTIL — SAFE DATE HANDLING
# ============================================================

def date_filter_params(start: Optional[date], end: Optional[date]):
    """Return (use_filter, start, end_exclusive)."""
    if start and end:
        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be <= end_date")
        return True, start, end + timedelta(days=1)
    return False, None, None


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/health",
         description= "Return the current health of the API and indicate whether reads are coming  "
                      "from the SLAVE or MASTER database.")
async def health():
    async with app.state.lock:
        return {"status": "OK", "reading_from": app.state.current_read}


@app.get("/")
async def root():
    return {"message": "BobaShop Analytics API Running", "docs": "/docs"}


# ------------------------------------------------------------
# 1. TOTAL PAGEVIEWS
# ------------------------------------------------------------

@app.get("/metrics/pageviews/total",
         description="Return the total number of pageviews. "
                     "If start_date and end_date are provided, results are filtered to the date range [start_date, end_date] (inclusive). "
                     "Otherwise, the count is computed from the entire dataset. ")
async def total_pageviews(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT COUNT(pageview_id) AS total_pageviews
        FROM pageviews
        WHERE pv_timestamp >= $1 AND pv_timestamp < $2;
        """
        row = await run_read_query_row(sql, start, end)
    else:
        row = await run_read_query_row("SELECT COUNT(pageview_id) AS total_pageviews FROM pageviews;")

    return dict(row) if row else {"total_pageviews": 0}


# ------------------------------------------------------------
# 2. USERS & SESSIONS
# ------------------------------------------------------------

@app.get("/metrics/users-sessions",
         description= "Return the total number of unique users and total number of sessions. "
                      "If start_date and end_date are provided, results are filtered by session_start  "
                      "in the date range [start_date, end_date] (inclusive).  "
                      "Otherwise, return overall totals.")
async def users_sessions(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(session_id)       AS total_sessions
        FROM sessions
        WHERE session_start >= $1 AND session_start < $2;
        """
        row = await run_read_query_row(sql, start, end)
    else:
        sql = """
        SELECT
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(session_id)       AS total_sessions
        FROM sessions;
        """
        row = await run_read_query_row(sql)

    return dict(row)


# ------------------------------------------------------------
# 3. SESSIONS BY CHANNEL
# ------------------------------------------------------------

@app.get("/metrics/sessions/by-channel",
         description="Return the number of sessions grouped by traffic channel  "
                     "(e.g., Direct, Referral, Organic, Social).  "
                     "Optional filtering by session_start using start_date and end_date.")
async def sessions_by_channel(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT channel, COUNT(session_id) AS total_sessions
        FROM sessions
        WHERE session_start >= $1 AND session_start < $2
        GROUP BY channel
        ORDER BY total_sessions DESC;
        """
        rows = await run_read_query(sql, start, end)
    else:
        sql = """
        SELECT channel, COUNT(session_id) AS total_sessions
        FROM sessions
        GROUP BY channel
        ORDER BY total_sessions DESC;
        """
        rows = await run_read_query(sql)

    return [dict(r) for r in rows]


# ------------------------------------------------------------
# 4. SESSIONS BY DEVICE
# ------------------------------------------------------------

@app.get("/metrics/sessions/by-device",
         description = "Return the number of sessions grouped by device type  "
                       "(e.g., Desktop, Mobile, Tablet).  "
                       "Optional filtering by session_start using start_date and end_date.")
async def sessions_by_device(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT device, COUNT(session_id) AS total_sessions
        FROM sessions
        WHERE session_start >= $1 AND session_start < $2
        GROUP BY device
        ORDER BY total_sessions DESC;
        """
        rows = await run_read_query(sql, start, end)
    else:
        sql = """
        SELECT device, COUNT(session_id) AS total_sessions
        FROM sessions
        GROUP BY device
        ORDER BY total_sessions DESC;
        """
        rows = await run_read_query(sql)

    return [dict(r) for r in rows]


# ------------------------------------------------------------
# 5. PAGEVIEWS BY PAGETYPE
# ------------------------------------------------------------

@app.get("/metrics/pageviews/by-pagetype",
         description= "Return the number of pageviews grouped by pagetype."
                      "Optional filtering by pv_timestamp using start_date and end_date.")
async def pageviews_by_pagetype(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT pagetype, COUNT(pageview_id) AS total_pageviews
        FROM pageviews
        WHERE pv_timestamp >= $1 AND pv_timestamp < $2
        GROUP BY pagetype
        ORDER BY total_pageviews DESC;
        """
        rows = await run_read_query(sql, start, end)
    else:
        sql = """
        SELECT pagetype, COUNT(pageview_id) AS total_pageviews
        FROM pageviews
        GROUP BY pagetype
        ORDER BY total_pageviews DESC;
        """
        rows = await run_read_query(sql)

    return [dict(r) for r in rows]


# ------------------------------------------------------------
# 6. TRANSACTION SUMMARY
# ------------------------------------------------------------

@app.get("/metrics/transactions/summary",
         description="Return a summary of all transactions, including:  "
                     "- Number of unique buyers  "
                     "- Number of sessions with at least one transaction  "
                     "- Total number of transactions  "
                     "If start_date and end_date are provided, results are filtered by t_timestamp  "
                     "in the date range [start_date, end_date] (inclusive).")
async def transactions_summary(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        SELECT
            COUNT(DISTINCT user_id)    AS unique_buyers,
            COUNT(DISTINCT session_id) AS sessions_with_transactions,
            COUNT(transaction_id)      AS total_transactions
        FROM transactions
        WHERE t_timestamp >= $1 AND t_timestamp < $2;
        """
        row = await run_read_query_row(sql, start, end)
    else:
        sql = """
        SELECT
            COUNT(DISTINCT user_id)    AS unique_buyers,
            COUNT(DISTINCT session_id) AS sessions_with_transactions,
            COUNT(transaction_id)      AS total_transactions
        FROM transactions;
        """
        row = await run_read_query_row(sql)

    return dict(row)


# ------------------------------------------------------------
# 7. TRANSACTION DETAILS
# ------------------------------------------------------------

@app.get("/transactions/{transaction_id}/items",
         description="Return detailed information for a specific transaction,"
                     "including each item (SKU, quantity, price, currency)."
                     "Matches the transaction to its corresponding purchase event (event_type_id = 5)  "
                     "and returns the associated event attributes.")
async def transaction_items(transaction_id: int):
    sql = """
    WITH purchase_event AS (
        SELECT * FROM events WHERE event_type_id = 5
    ),
    transactions_filtered AS (
        SELECT session_id, t_timestamp
        FROM transactions
        WHERE transaction_id = $1
    ),
    get_event_id AS (
        SELECT pe.event_id
        FROM transactions_filtered tf
        LEFT JOIN purchase_event pe
          ON tf.t_timestamp = pe.event_timestamp
         AND tf.session_id = pe.session_id
    )
    SELECT sku, quantity, price, currency
    FROM event_attributes
    WHERE event_type_id = 5
      AND event_id IN (SELECT event_id FROM get_event_id);
    """
    rows = await run_read_query(sql, transaction_id)
    return {"transaction_id": transaction_id, "items": [dict(r) for r in rows]}


# ------------------------------------------------------------
# 8. TRANSACTIONS AOV
# ------------------------------------------------------------

@app.get("/metrics/transactions/aov",
         description = "Return two metrics for completed orders (event_type_id = 5):"
                       "- Average number of items per order  "
                       "- Average order value (sum of item prices)"
                       "If start_date and end_date are provided, results are filtered by t_timestamp  "
                       "in the date range [start_date, end_date] (inclusive).")
async def transactions_aov(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        WITH purchase_event AS (
            SELECT SUM(a.quantity) AS number_of_items,
                   SUM(a.price)    AS order_value
            FROM event_attributes a
            JOIN events e ON e.event_id = a.event_id AND e.event_type_id = a.event_type_id
            JOIN transactions t ON t.pageview_id = e.pageview_id
            WHERE a.event_type_id = 5
              AND t.t_timestamp >= $1
              AND t.t_timestamp < $2
            GROUP BY a.event_id
        )
        SELECT AVG(number_of_items) AS avg_n_items_order,
               AVG(order_value)     AS avg_order_value
        FROM purchase_event;
        """
        row = await run_read_query_row(sql, start, end)
    else:
        sql = """
        WITH purchase_event AS (
            SELECT SUM(a.quantity) AS number_of_items,
                   SUM(a.price)    AS order_value
            FROM event_attributes a
            WHERE a.event_type_id = 5
            GROUP BY a.event_id
        )
        SELECT AVG(number_of_items) AS avg_n_items_order,
               AVG(order_value)     AS avg_order_value
        FROM purchase_event;
        """
        row = await run_read_query_row(sql)

    return dict(row)


# ------------------------------------------------------------
# 9. AVG CART VALUE
# ------------------------------------------------------------

@app.get("/metrics/cart/avg",
         description="Return two metrics for view-cart events (event_type_id = 4): "
                     "- Average number of items in cart  "
                     "- Average cart value (sum of item prices)"
                     "If start_date and end_date are provided, results are filtered using the timestamp  "
                     "of related transactions in the date range [start_date, end_date] (inclusive).")
async def avg_cart_value(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    use_filter, start, end = date_filter_params(start_date, end_date)

    if use_filter:
        sql = """
        WITH view_cart_event AS (
            SELECT SUM(a.quantity) AS number_of_items,
                   SUM(a.price)    AS cart_value
            FROM event_attributes a
            JOIN events e ON e.event_id = a.event_id AND e.event_type_id = a.event_type_id
            JOIN transactions t ON t.pageview_id = e.pageview_id
            WHERE a.event_type_id = 4
              AND t.t_timestamp >= $1
              AND t.t_timestamp < $2
            GROUP BY a.event_id
        )
        SELECT AVG(number_of_items) AS avg_n_cart_items,
               AVG(cart_value)      AS avg_cart_value
        FROM view_cart_event;
        """
        row = await run_read_query_row(sql, start, end)
    else:
        sql = """
        WITH view_cart_event AS (
            SELECT SUM(a.quantity) AS number_of_items,
                   SUM(a.price)    AS cart_value
            FROM event_attributes a
            WHERE a.event_type_id = 4
            GROUP BY a.event_id
        )
        SELECT AVG(number_of_items) AS avg_n_cart_items,
               AVG(cart_value)      AS avg_cart_value
        FROM view_cart_event;
        """
        row = await run_read_query_row(sql)

    return dict(row)


# ============================================================
# Run directly
# ============================================================

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting API with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)

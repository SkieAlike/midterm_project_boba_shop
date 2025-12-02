#!/usr/bin/env python3
"""
generate_website_traffic_data.py

Generates synthetic BobaShop analytics data matching the provided schema:
- sessions (user_id, session_id, session_start, channel, device)
- pageviews (session_id, pageview_id, pv_timestamp, referer, url, pagetype)
- events (session_id, pageview_id, event_id, event_type_id, event_timestamp)
    PARTITION BY LIST (event_type_id) -> this script ensures partitions exist
- event_attributes (event_id, event_type_id, sku, quantity, price, currency)
    PK (event_id, sku) ; FK (event_id,event_type_id) -> events(event_id,event_type_id)
- transactions (transaction_id, user_id, session_id, pageview_id, t_timestamp)
    PARTITION BY RANGE (t_timestamp) -> this script creates monthly partitions

Notes:
- First pageview_id for a session == session_id.
- Purchase event has the same timestamp as its transaction.
- view_cart and purchase events can have multiple items (multiple event_attribute rows).
- This script batches inserts (BATCH_SIZE) and commits after each batch.
"""

import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ---------------- CONFIG ----------------
conn_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "gvantsa",
    "host": "localhost",
    "port": 5432
}

NUM_SESSIONS = 1_000_000   # change to smaller number for testing
BATCH_SIZE = 5000          # number of sessions per batch commit

START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2026, 1, 1)  # exclusive end for random timestamps

NUM_USERS = 250_000  # users pool (so users can have multiple sessions)

AVG_PAGEVIEWS_PER_SESSION = 2.6
AVG_EVENTS_PER_PAGEVIEW = 3

PURCHASE_PROBABILITY = 0.02  # per pageview chance to have purchase event

CHANNELS = ["SEO", "Email", "Social", "Paid", "Direct", "Affiliate"]
DEVICES = ["Mobile", "Desktop", "Tablet"]
PAGETYPES = ["home", "category", "search", "detail", "wishlist",
             "promotion", "basket", "checkout", "account"]

# Event types mapping (IDs must exist in event_name_mapping in DB)
EVENT_TYPES = {
    1: "view_page",
    2: "click",
    3: "add_to_cart",
    4: "view_cart",
    5: "purchase",
    6: "remove_from_cart"
}

# SKU pool
SKU_POOL = [f"SKU_{i:04d}" for i in range(1, 2001)]  # 2000 SKUs

# reproducible
random.seed(42)

# ---------------- HELPERS ----------------
def rand_timestamp_between(start: datetime, end: datetime) -> datetime:
    total_seconds = int((end - start).total_seconds())
    offset = random.randint(0, total_seconds - 1)
    return start + timedelta(seconds=offset)

def make_id_from_second(ts: datetime, counter: int) -> int:
    """
    BIGINT id = YYYYMMDDHHMMSS + 4-digit counter -> 18 digits approx
    ts must be naive (no tz)
    """
    s = ts.strftime("%Y%m%d%H%M%S")
    return int(s + f"{counter:04d}")

def random_referer():
    choices = ["google.com", "facebook.com", "instagram.com", "t.co", "newsletter.example", None]
    pick = random.choice(choices)
    if not pick:
        return None
    return f"https://{pick}/q={random.randint(1000,9999)}"

def random_url(pagetype):
    return f"https://boba.example/{pagetype}/{random.randint(1000,999999)}"

def generate_attrs_for_event(event_id: int, event_type_id: int):
    """
    Return list of (event_id, event_type_id, sku, quantity, price, currency)
    Guarantees unique SKUs per event (no duplicate (event_id, sku))
    """
    rows = []
    if event_type_id in (4, 5):  # view_cart or purchase -> multiple items likely
        n = random.randint(1, 5)
        used = set()
        while len(used) < n:
            sku = random.choice(SKU_POOL)
            if sku in used:
                continue
            used.add(sku)
            rows.append((event_id, event_type_id, sku, random.randint(1, 4), round(random.uniform(2.0, 500.0), 2), "USD"))
    elif event_type_id in (3, 6):  # add_to_cart, remove_from_cart -> usually single SKU
        sku = random.choice(SKU_POOL)
        rows.append((event_id, event_type_id, sku, 1, round(random.uniform(2.0, 200.0), 2), "USD"))
    # view_page or click -> no attributes
    return rows

# ---------------- DB helpers: ensure partitions exist ----------------
def ensure_event_partitions(cur):
    """
    Create LIST partitions for events table for each event_type_id found in event_name_mapping.
    Partition names: events_type_<id>
    """
    cur.execute("SELECT event_type_id FROM event_name_mapping")
    ids = [row[0] for row in cur.fetchall()]
    for et in ids:
        part_name = f"events_type_{et}"
        sql = f"CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF events FOR VALUES IN ({et});"
        cur.execute(sql)

def ensure_transaction_partitions(cur, start: datetime, end: datetime):
    """
    Create monthly partitions for transactions between start (inclusive) and end (exclusive).
    Partition names: transactions_YYYYMM
    """
    curr = datetime(start.year, start.month, 1)
    while curr < end:
        next_month = (curr.replace(day=28) + timedelta(days=4)).replace(day=1)
        start_id = curr  # used only for naming; transactions are partitioned by timestamp
        suffix = curr.strftime("%Y%m")
        part_name = f"transactions_{suffix}"
        # Create partition
        sql = f"""
        CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF transactions
        FOR VALUES FROM ('{curr.strftime('%Y-%m-%d')}') TO ('{next_month.strftime('%Y-%m-%d')}');
        """
        cur.execute(sql)
        curr = next_month

# ---------------- MAIN GENERATOR ----------------
def generate_data():
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    try:
        # 0. populate event_name_mapping if not exists
        mapping_rows = [(k, v) for k, v in EVENT_TYPES.items()]
        execute_values(cur,
                       "INSERT INTO event_name_mapping(event_type_id, event_name) VALUES %s ON CONFLICT DO NOTHING",
                       mapping_rows)
        conn.commit()

        # Ensure partitions exist BEFORE inserting
        ensure_event_partitions(cur)
        ensure_transaction_partitions(cur, START_DATE, END_DATE)
        conn.commit()

        # Batches to accumulate rows before insert
        sessions_batch = []
        pageviews_batch = []
        events_batch = []
        attrs_batch = []
        tx_batch = []

        per_second_counter = {}  # to create unique ids when multiple events in same second
        event_id_seq = 1

        user_pool = [i + 1 for i in range(NUM_USERS)]

        inserted_sessions = 0
        start_wall = datetime.now()

        for i in range(NUM_SESSIONS):
            # pick user
            user_id = random.choice(user_pool)

            # session start timestamp
            session_start = rand_timestamp_between(START_DATE, END_DATE)
            sec_key = session_start.strftime("%Y%m%d%H%M%S")
            cnt = per_second_counter.get(sec_key, 0)
            per_second_counter[sec_key] = cnt + 1
            if per_second_counter[sec_key] > 9999:
                per_second_counter[sec_key] = 1
                cnt = 0
            session_id = make_id_from_second(session_start, cnt)

            # Append session
            sessions_batch.append((user_id, session_id, session_start, random.choice(CHANNELS), random.choice(DEVICES)))

            # pageviews for the session
            num_pv = max(1, int(random.gauss(AVG_PAGEVIEWS_PER_SESSION, 1)))
            pv_time = session_start
            pageview_ids_in_session = []

            for pv_index in range(num_pv):
                pv_time = pv_time + timedelta(seconds=random.randint(5, 300))
                if pv_index == 0:
                    pageview_id = session_id  # first pageview equals session id
                else:
                    sec_key_pv = pv_time.strftime("%Y%m%d%H%M%S")
                    c_pv = per_second_counter.get(sec_key_pv, 0)
                    per_second_counter[sec_key_pv] = c_pv + 1
                    if per_second_counter[sec_key_pv] > 9999:
                        per_second_counter[sec_key_pv] = 1
                        c_pv = 0
                    pageview_id = make_id_from_second(pv_time, c_pv)

                pagetype = random.choice(PAGETYPES)
                referer = random_referer()
                url = random_url(pagetype)
                pageviews_batch.append((session_id, pageview_id, pv_time, referer, url, pagetype))
                pageview_ids_in_session.append((pageview_id, pv_time))

                # events for this pageview
                num_events = max(1, int(random.gauss(AVG_EVENTS_PER_PAGEVIEW, 1)))
                evt_time = pv_time
                for _ in range(num_events):
                    # choose event type with bias
                    event_type_id = random.choices(list(EVENT_TYPES.keys()),
                                                  weights=[50, 15, 10, 8, 4, 13])[0]
                    evt_time = evt_time + timedelta(seconds=random.randint(0, 120))
                    event_id = event_id_seq
                    event_id_seq += 1

                    # store event row
                    events_batch.append((session_id, pageview_id, event_id, event_type_id, evt_time))

                    # attributes if item events: (event_id, event_type_id, sku, quantity, price, currency)
                    if event_type_id in (3, 4, 5, 6):
                        new_attrs = generate_attrs_for_event(event_id, event_type_id)
                        # new_attrs elements are (event_id, event_type_id, sku, qty, price, currency)
                        attrs_batch.extend(new_attrs)

                    # if purchase, create transaction (one per pageview typically)
                    if event_type_id == 5:
                        t_time = evt_time
                        sec_key_tx = t_time.strftime("%Y%m%d%H%M%S")
                        c_tx = per_second_counter.get(sec_key_tx, 0)
                        per_second_counter[sec_key_tx] = c_tx + 1
                        if per_second_counter[sec_key_tx] > 9999:
                            per_second_counter[sec_key_tx] = 1
                            c_tx = 0
                        transaction_id = make_id_from_second(t_time, c_tx)
                        tx_batch.append((transaction_id, user_id, session_id, pageview_id, t_time))

            # end pageviews for session

            # Batch insert when ready
            if (i + 1) % BATCH_SIZE == 0:
                inserted_sessions += len(sessions_batch)
                elapsed = (datetime.now() - start_wall).total_seconds()
                print(f"[{datetime.now()}] Inserting batch. Sessions so far: {inserted_sessions} (elapsed {elapsed:.1f}s)")

                # insert sessions
                if sessions_batch:
                    execute_values(cur,
                                   "INSERT INTO sessions(user_id, session_id, session_start, channel, device) VALUES %s",
                                   sessions_batch, page_size=1000)

                # pageviews
                if pageviews_batch:
                    execute_values(cur,
                                   "INSERT INTO pageviews(session_id, pageview_id, pv_timestamp, referer, url, PAGETYPE) VALUES %s",
                                   pageviews_batch, page_size=1000)

                # events
                if events_batch:
                    execute_values(cur,
                                   "INSERT INTO events(session_id, pageview_id, event_id, event_type_id, event_timestamp) VALUES %s",
                                   events_batch, page_size=1000)

                # event_attributes
                if attrs_batch:
                    # Ensure attrs_batch has unique (event_id, sku) before insert — they are generated unique by helper
                    execute_values(cur,
                                   "INSERT INTO event_attributes(event_id, event_type_id, sku, quantity, price, currency) VALUES %s",
                                   attrs_batch, page_size=1000)

                # transactions
                if tx_batch:
                    execute_values(cur,
                                   "INSERT INTO transactions(transaction_id, user_id, session_id, pageview_id, t_timestamp) VALUES %s",
                                   tx_batch, page_size=500)

                conn.commit()

                # clear batches
                sessions_batch.clear()
                pageviews_batch.clear()
                events_batch.clear()
                attrs_batch.clear()
                tx_batch.clear()

        # final flush for any remaining data
        if sessions_batch:
            print(f"[{datetime.now()}] Final insert for remaining {len(sessions_batch)} sessions.")
            if sessions_batch:
                execute_values(cur,
                               "INSERT INTO sessions(user_id, session_id, session_start, channel, device) VALUES %s",
                               sessions_batch, page_size=1000)
            if pageviews_batch:
                execute_values(cur,
                               "INSERT INTO pageviews(session_id, pageview_id, pv_timestamp, referer, url, PAGETYPE) VALUES %s",
                               pageviews_batch, page_size=1000)
            if events_batch:
                execute_values(cur,
                               "INSERT INTO events(session_id, pageview_id, event_id, event_type_id, event_timestamp) VALUES %s",
                               events_batch, page_size=1000)
            if attrs_batch:
                execute_values(cur,
                               "INSERT INTO event_attributes(event_id, event_type_id, sku, quantity, price, currency) VALUES %s",
                               attrs_batch, page_size=1000)
            if tx_batch:
                execute_values(cur,
                               "INSERT INTO transactions(transaction_id, user_id, session_id, pageview_id, t_timestamp) VALUES %s",
                               tx_batch, page_size=500)
            conn.commit()

        total_elapsed = (datetime.now() - start_wall).total_seconds()
        print(f"[{datetime.now()}] DONE. Inserted {NUM_SESSIONS} sessions in {total_elapsed:.1f}s")

    except Exception as e:
        print("ERROR during generation, rolling back:", e)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # safety tip: run with small NUM_SESSIONS first for testing
    generate_data()

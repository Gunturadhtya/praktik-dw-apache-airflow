"""One function per Airflow task. Each stage reads the previous staging layer and writes its own:

  init_run            -> etl_run_log (+ creates staging tables)
  extract_stage       OLTP/DW  -> stg_extract            (+ pending watermarks, manifest)
  transform_dims      stg_extract -> stg_transform       (dimension rows)
  transform_facts     stg_extract -> stg_transform       (fact rows)
  prepare_dim_load    stg_transform -> stg_load.dim_*    (SCD2 decision made here)
  load_dimensions     stg_load.dim_* -> DW               (1 transaction)
  prepare_fact_load   stg_transform + DW keys -> stg_load.fact_* / bridge  (surrogate keys resolved)
  load_facts          stg_load.fact_* -> DW              (1 transaction)
  validate            reconciles the three layers + the DW
  commit_watermarks   pending -> real watermarks
  finish_run          final status, rejects -> etl_control, staging purge

transform.py / extract.py / load.py are reused unchanged (business rules stay where they were).
Every stage starts by clearing its own batch, so retries are safe.
"""
import logging
import time
from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal

import staging
import staging_schema as S
import transform as T
from control import Control
from db import oltp_conn, dw_conn, dw_read_conn, stg_conn
from extract import extract
from load import (STATIC_DIMS, FAR_FUTURE, EPOCH, CHUNK, _upsert_sql, _write, _keymap, _statusmap,
                  _ids, _product_versions, _bad)
from metrics import Metrics

log = logging.getLogger("etl")
EXT, TRF, LD = "stg_extract", "stg_transform", "stg_load"

STATIC_COL = {t: col for t, (col, _) in STATIC_DIMS.items()}
STATIC_COL["dim_carrier"] = "carrier_name"

# (stg_load table, DW columns, key columns) for type-1 dimensions. clean key = stg_load name minus "dim_"
TYPE1 = [
    ("dim_customer", ["user_id", "email", "full_name", "registered_at"], ["user_id"]),
    ("dim_seller", ["user_id", "email", "full_name"], ["user_id"]),
    ("dim_geography", ["address_id", "city", "state", "postal_code", "country"], ["address_id"]),
    ("dim_coupon", ["coupon_id", "code", "discount_type", "discount_value", "valid_from", "valid_until"],
     ["coupon_id"]),
    ("dim_tag", ["tag_id", "name"], ["tag_id"]),
]
TYPE1_CLEAN = {"dim_customer": "customer", "dim_seller": "seller", "dim_geography": "geography",
               "dim_coupon": "coupon", "dim_tag": "tag"}

FACTS = {  # DW fact table -> key columns of the upsert
    "fact_sales": ["order_item_id"],
    "fact_order_coupon": ["order_id", "coupon_key"],
    "fact_payment": ["payment_id"],
    "fact_shipment": ["shipment_id"],
    "fact_return": ["return_id"],
    "fact_review": ["review_id"],
    "fact_inventory_daily": ["date_key", "product_key"],
    "fact_cart_item": ["cart_id", "product_key"],
}

# (clean key, raw table counted as rows_in, fn(raw, ctx, rej, today))
DIM_STEPS = [
    ("customer", "users", lambda raw, ctx, rej, td: T.t_customer(raw, rej)),
    ("seller", "products", lambda raw, ctx, rej, td: T.t_seller(raw, rej)),
    ("geography", "addresses", lambda raw, ctx, rej, td: T.t_geography(raw, rej)),
    ("product", "products", lambda raw, ctx, rej, td: T.t_product(raw, rej)),
    ("coupon", "coupons", lambda raw, ctx, rej, td: T.t_coupon(raw, rej)),
    ("tag", "tags", lambda raw, ctx, rej, td: T.t_tag(raw, rej)),
    ("product_tag", "product_tags", lambda raw, ctx, rej, td: T.t_product_tag(raw, rej)),
]
FACT_STEPS = [
    ("sales", "order_items", lambda raw, ctx, rej, td: T.t_sales(raw, ctx, rej)),
    ("order_coupon", "order_coupons", lambda raw, ctx, rej, td: T.t_order_coupon(raw, ctx, rej)),
    ("payment", "payments", lambda raw, ctx, rej, td: T.t_payment(raw, ctx, rej)),
    ("shipment", "shipment", lambda raw, ctx, rej, td: T.t_shipment(raw, ctx, rej)),
    ("return", "returns", lambda raw, ctx, rej, td: T.t_return(raw, ctx, rej)),
    ("review", "reviews", lambda raw, ctx, rej, td: T.t_review(raw, rej)),
    ("inventory", "inventory", lambda raw, ctx, rej, td: T.t_inventory(raw, td, rej)),
    ("cart_item", "cart_items", lambda raw, ctx, rej, td: T.t_cart_item(raw, ctx, rej)),
]

# validation maps: (clean/fact table, raw table, source_table name used in reject rows)
TRANSFORM_RECON = [
    ("customer", "users", "users"), ("geography", "addresses", "addresses"),
    ("coupon", "coupons", "coupons"), ("tag", "tags", "tags"),
    ("product_tag", "product_tags", "product_tags"), ("sales", "order_items", "order_items"),
    ("order_coupon", "order_coupons", "order_coupons"), ("payment", "payments", "payments"),
    ("shipment", "shipment", "shipment"), ("return", "returns", "returns"),
    ("review", "reviews", "product_reviews"),
]
LOAD_RECON = [
    ("fact_sales", "sales", "order_items"), ("fact_order_coupon", "order_coupon", "order_coupons"),
    ("fact_payment", "payment", "payments"), ("fact_shipment", "shipment", "shipment"),
    ("fact_return", "return", "returns"), ("fact_review", "review", "product_reviews"),
    ("fact_inventory_daily", "inventory", "products"), ("fact_cart_item", "cart_item", "cart_items"),
    ("bridge_product_tag", "product_tag", "product_tags"),
]
DW_PRESENCE = [("fact_sales", "order_item_id"), ("fact_payment", "payment_id"),
               ("fact_shipment", "shipment_id"), ("fact_return", "return_id"), ("fact_review", "review_id")]


# ----------------------------------------------------------------------------- plumbing
class _Run:
    def __init__(self, run_id, task):
        self.run_id, self.task = run_id, task
        self.ctl = Control()
        self.ctl.task = task
        self.ctl.delete_task_metrics(run_id, task)       # a retried task must not double-log
        self.m = Metrics(self.ctl, run_id)
        self.stg = stg_conn()

    def close(self):
        for c in (self.stg, self.ctl):
            try:
                c.close()
            except Exception:
                pass


@contextmanager
def run_ctx(run_id, task):
    r = _Run(run_id, task)
    try:
        yield r
    finally:
        r.close()


def _stage_write(r, stage, schema, table, rows):
    t0 = time.perf_counter()
    n = staging.write(r.stg, schema, table, rows, r.run_id)
    r.m.record(stage, f"stg:{table}", n, n, 0, time.perf_counter() - t0)


# ----------------------------------------------------------------------------- init / finish
def init_run(trigger="Cron") -> dict:
    """Creates the run row and FREEZES today's date (SCD2 + daily inventory snapshot use it)."""
    stg = stg_conn()
    try:
        S.ensure(stg)
    finally:
        stg.close()
    ctl = Control(ensure=True)
    try:
        ctl.fail_stale_runs()
        run_id = ctl.start_run("Manual" if "manual" in str(trigger).lower() else "Cron")
    finally:
        ctl.close()
    log.info("=== run %d started (%s) ===", run_id, trigger)
    return {"run_id": run_id, "today": date.today().isoformat()}


def finish_run(run_id, status, error=None, keep_last=10) -> None:
    ctl, stg = Control(), stg_conn()
    try:
        ctl.copy_rejects(run_id)
        ctl.finish_run(run_id, status, error)
        if status == "Success":
            staging.purge(stg, run_id - keep_last)
            ctl.purge_pending(run_id - keep_last)
    finally:
        stg.close()
        ctl.close()
    log.info("=== run %d %s ===", run_id, status)


def commit_watermarks(run_id) -> None:
    with run_ctx(run_id, "commit_watermarks") as r:
        r.ctl.promote_pending(run_id)


# ----------------------------------------------------------------------------- 1. EXTRACT -> stg_extract
def extract_stage(run_id, today) -> None:
    td = date.fromisoformat(today)
    with run_ctx(run_id, "extract") as r:
        staging.clear(r.stg, EXT, list(S.EXTRACT), run_id)
        r.ctl.clear_pending(run_id)
        state, fp_old = r.ctl.load_state(), r.ctl.load_product_fp()
        src, dw_read = oltp_conn(), dw_read_conn()
        try:   # ONE task = ONE consistent REPEATABLE READ snapshot (see extract.py)
            raw, new_state, fp_updates, manifest = extract(src, dw_read, state, fp_old, r.m, td)
        finally:
            src.close()
            dw_read.close()
        for table in S.EXTRACT:
            _stage_write(r, "Extract", EXT, table, raw.get(table, []))
        r.ctl.add_manifest(run_id, manifest)
        r.ctl.save_pending(run_id, new_state, fp_updates)   # NOT promoted yet


# ----------------------------------------------------------------------------- 2. TRANSFORM -> stg_transform
def _transform(run_id, today, task, steps, rej_stage) -> None:
    td = date.fromisoformat(today)
    with run_ctx(run_id, task) as r:
        staging.clear(r.stg, TRF, [s[0] for s in steps], run_id)
        staging.clear_rejects(r.stg, run_id, rej_stage)
        raw = staging.read_many(r.stg, EXT, list(S.EXTRACT), run_id)
        ctx, rej = T._ctx(raw), []
        for name, src, fn in steps:
            before, t0 = len(rej), time.perf_counter()
            rows = fn(raw, ctx, rej, td)
            r.m.record("Transform", name, len(raw[src]), len(rows), len(rej) - before,
                       time.perf_counter() - t0)
            _stage_write(r, "Transform", TRF, name, rows)
        staging.write_rejects(r.stg, run_id, rej_stage, rej)


def transform_dims(run_id, today) -> None:
    _transform(run_id, today, "transform_dims", DIM_STEPS, "transform_dims")


def transform_facts(run_id, today) -> None:
    _transform(run_id, today, "transform_facts", FACT_STEPS, "transform_facts")


# ----------------------------------------------------------------------------- 3a. DIMENSIONS -> stg_load -> DW
def prepare_dim_load(run_id, today) -> None:
    td = date.fromisoformat(today)
    with run_ctx(run_id, "prepare_dim_load") as r:
        staging.clear(r.stg, LD, ["dim_static", *TYPE1_CLEAN, "dim_product"], run_id)
        clean = staging.read_many(r.stg, TRF, [*TYPE1_CLEAN.values(), "product", "shipment"], run_id)

        # static dimensions + carriers (carrier names come from the shipments of this batch)
        static = [dict(dim_table=t, value=v) for t, (_, values) in STATIC_DIMS.items() for v in values]
        static += [dict(dim_table="dim_carrier", value=c)
                   for c in sorted({s["carrier_name"] for s in clean["shipment"]})]
        _stage_write(r, "Load", LD, "dim_static", static)

        for table, _, _ in TYPE1:
            _stage_write(r, "Load", LD, table, clean[TYPE1_CLEAN[table]])

        # dim_product SCD2: decide NEW / CHANGED / UNCHANGED against the DW's current versions
        t0, products = time.perf_counter(), clean["product"]
        current, ids = {}, [p["product_id"] for p in products]
        dw = dw_read_conn()
        try:
            with dw.cursor() as cur:
                for i in range(0, len(ids), CHUNK):
                    ch = ids[i:i + CHUNK]
                    cur.execute("SELECT product_key,product_id,name,description,category_level1,category_level2,"
                                "category_level3,current_price,effective_from FROM dim_product "
                                f"WHERE is_current=1 AND product_id IN ({','.join(['%s'] * len(ch))})", ch)
                    current.update({x["product_id"]: x for x in cur.fetchall()})
        finally:
            dw.close()
        tracked = ("name", "description", "category_level1", "category_level2", "category_level3")
        rows = []
        for p in products:
            base = {k: p[k] for k in ("product_id", "name", "description", "category_level1",
                                      "category_level2", "category_level3", "current_price")}
            base.update(effective_to=FAR_FUTURE, close_key=None, close_to=None)
            old = current.get(p["product_id"])
            if old is None:                                              # brand new product
                rows.append({**base, "effective_from": EPOCH})
                continue
            same = (all((old[c] or "") == (p[c] or "") for c in tracked)
                    and Decimal(str(old["current_price"])) == p["current_price"])
            if same:
                continue                                                 # no new version
            if old["effective_from"] == td:                              # 2nd change same day: fix in place
                rows.append({**base, "effective_from": td})
            else:                                                        # close old version, open a new one
                rows.append({**base, "effective_from": td, "close_key": old["product_key"],
                             "close_to": td - timedelta(days=1)})
        staging.write(r.stg, LD, "dim_product", rows, run_id)
        r.m.record("Load", "prep:dim_product(SCD2)", len(products), len(rows), 0, time.perf_counter() - t0)


_PRODUCT_UPSERT = (
    "INSERT INTO dim_product (product_id,name,description,category_level1,category_level2,category_level3,"
    "current_price,effective_from,effective_to,is_current) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1) "
    "ON DUPLICATE KEY UPDATE name=VALUES(name),description=VALUES(description),"
    "category_level1=VALUES(category_level1),category_level2=VALUES(category_level2),"
    "category_level3=VALUES(category_level3),current_price=VALUES(current_price),"
    "effective_to=VALUES(effective_to),is_current=1")


def load_dimensions(run_id) -> None:
    with run_ctx(run_id, "load_dimensions") as r:
        stg = lambda t: staging.read(r.stg, LD, t, run_id)   # noqa: E731
        dw = dw_conn()
        try:
            with dw.cursor() as cur:
                t0, affected, static = time.perf_counter(), 0, stg("dim_static")
                for table in sorted({x["dim_table"] for x in static}):
                    vals = [(x["value"],) for x in static if x["dim_table"] == table]
                    affected += _write(cur, f"INSERT IGNORE INTO {table} ({STATIC_COL[table]}) VALUES (%s)", vals)
                r.m.record("Load", "static_dimensions", len(static), affected, 0, time.perf_counter() - t0)

                for table, cols, keys in TYPE1:
                    t0, rows = time.perf_counter(), stg(table)
                    tuples = [tuple(x[c] for c in cols) for x in rows]
                    n = _write(cur, _upsert_sql(table, cols, keys), tuples) if tuples else 0
                    r.m.record("Load", table, len(rows), n, 0, time.perf_counter() - t0)

                t0, rows, changed = time.perf_counter(), stg("dim_product"), 0
                for x in rows:
                    if x["close_key"] is not None:
                        cur.execute("UPDATE dim_product SET effective_to=%s,is_current=0 WHERE product_key=%s",
                                    (x["close_to"], x["close_key"]))
                    cur.execute(_PRODUCT_UPSERT, (x["product_id"], x["name"], x["description"],
                                                  x["category_level1"], x["category_level2"],
                                                  x["category_level3"], x["current_price"],
                                                  x["effective_from"], x["effective_to"]))
                    changed += 1
                r.m.record("Load", "dim_product(SCD2)", len(rows), changed, 0, time.perf_counter() - t0)
            dw.commit()                    # idempotent: a retry after this point rewrites the same rows
        except Exception:
            dw.rollback()
            raise
        finally:
            dw.close()


# ----------------------------------------------------------------------------- 3b. FACTS -> stg_load -> DW
def prepare_fact_load(run_id) -> None:
    """Resolve surrogate keys against the (already committed) DW dimensions and stage DW-shaped rows."""
    with run_ctx(run_id, "prepare_fact_load") as r:
        staging.clear(r.stg, LD, [*FACTS, "bridge_product_tag"], run_id)
        staging.clear_rejects(r.stg, run_id, "load_prep")
        clean = staging.read_many(r.stg, TRF, ["sales", "order_coupon", "payment", "shipment", "return",
                                               "review", "inventory", "cart_item", "product_tag"], run_id)
        rej = []
        dw = dw_read_conn()
        try:
            with dw.cursor() as cur:
                _build_facts(cur, clean, r, rej)
        finally:
            dw.close()
        staging.write_rejects(r.stg, run_id, "load_prep", rej)


def _build_facts(cur, clean, r, rej) -> None:
    cur.execute("SELECT MIN(date_key) AS lo, MAX(date_key) AS hi FROM dim_date")
    rng = cur.fetchone()
    in_range = lambda k: k is None or rng["lo"] <= k <= rng["hi"]           # noqa: E731

    cust = _keymap(cur, "dim_customer", "user_id", "customer_key",
                   _ids(clean, "customer_user_id", "sales", "order_coupon", "payment", "shipment", "return",
                        "review", "cart_item"))
    sell = _keymap(cur, "dim_seller", "user_id", "seller_key",
                   _ids(clean, "seller_user_id", "sales", "return", "inventory"))
    geo = _keymap(cur, "dim_geography", "address_id", "geo_key", _ids(clean, "address_id", "sales", "shipment"))
    coup = _keymap(cur, "dim_coupon", "coupon_id", "coupon_key", _ids(clean, "coupon_id", "order_coupon"))
    versions = _product_versions(cur, _ids(clean, "product_id", "sales", "return", "review", "inventory",
                                           "cart_item"))
    ostat = _statusmap(cur, "dim_order_status", "order_status_key", "status_name")
    pmeth = _statusmap(cur, "dim_payment_method", "payment_method_key", "method_name")
    pstat = _statusmap(cur, "dim_payment_status", "payment_status_key", "status_name")
    sstat = _statusmap(cur, "dim_shipment_status", "shipment_status_key", "status_name")
    rstat = _statusmap(cur, "dim_return_status", "return_status_key", "status_name")
    carr = _statusmap(cur, "dim_carrier", "carrier_key", "carrier_name")

    def pkey(pid, on_date):                  # product version that was valid on that date
        for key, f, t in versions.get(pid, ()):
            if f <= on_date <= t:
                return key
        return None

    def date_ok(table, sid, *dks):
        if all(in_range(k) for k in dks):
            return True
        T.reject(rej, table, sid, "date_not_in_dim", "date outside dim_date range")
        return False

    def emit(table, src, build):
        t0, before = time.perf_counter(), len(rej)
        cols = S.cols(LD, table)
        rows = [dict(zip(cols, x)) for x in map(build, src) if x is not None]
        staging.write(r.stg, LD, table, rows, r.run_id)
        r.m.record("Load", f"prep:{table}", len(src), len(rows), len(rej) - before, time.perf_counter() - t0)

    # ---- bridge_product_tag (needs the CURRENT product_key, so it is resolved after dim load) -------
    pairs = clean["product_tag"]
    pids, prod = sorted({p["product_id"] for p in pairs}), {}
    for i in range(0, len(pids), CHUNK):
        ch = pids[i:i + CHUNK]
        cur.execute(f"SELECT product_id,product_key FROM dim_product WHERE is_current=1 AND product_id IN "
                    f"({','.join(['%s'] * len(ch))})", ch)
        prod.update({x["product_id"]: x["product_key"] for x in cur.fetchall()})
    tags = _keymap(cur, "dim_tag", "tag_id", "tag_key", {p["tag_id"] for p in pairs})

    def b_bridge(p):
        pk, tk = prod.get(p["product_id"]), tags.get(p["tag_id"])
        if pk is None or tk is None:
            T.reject(rej, "product_tags", f"{p['product_id']}-{p['tag_id']}", "orphan_key",
                     "product or tag not in DW")
            return None
        return (pk, tk)
    emit("bridge_product_tag", pairs, b_bridge)

    # ---- fact_sales -----------------------------------------------------------------------------
    def b_sales(x):
        ck, sk, gk, ok = (cust.get(x["customer_user_id"]), sell.get(x["seller_user_id"]),
                          geo.get(x["address_id"]), ostat.get(x["order_status"]))
        pk = pkey(x["product_id"], x["order_date"])
        if _bad(rej, "order_items", x["order_item_id"], customer=ck, seller=sk, geography=gk,
                order_status=ok, product=pk) or not date_ok("order_items", x["order_item_id"], x["date_key"]):
            return None
        return (x["order_item_id"], x["order_id"], x["date_key"], ck, sk, pk, gk, ok, x["quantity"],
                x["unit_price"], x["subtotal"], x["allocated_discount"])
    emit("fact_sales", clean["sales"], b_sales)

    # ---- fact_order_coupon ----------------------------------------------------------------------
    def b_oc(x):
        ck, cq = cust.get(x["customer_user_id"]), coup.get(x["coupon_id"])
        if _bad(rej, "order_coupons", f"{x['order_id']}-{x['coupon_id']}", customer=ck, coupon=cq) or \
                not date_ok("order_coupons", x["order_id"], x["date_key"]):
            return None
        return (x["order_id"], cq, x["date_key"], ck, x["discount_applied"])
    emit("fact_order_coupon", clean["order_coupon"], b_oc)

    # ---- fact_payment ---------------------------------------------------------------------------
    def b_pay(x):
        ck, mk, sk = cust.get(x["customer_user_id"]), pmeth.get(x["payment_method"]), pstat.get(x["payment_status"])
        if _bad(rej, "payments", x["payment_id"], customer=ck, payment_method=mk, payment_status=sk) or \
                not date_ok("payments", x["payment_id"], x["date_key"]):
            return None
        return (x["payment_id"], x["order_id"], x["provider_transaction_id"], x["date_key"], ck, mk, sk,
                x["amount"])
    emit("fact_payment", clean["payment"], b_pay)

    # ---- fact_shipment (accumulating snapshot) --------------------------------------------------
    def b_ship(x):
        ck, gk, cr, ss = (cust.get(x["customer_user_id"]), geo.get(x["address_id"]),
                          carr.get(x["carrier_name"]), sstat.get(x["shipment_status"]))
        if _bad(rej, "shipment", x["shipment_id"], customer=ck, geography=gk, carrier=cr, shipment_status=ss) or \
                not date_ok("shipment", x["shipment_id"], x["shipped_date_key"],
                            x["estimated_delivery_date_key"], x["actual_delivery_date_key"]):
            return None
        return (x["shipment_id"], x["order_id"], x["tracking_number"], ck, gk, cr, ss, x["shipped_date_key"],
                x["estimated_delivery_date_key"], x["actual_delivery_date_key"], x["days_to_ship"],
                x["days_to_deliver"], x["days_late"])
    emit("fact_shipment", clean["shipment"], b_ship)

    # ---- fact_return ----------------------------------------------------------------------------
    def b_ret(x):
        ck, sk, rs = cust.get(x["customer_user_id"]), sell.get(x["seller_user_id"]), rstat.get(x["return_status"])
        pk = pkey(x["product_id"], x["return_date"])
        if _bad(rej, "returns", x["return_id"], customer=ck, seller=sk, return_status=rs, product=pk) or \
                not date_ok("returns", x["return_id"], x["date_key"]):
            return None
        return (x["return_id"], x["order_item_id"], x["date_key"], ck, pk, sk, rs, x["reason"], x["refund_amount"])
    emit("fact_return", clean["return"], b_ret)

    # ---- fact_review ----------------------------------------------------------------------------
    def b_rev(x):
        ck, pk = cust.get(x["customer_user_id"]), pkey(x["product_id"], x["review_date"])
        if _bad(rej, "product_reviews", x["review_id"], customer=ck, product=pk) or \
                not date_ok("product_reviews", x["review_id"], x["date_key"]):
            return None
        return (x["review_id"], x["order_item_id"], x["date_key"], ck, pk, x["rating"])
    emit("fact_review", clean["review"], b_rev)

    # ---- fact_inventory_daily -------------------------------------------------------------------
    def b_inv(x):
        sk, pk = sell.get(x["seller_user_id"]), pkey(x["product_id"], x["snapshot_date"])
        if _bad(rej, "products", x["product_id"], seller=sk, product=pk) or \
                not date_ok("products", x["product_id"], x["date_key"]):
            return None
        return (x["date_key"], pk, sk, x["stock_quantity"])
    emit("fact_inventory_daily", clean["inventory"], b_inv)

    # ---- fact_cart_item -------------------------------------------------------------------------
    def b_cart(x):
        sid = f"{x['cart_id']}-{x['product_id']}"
        ck, pk = cust.get(x["customer_user_id"]), pkey(x["product_id"], x["added_date"])
        if _bad(rej, "cart_items", sid, customer=ck, product=pk) or \
                not date_ok("cart_items", sid, x["date_key"]):
            return None
        return (x["cart_id"], pk, x["date_key"], ck, x["quantity"])
    emit("fact_cart_item", clean["cart_item"], b_cart)


def load_facts(run_id) -> None:
    with run_ctx(run_id, "load_facts") as r:
        dw = dw_conn()
        try:
            with dw.cursor() as cur:
                t0 = time.perf_counter()
                rows = staging.read(r.stg, LD, "bridge_product_tag", run_id)
                n = _write(cur, "INSERT IGNORE INTO bridge_product_tag (product_key,tag_key) VALUES (%s,%s)",
                           [(x["product_key"], x["tag_key"]) for x in rows]) if rows else 0
                r.m.record("Load", "bridge_product_tag", len(rows), n, 0, time.perf_counter() - t0)

                for table, keys in FACTS.items():
                    t0, cols = time.perf_counter(), S.cols(LD, table)
                    rows = staging.read(r.stg, LD, table, run_id)
                    tuples = [tuple(x[c] for c in cols) for x in rows]
                    n = _write(cur, _upsert_sql(table, cols, keys), tuples) if tuples else 0
                    r.m.record("Load", table, len(rows), n, 0, time.perf_counter() - t0)
            dw.commit()
        except Exception:
            dw.rollback()
            raise
        finally:
            dw.close()


# ----------------------------------------------------------------------------- 4. VALIDATE
def validate(run_id) -> None:
    """Row-count reconciliation across the three layers + proof that staged facts reached the DW.
    Raises (-> task fails -> watermarks never advance) when anything does not add up."""
    with run_ctx(run_id, "validate") as r:
        r.ctl.delete_tests(run_id)
        fails = []

        def check(rule, before, expected, actual):
            ok = expected == actual
            r.ctl.add_test(run_id, rule, before, expected, actual, ok)
            if not ok:
                fails.append(f"{rule}: expected {expected}, got {actual}")

        n = lambda schema, t: staging.count(r.stg, schema, t, run_id)                       # noqa: E731
        rj = lambda src, like: staging.count_rejects(r.stg, run_id, src, like)               # noqa: E731

        for clean_t, raw_t, src in TRANSFORM_RECON:     # stg_extract = stg_transform + rejects
            n_in = n(EXT, raw_t)
            check(f"extract->transform {raw_t}", n_in, n_in, n(TRF, clean_t) + rj(src, "transform%"))
        for fact, clean_t, src in LOAD_RECON:           # stg_transform = stg_load + load-prep rejects
            n_in = n(TRF, clean_t)
            check(f"transform->load {fact}", n_in, n_in, n(LD, fact) + rj(src, "load_prep"))

        dw = dw_read_conn()                             # staged facts must really exist in the DW
        try:
            with dw.cursor() as cur:
                for fact, pk in DW_PRESENCE:
                    ids = sorted({x[pk] for x in staging.read(r.stg, LD, fact, run_id)})
                    found = 0
                    for i in range(0, len(ids), CHUNK):
                        ch = ids[i:i + CHUNK]
                        cur.execute(f"SELECT COUNT(*) AS c FROM {fact} WHERE {pk} IN ({','.join(['%s'] * len(ch))})", ch)
                        found += int(cur.fetchone()["c"])
                    check(f"dw presence {fact}", len(ids), len(ids), found)
        finally:
            dw.close()

        if fails:
            raise ValueError("validation failed: " + "; ".join(fails))
        log.info("validation OK")
"""Layout of the 3 staging layers (MySQL, staging-db). Single source of truth for table/column lists.

  stg_extract   : raw rows exactly as read from OLTP/DW lookups (the old `raw` dict, 1 table per key)
  stg_transform : clean rows (the old `clean` dict, 1 table per key) + rejected_rows
  stg_load      : DW-shaped rows. Dimensions as they will be upserted; facts with surrogate keys
                  already resolved; dim_product carries the SCD2 decision (close_key / effective_from).

Every table gets  row_id (auto, keeps row order)  and  batch_id (= run_id, indexed).
Spec syntax: "column:TYPE ..."   B=BIGINT I=INT D=DECIMAL(12,2) S=VARCHAR(255) T=TEXT DT=DATETIME DA=DATE
"""
import pymysql

TYPES = {"B": "BIGINT", "I": "INT", "D": "DECIMAL(12,2)", "S": "VARCHAR(255)", "T": "TEXT",
         "DT": "DATETIME NULL", "DA": "DATE NULL"}


def _spec(d):
    return {t: [tuple(x.split(":")) for x in s.split()] for t, s in d.items()}


_USER = "id:B email:S full_name:S created_at:DT"

EXTRACT = _spec({
    "users": _USER,
    "seller_users": _USER,
    "addresses": "id:B city:S state:S postal_code:S country:S",
    "categories": "id:I parent_category_id:I name:S",
    "products": "id:B category_id:I seller_id:B name:S description:T current_price:D stock_quantity:I",
    "inventory": "product_id:B seller_id:B stock_quantity:I",
    "coupons": "id:I code:S discount_type:S discount_value:D valid_from:DT valid_until:DT",
    "tags": "id:I name:S",
    "product_tags": "product_id:B tag_id:I",
    "orders": "id:B user_id:B shipping_address_id:B order_date:DT status:S",
    "order_items": "id:B order_id:B product_id:B quantity:I unit_price:D subtotal:D",
    "order_coupons": "order_id:B coupon_id:I discount_applied:D",
    "payments": "id:B order_id:B payment_method:S provider_transaction_id:S amount:D payment_date:DT status:S",
    "shipment": "id:B order_id:B carrier_name:S tracking_number:S shipped_date:DT "
                "estimated_delivery_date:DT actual_delivery_date:DT status:S",
    "returns": "id:I order_item_id:B reason:T return_date:DT refund_amount:D status:S",
    "reviews": "id:I user_id:B product_id:B order_item_id:B rating:I created_at:DT",
    "cart_items": "cart_id:B product_id:B quantity:I added_at:DT",
    "lk_items": "id:B order_id:B product_id:B quantity:I subtotal:D",
    "lk_orders": "id:B user_id:B shipping_address_id:B order_date:DT",
    "lk_products": "id:B seller_id:B",
    "lk_carts": "id:B user_id:B",
})

_COUPON = "coupon_id:I code:S discount_type:S discount_value:D valid_from:DT valid_until:DT"

TRANSFORM = _spec({
    "customer": "user_id:B email:S full_name:S registered_at:DT",
    "seller": "user_id:B email:S full_name:S",
    "geography": "address_id:B city:S state:S postal_code:S country:S",
    "product": "product_id:B name:S description:T category_level1:S category_level2:S category_level3:S "
               "current_price:D",
    "coupon": _COUPON,
    "tag": "tag_id:I name:S",
    "product_tag": "product_id:B tag_id:I",
    "sales": "order_item_id:B order_id:B date_key:I order_date:DA customer_user_id:B seller_user_id:B "
             "product_id:B address_id:B order_status:S quantity:I unit_price:D subtotal:D allocated_discount:D",
    "order_coupon": "order_id:B coupon_id:I date_key:I customer_user_id:B discount_applied:D",
    "payment": "payment_id:B order_id:B provider_transaction_id:S date_key:I customer_user_id:B "
               "payment_method:S payment_status:S amount:D",
    "shipment": "shipment_id:B order_id:B tracking_number:S customer_user_id:B address_id:B carrier_name:S "
                "shipment_status:S shipped_date_key:I estimated_delivery_date_key:I actual_delivery_date_key:I "
                "days_to_ship:I days_to_deliver:I days_late:I",
    "return": "return_id:I order_item_id:B date_key:I return_date:DA customer_user_id:B product_id:B "
              "seller_user_id:B return_status:S reason:T refund_amount:D",
    "review": "review_id:I order_item_id:B date_key:I review_date:DA customer_user_id:B product_id:B rating:I",
    "inventory": "date_key:I snapshot_date:DA product_id:B seller_user_id:B stock_quantity:I",
    "cart_item": "cart_id:B product_id:B date_key:I added_date:DA customer_user_id:B quantity:I",
    "rejected_rows": "stage:S source_table:S source_id:S rule_name:S reason:T",
})

LOAD = _spec({
    "dim_static": "dim_table:S value:S",
    "dim_customer": "user_id:B email:S full_name:S registered_at:DT",
    "dim_seller": "user_id:B email:S full_name:S",
    "dim_geography": "address_id:B city:S state:S postal_code:S country:S",
    "dim_coupon": _COUPON,
    "dim_tag": "tag_id:I name:S",
    "dim_product": "product_id:B name:S description:T category_level1:S category_level2:S category_level3:S "
                   "current_price:D effective_from:DA effective_to:DA close_key:B close_to:DA",
    "bridge_product_tag": "product_key:B tag_key:I",
    "fact_sales": "order_item_id:B order_id:B date_key:I customer_key:B seller_key:B product_key:B "
                  "ship_to_geo_key:B order_status_key:I quantity:I unit_price:D subtotal:D allocated_discount:D",
    "fact_order_coupon": "order_id:B coupon_key:I date_key:I customer_key:B discount_applied:D",
    "fact_payment": "payment_id:B order_id:B provider_transaction_id:S date_key:I customer_key:B "
                    "payment_method_key:I payment_status_key:I amount:D",
    "fact_shipment": "shipment_id:B order_id:B tracking_number:S customer_key:B ship_to_geo_key:B carrier_key:I "
                     "shipment_status_key:I shipped_date_key:I estimated_delivery_date_key:I "
                     "actual_delivery_date_key:I days_to_ship:I days_to_deliver:I days_late:I",
    "fact_return": "return_id:I order_item_id:B date_key:I customer_key:B product_key:B seller_key:B "
                   "return_status_key:I reason:T refund_amount:D",
    "fact_review": "review_id:I order_item_id:B date_key:I customer_key:B product_key:B rating:I",
    "fact_inventory_daily": "date_key:I product_key:B seller_key:B stock_quantity:I",
    "fact_cart_item": "cart_id:B product_key:B date_key:I customer_key:B quantity:I",
})

SCHEMAS = {"stg_extract": EXTRACT, "stg_transform": TRANSFORM, "stg_load": LOAD}


def cols(schema: str, table: str) -> list:
    return [c for c, _ in SCHEMAS[schema][table]]


def ensure(conn) -> None:
    """Create the staging tables if they do not exist (idempotent; called by the init task)."""
    with conn.cursor() as c:
        for schema, tables in SCHEMAS.items():
            try:
                c.execute(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
            except pymysql.MySQLError:
                pass   # no CREATE privilege: the schema must already exist (initdb creates it)
            for table, columns in tables.items():
                body = ", ".join(f"`{n}` {TYPES[t]}" for n, t in columns)
                c.execute(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` ("
                          f"row_id BIGINT AUTO_INCREMENT PRIMARY KEY, batch_id BIGINT NOT NULL, {body}, "
                          f"KEY ix_batch (batch_id))")
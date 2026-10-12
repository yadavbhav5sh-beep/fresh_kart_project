from pyspark.sql.functions import col, to_date

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")


# ============================================================
# 2. TABLE LOCATIONS
# ============================================================

ORDER_ITEMS_SILVER = f"{CATALOG}.silver.order_items_silver"
ORDERS_SILVER = f"{CATALOG}.silver.orders_silver"

DIM_CUSTOMER = f"{CATALOG}.gold.dim_customer"
DIM_DATE = f"{CATALOG}.gold.dim_date"

FACT_TABLE = f"{CATALOG}.gold.fact_order_items"


# ============================================================
# 3. CREATE GOLD SCHEMA IF NOT EXISTS
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")


# ============================================================
# 4. READ SILVER TABLES
# ============================================================

items_df = spark.read.table(ORDER_ITEMS_SILVER)

orders_df = (
    spark.read.table(ORDERS_SILVER)
    .select(
        col("order_id"),
        col("updated_at"),
        col("promised_mins"),
        col("actual_mins"),
        col("delivery_fee"),
        col("payment_method"),
        col("txn_ref"),
        col("items_count"),
        col("total_qty"),
        col("gross_amount"),
        col("discount_amount"),
        col("net_amount"),
    )
)


# ============================================================
# 5. READ GOLD DIMENSIONS
# ============================================================

dim_customer_df = (
    spark.read.table(DIM_CUSTOMER)
    .select("customer_id", "customer_sk")
)

dim_date_df = (
    spark.read.table(DIM_DATE)
    .select("calendar_date", "date_key")
)


# ============================================================
# 6. JOIN ORDER ITEMS WITH ORDERS
# ============================================================

df = items_df.join(
    orders_df,
    on="order_id",
    how="left"
)


# ============================================================
# 7. LOOK UP CUSTOMER SURROGATE KEY
# ============================================================

df = df.join(
    dim_customer_df,
    on="customer_id",
    how="left"
)


# ============================================================
# 8. LOOK UP DATE KEY
# ============================================================

df = df.withColumn(
    "_order_date",
    to_date(col("order_ts"))
)

df = (
    df.join(
        dim_date_df,
        col("_order_date") == col("calendar_date"),
        how="left"
    )
    .drop("_order_date", "calendar_date")
)


# ============================================================
# 9. SELECT FINAL FACT TABLE COLUMNS
# ============================================================

df = df.select(
    # Foreign keys
    col("date_key"),
    col("customer_sk"),
    col("store_id"),
    col("product_id"),

    # Order identifiers and status
    col("order_id"),
    col("line_number"),
    col("order_status"),
    col("payment_method"),
    col("txn_ref"),

    # Timestamps
    col("order_ts"),
    col("updated_at"),

    # Line-item measures
    col("qty"),
    col("unit_price"),
    col("discount"),
    col("line_amount"),

    # Order-level measures
    col("delivery_fee"),
    col("promised_mins"),
    col("actual_mins"),
    col("items_count"),
    col("total_qty"),
    col("gross_amount"),
    col("discount_amount"),
    col("net_amount"),
)


# ============================================================
# 10. WRITE GOLD FACT TABLE
# ============================================================

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FACT_TABLE)

# ============================================================
# 11. VERIFY THE TABLE
# ============================================================

print(f"Gold fact table created successfully: {FACT_TABLE}")

display(spark.read.table(FACT_TABLE))
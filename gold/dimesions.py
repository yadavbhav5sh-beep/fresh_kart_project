from pyspark.sql import functions as F

# Catalog and schema
catalog_name = "fk_1374"
gold_schema = f"{catalog_name}.gold"
silver_schema = f"{catalog_name}.silver"

# Create Gold schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

# =========================================================
# 1. GOLD DIM DATE
# =========================================================

dim_date = (
    spark.sql("""
        SELECT explode(
            sequence(
                to_date('2026-03-01'),
                to_date('2026-03-31'),
                interval 1 day
            )
        ) AS calendar_date
    """)
    .withColumn(
        "date_key",
        F.date_format("calendar_date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "day_name",
        F.date_format("calendar_date", "EEEE")
    )
    .withColumn(
        "iso_week",
        F.weekofyear("calendar_date")
    )
    .withColumn(
        "is_weekend",
        F.dayofweek("calendar_date").isin([1, 7])
    )
)

dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.dim_date")


# =========================================================
# 2. GOLD DIM STORE
# =========================================================

dim_store = spark.read.table(
    f"{silver_schema}.stores"
)

dim_store.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.dim_store")

# =========================================================
# 3. GOLD DIM PRODUCT
# =========================================================

dim_product = spark.read.table(
    f"{silver_schema}.products"
)

product_columns = [
    "brand",
    "is_active",
    "mrp",
    "product_id",
    "product_name",
    "selling_price",
    "snapshot_date",
    "category_l1",
    "category_l2",
    "pack_size",
    "pack_unit"
]

dim_product = dim_product.select(*product_columns)

dim_product.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.dim_product")


# =========================================================
# 4. GOLD DIM CUSTOMER
# =========================================================

dim_customer = spark.read.table(
    f"{silver_schema}.customers"
)

dim_customer = dim_customer.withColumn(
    "customer_sk",
    F.monotonically_increasing_id()
)

dim_customer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.dim_customer")

# =========================================================
# 5. VERIFY GOLD TABLES
# =========================================================

spark.sql(f"SHOW TABLES IN {gold_schema}").show(truncate=False)

display(spark.read.table(f"{gold_schema}.dim_date"))
display(spark.read.table(f"{gold_schema}.dim_store"))
display(spark.read.table(f"{gold_schema}.dim_product"))
display(spark.read.table(f"{gold_schema}.dim_customer"))
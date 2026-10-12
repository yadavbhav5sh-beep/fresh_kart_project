from pyspark.sql.functions import col, trim, upper, initcap, lower, to_date, lit, row_number, when, coalesce, regexp_replace, expr

from pyspark.sql.window import Window


def standardize_orders(df):
    """
    Standardize columns as per schema requirements:
    - Trim all strings
    - Uppercase order_id, store_id, customer_id
    - Lowercase order_status
    - Convert order_ts, updated_at to TIMESTAMP
    """
    return (
        df
        .withColumns({
            "order_id": upper(trim(col("order_id"))).cast("string"),
            "customer_id": upper(trim(col("customer_id"))).cast("string"),
            "store_id": upper(trim(col("store_id"))).cast("string"),
            "order_status": lower(trim(col("order_status"))),
            "order_ts": to_date(col("order_ts")),
            "updated_at": to_date(col("updated_at")),
        })
    )

def unpack_order_items(df):
    """
    Explode items array and unpack columns for silver.order_items
    """
    from pyspark.sql.functions import explode
    return (
        df
        .select("order_id", "items")
        .withColumn("item", explode(col("items")))
        .select(
            col("order_id"),
            col("item.product_id").alias("product_id"),
            col("item.qty").alias("qty"),
            col("item.unit_price").alias("unit_price"),
            col("item.discount").alias("discount")
        )
    )

def quarantine_and_load_orders(spark):
    """
    Clean fk_1374.bronze.orders, validate, and split into:
    - Valid orders/items (referential integrity: customer_id & product_id in silver tables)
    - Quarantine for rejects
    """
    # Read silver master tables
    customers = spark.table("fk_1374.silver.customers")
    products = spark.table("fk_1374.silver.products")
    bronze_orders = spark.table("fk_1374.bronze.orders")

    # Standardize orders
    std_orders = standardize_orders(bronze_orders)

    # Referential checks
    valid_orders = std_orders.join(customers, "customer_id", "inner")
    valid_order_items = unpack_order_items(valid_orders).join(products, "product_id", "inner")

    from pyspark.sql.functions import monotonically_increasing_id, current_timestamp
    order_items = (
        valid_order_items
        .withColumn("line_number", monotonically_increasing_id() % 10)
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("quantity", expr("try_cast(qty as int)"))
        .withColumn("discount_amount", coalesce(col("discount").cast("decimal(10,2)"), lit(0)))
        .withColumn("gross_amount", col("unit_price") * col("quantity"))
        .withColumn("net_amount", col("gross_amount") - col("discount_amount"))
        .select(
            "order_id", "line_number", "product_id", "quantity", "unit_price", 
            "discount_amount", "gross_amount", "net_amount")
    )

    # Quarantine for invalid customer_id or product_id
    invalid_orders = std_orders.join(customers, "customer_id", "left_anti")
    invalid_items = unpack_order_items(std_orders).join(products, "product_id", "left_anti")

    quarantine_orders = (
        invalid_orders
        .withColumn("reject_reason", lit("customer_id not found"))
        .withColumn("raw_record", col("order_id"))  # Ideally serialize full row
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("updated_at", col("updated_at"))
        .select("order_id", "reject_reason", "updated_at", "raw_record", "quarantined_at")
    )
    quarantine_items = (
        invalid_items
        .withColumn("reject_reason", lit("product_id not found"))
        .withColumn("raw_record", col("order_id"))
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("updated_at", lit(None))
        .select("order_id", "reject_reason", "updated_at", "raw_record", "quarantined_at")
    )

    # Write (merge/upsert logic should be used in production)

    order_items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fk_1374.silver.order_items")
    quarantine_orders.write.mode("append").saveAsTable("fk_1374.silver.quarantine_orders")
    quarantine_items.write.mode("append").saveAsTable("fk_1374.silver.quarantine_orders")

    return order_items.count(), quarantine_orders.count(), quarantine_items.count()

# --- execute ---
order_items_count, quarantine_orders_count, quarantine_items_count = quarantine_and_load_orders(spark)
print(f"Order items written: {order_items_count}")
print(f"Quarantine orders: {quarantine_orders_count}")
print(f"Quarantine items: {quarantine_items_count}")
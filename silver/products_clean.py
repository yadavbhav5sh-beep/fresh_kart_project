from pyspark.sql.functions import col, trim, upper, initcap, lower, to_date, lit, transform, row_number, when
from pyspark.sql.window import Window

def standardize_products(df):
    """
    Apply standardization rules for data quality:
    - Trim whitespace from all string columns
    - product_id: uppercase string (enforce type + casing)
    - brand, product_name: trim
    - category: unnest to category_l1, category_l2 (initcap + trim)
    - pack: unnest to pack_size (long), pack_unit (lower + trim)
    - tags: trim each element, keep as array<string>
    - snapshot_date: convert from string to DATE type
    - mrp, selling_price: cast to decimal(10,2)
    - is_active: cast to boolean
    """
    return (
        df
        .withColumns({
            "product_id": upper(trim(col("product_id"))).cast("string"),
            "brand": trim(col("brand")),
            "product_name": trim(col("product_name")),
            "category_l1": initcap(trim(col("category.l1"))),
            "category_l2": initcap(trim(col("category.l2"))),
            "pack_size": col("pack.size").cast("int"),
            "pack_unit": lower(trim(col("pack.unit"))).cast("string"),
            "tags": transform(col("tags"), lambda x: trim(x)),
            "snapshot_date": to_date(col("snapshot_date"), "yyyy-MM-dd"),
            "mrp": col("mrp").cast("decimal(10,2)"),
            "selling_price": col("selling_price").cast("decimal(10,2)"),
            "is_active": col("is_active").cast("boolean"),
        })
        .drop("category", "pack")
    )

def quarantine_and_load_products(spark, bronze_table="fk_1374.bronze.products"):
    """
    Read bronze products, standardize, deduplicate, then split valid vs invalid records:
    - Records with NULL product_id → quarantine
    - Duplicate product_id with older snapshot_date → quarantine
    - Records with NULL product_name → quarantine
    - Records with non-positive mrp or selling_price → quarantine
    - Valid records (latest snapshot, all checks pass) → fk_1374.silver.products
    """
    # ensure schemas exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.quarantine")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.silver")

    # read & standardize bronze data
    df = spark.table(bronze_table)
    df_clean = standardize_products(df)

    # mark duplicates: _rn == 1 is latest snapshot per product_id
    df_marked = (
        df_clean
        .withColumns({
            "_rn": row_number().over(Window.partitionBy("product_id").orderBy(col("snapshot_date").desc_nulls_last()))
        })
    )

    # quarantine: NULL product_id, older duplicates, NULL product_name, non-positive prices
    df_quarantine = (
        df_marked
        .filter(
            col("product_id").isNull()
            | (col("_rn") > 1)
            | col("product_name").isNull()
            | (col("mrp") <= 0)
            | (col("selling_price") <= 0)
        )
        .withColumns({
            "_quarantine_reason": when(col("product_id").isNull(), lit("NULL product_id"))
                .when(col("_rn") > 1, lit("Duplicate product_id, older snapshot"))
                .when(col("product_name").isNull(), lit("NULL product_name"))
                .when(col("mrp") <= 0, lit("Non-positive MRP"))
                .when(col("selling_price") <= 0, lit("Non-positive selling_price"))
                .otherwise(lit("Unknown"))
        })
        .drop("_rn")
    )

    # valid: product_id present, latest snapshot, product_name present, positive prices
    df_valid = (
        df_marked
        .filter(
            col("product_id").isNotNull()
            & (col("_rn") == 1)
            & col("product_name").isNotNull()
            & (col("mrp") > 0)
            & (col("selling_price") > 0)
        )
        .drop("_rn", "_source_file", "_ingestion_at")
    )

    # write quarantined records
    df_quarantine.write.mode("append").option("mergeSchema", "true").saveAsTable("fk_1374.quarantine.products")

    # write valid records to silver
    df_valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fk_1374.silver.products")

    return df_quarantine.count(), df_valid.count()

# --- execute ---
quarantined_count, valid_count = quarantine_and_load_products(spark)
print(f"Quarantined records: {quarantined_count}")
print(f"Valid records written to silver: {valid_count}")
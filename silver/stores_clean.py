from pyspark.sql.functions import col, trim, upper, initcap, to_date, lit, current_timestamp

def standardize_stores(df):
    """
    Apply standardization rules for data quality:
    - Trim whitespace from all string columns
    - store_id: uppercase string (enforce type + casing)
    - city, state, region: title case (initcap) for consistency
    - opening_date: convert from string to DATE type
    - Drop exact duplicate records
    """
    return (
        df
        .withColumns({
            "store_id": upper(trim(col("store_id"))).cast("string"),
            "store_name": trim(col("store_name")),
            "city": initcap(trim(col("city"))),
            "state": initcap(trim(col("state"))),
            "region": initcap(trim(col("region"))),
            "opening_date": to_date(col("opening_date"), "yyyy-MM-dd"),
        })
        .dropDuplicates()
    )


def quarantine_and_load_stores(spark, bronze_table="fk_1374.bronze.stores"):
    """
    Read bronze stores, standardize, then split valid vs invalid records:
    - Records with NULL store_id  → fk_1374.quarantine.stores (with _quarantine_reason)
    - Valid records (store_id present) → fk_1374.silver.stores
    """
    # ensure schemas exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.quarantine")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.silver")

    # read & standardize bronze data
    df = spark.table(bronze_table)
    df_clean = standardize_stores(df)

    # quarantine: records where store_id is NULL
    df_quarantine = (
        df_clean
        .filter(col("store_id").isNull())
        .withColumns({"_quarantine_reason": lit("NULL store_id")})
    )

    # valid: records where store_id is NOT NULL
    df_valid = df_clean.filter(col("store_id").isNotNull())

    # drop meta data columns
    df_valid = df_valid.drop("_source_file", "_ingestion_at")

    # write quarantined records
    df_quarantine.write.mode("append").option("mergeSchema", "true").saveAsTable("fk_1374.quarantine.stores")

    # write valid records to silver
    df_valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fk_1374.silver.stores")

    return df_quarantine.count(), df_valid.count()

# --- execute ---
quarantined_count, valid_count = quarantine_and_load_stores(spark)
print(f"Quarantined records: {quarantined_count}")
print(f"Valid records written to silver: {valid_count}")
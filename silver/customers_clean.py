from pyspark.sql.functions import col, trim, upper, initcap, lower, to_date, lit, row_number, when, coalesce, regexp_replace
from pyspark.sql.window import Window

def standardize_customers(df):
    """
    Apply standardization rules for data quality:
    - Trim whitespace from all string columns
    - customer_id: uppercase string (enforce type + casing)
    - full_name: trim
    - email: lowercase + trim (standard format validated in quarantine)
    - phone: trim, remove non-digit characters
    - city: initcap + trim
    - tier: uppercase + trim
    - signup_date: convert to DATE (handle dd/MM/yyyy and yyyy-MM-dd formats)
    """
    return (
        df
        .withColumns({
            "customer_id": upper(trim(col("customer_id"))).cast("string"),
            "full_name": trim(col("full_name")),
            "email": lower(trim(col("email"))),
            "phone": regexp_replace(trim(col("phone")), "[^0-9]", ""),
            "city": initcap(trim(col("city"))),
            "tier": upper(trim(col("tier"))),
            "signup_date": when(col("signup_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), to_date(col("signup_date"), "yyyy-MM-dd"))
                .otherwise(to_date(col("signup_date"), "dd/MM/yyyy")),
        })
    )

def quarantine_and_load_customers(spark, bronze_table="fk_1374.bronze.customers"):
    """
    Read bronze customers, standardize, deduplicate, then split valid vs invalid records:
    - Records with NULL customer_id → quarantine
    - Duplicate customer_id with older signup_date → quarantine
    - Records with NULL or invalid email format → quarantine
    - Records with NULL full_name → quarantine
    - Valid records (latest signup, all checks pass) → fk_1374.silver.customers
    """
    # ensure schemas exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.quarantine")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fk_1374.silver")

    # read & standardize bronze data
    df = spark.table(bronze_table)
    df_clean = standardize_customers(df)

    # mark duplicates: _rn == 1 is latest signup_date per customer_id
    df_marked = (
        df_clean
        .withColumns({
            "_rn": row_number().over(Window.partitionBy("customer_id").orderBy(col("signup_date").desc_nulls_last()))
        })
    )

    # email format validation: standard email pattern (already lowercased)
    email_valid = col("email").rlike(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')

    # quarantine: NULL customer_id, older duplicates, NULL/invalid email, NULL full_name
    df_quarantine = (
        df_marked
        .filter(
            col("customer_id").isNull()
            | (col("_rn") > 1)
            | col("email").isNull()
            | ~email_valid
            | col("full_name").isNull()
        )
        .withColumns({
            "_quarantine_reason": when(col("customer_id").isNull(), lit("NULL customer_id"))
                .when(col("_rn") > 1, lit("Duplicate customer_id, older signup_date"))
                .when(col("email").isNull(), lit("NULL email"))
                .when(~email_valid, lit("Invalid email format"))
                .when(col("full_name").isNull(), lit("NULL full_name"))
                .otherwise(lit("Unknown"))
        })
        .drop("_rn")
    )

    # valid: customer_id present, latest signup, email valid, full_name present
    df_valid = (
        df_marked
        .filter(
            col("customer_id").isNotNull()
            & (col("_rn") == 1)
            & col("email").isNotNull()
            & email_valid
            & col("full_name").isNotNull()
        )
        .drop("_rn", "_source_file", "_ingestion_at")
    )

    # write quarantined records
    df_quarantine.write.mode("append").option("mergeSchema", "true").saveAsTable("fk_1374.quarantine.customers")

    # write valid records to silver
    df_valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fk_1374.silver.customers")

    return df_quarantine.count(), df_valid.count()

# --- execute ---
quarantined_count, valid_count = quarantine_and_load_customers(spark)
print(f"Quarantined records: {quarantined_count}")
print(f"Valid records written to silver: {valid_count}")
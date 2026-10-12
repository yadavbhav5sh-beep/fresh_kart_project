from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, when, col

spark = SparkSession.builder.getOrCreate()

items = spark.table("fk_1374.silver.order_items")

# If additional columns needed, adjust below
agg = items.groupBy("order_id")\
    .agg(
        sum(col("quantity")).alias("units_sold"),
        sum(col("gross_amount")).alias("gross_amount"),
        sum(col("discount_amount")).alias("discount_amount"),
        sum(col("net_amount")).alias("net_items_revenue")
    )

# If you want store-level/date-level: you'll need order-level info from elsewhere (orders table/join/source file update)


agg.write.mode("overwrite").saveAsTable("fk_1374.gold.avg_daily_store_sales")

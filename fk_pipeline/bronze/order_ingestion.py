from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(
    name="fk_1374.bronze.orders",
    comment="Bronze layer: raw orders data ingested via Auto Loader from JSON files",
)
def orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", "/Volumes/fk_1374/ops/pipeline_state/orders/")
        .load("/Volumes/fk_1374/landing/raw/orders/")
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )
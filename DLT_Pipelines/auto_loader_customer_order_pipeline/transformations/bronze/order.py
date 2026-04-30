from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp


@dp.table(name="bronze_orders")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "s3://customers-orders-bucket-v1/schema/order")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://customers-orders-bucket-v1/orders")
    )
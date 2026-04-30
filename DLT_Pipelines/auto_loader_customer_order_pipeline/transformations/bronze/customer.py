from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

#here we are reading data from cloud s3 using auto-loader

@dp.table(name="bronze_customers")  # this will create a streaming table for customers 
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "s3://customers-orders-bucket-v1/schema/customer")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("s3://customers-orders-bucket-v1/customers")
    )
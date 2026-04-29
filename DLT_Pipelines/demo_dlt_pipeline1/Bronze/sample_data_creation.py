
from pyspark import pipelines as dp
@dp.table(
    name= "demo.bronze.mt_vw_customers",
    comment="create materialized view for customers"
)
def fun():
    df = spark.read.table("demo.bronze.bronze_customers")
    return df

# streaming table for orders

@dp.table(
    name = "demo.bronze.st_tb_orders",
    comment="creating streaming table for orders"
)
def fun():
    df = spark.readStream.table("demo.bronze.bronze_orders")
    return df
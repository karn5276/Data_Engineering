from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="demo.silver.orders",
    comment="Cleaned streaming orders data"
)
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_orders():
    return (
        spark.readStream.table("demo.bronze.st_tb_orders")
    )


dp.create_streaming_table(
    name="demo.gold.main_orders",
    comment="order data with SCD Type 1 Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)

dp.create_auto_cdc_flow(
    target="demo.gold.main_orders",
    source="demo.silver.orders",
    keys=["order_id"],
    sequence_by=col("order_date"),
    stored_as_scd_type="1"   # acts like upsert, if amount updated
)
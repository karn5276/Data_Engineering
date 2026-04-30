from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name="demo.silver.customer",
    comment="Cleaned materialized customer data"
)
@dp.expect("valid_name","name is not null")
def silver_orders():
    return (
        spark.read.table("demo.bronze.mt_vw_customers")
    )

dp.create_streaming_table(
    name="demo.gold.main_customer",
    comment="Customer data with SCD Type 2",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)

dp.create_auto_cdc_flow(
    target="demo.gold.main_customer",
    source="demo.silver.customer",
    keys=["customer_id"],
    sequence_by=col("customer_id"),
    stored_as_scd_type=2
)
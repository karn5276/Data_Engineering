from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="demo.silver.customer",
    comment="Cleaned materialized customer data"
)
@dp.expect("valid_name","name is not null")
def silver_orders():
    return (
        spark.read.table("demo.bronze.mt_vw_customers")
    )

# Create final Silver table
dp.create_table(   # .............................. 3
    name="demo.silver.dim_customers_scd2",
    comment="Cleaned and validated customer data with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="demo.silver.dim_customers_scd2",
    source="demo.silver.customer",
    keys=["customer_id"],
    sequence_by=col("customer_id"),
    stored_as_scd_type="2"
)

from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.view(
    name="customer_bronze_vw"
)
def fun():
    df = spark.readStream.table("customer_orders.bronze.new_customers")
    return df



# create here scd2 logic

dp.create_streaming_table("customer_scd2_bronze")

dp.create_auto_cdc_flow(
    target="customer_scd2_bronze",
    source="customer_bronze_vw",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)
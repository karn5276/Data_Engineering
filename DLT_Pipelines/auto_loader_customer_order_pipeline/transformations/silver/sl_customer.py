from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

@dp.view(name="silver_customers_clean")
def silver_customers_clean():
    return (
        spark.readStream.table("bronze_customers")
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("updated_at", to_timestamp("updated_at"))
    )



# =====================================================
# 5. SCD TYPE 2 FOR CUSTOMERS
# =====================================================

dp.create_streaming_table("silver_customers_scd2") # this will be our final customer table.
dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="silver_customers_clean",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)
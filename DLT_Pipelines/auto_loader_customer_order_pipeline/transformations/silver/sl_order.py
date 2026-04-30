from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

# =====================================================
# 4. SILVER LAYER (ORDERS CLEAN - STREAMING)
# =====================================================
@dp.table(name="silver_orders_clean")
def silver_orders_clean():
    return (
        spark.readStream.table("bronze_orders")
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("order_time", to_timestamp("order_time"))
    )


# =====================================================
# SCD TYPE 1 FOR ORDERS (UPSERT - KEEP LATEST ONLY)
# =====================================================
dp.create_streaming_table("silver_orders_scd1")
dp.create_auto_cdc_flow(
    target="silver_orders_scd1",
    source="silver_orders_clean",
    keys=["order_id"],              # unique key
    sequence_by=col("order_time"),  # latest record wins
    stored_as_scd_type=1            # 👈 SCD1
)
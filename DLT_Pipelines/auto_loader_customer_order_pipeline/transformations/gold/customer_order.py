from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# =====================================================
# 6. GOLD LAYER (JOIN CUSTOMERS + ORDERS)
# =====================================================
@dp.materialized_view(name="gold_customer_orders")
def gold_customer_orders():
    customers = spark.table("customer_orders.autoloader_demo.silver_customers_scd2")
    orders = spark.table("customer_orders.autoloader_demo.silver_orders_scd1")

 # perform broadcast join on customers table
    result = broadcast(customers).alias("c") \
        .join(
            orders.alias("o"),
            F.col("c.customer_id") == F.col("o.customer_id"),
            "left"
        ) \
        .groupBy(
            F.col("c.customer_id"),
            F.col("c.name")
        ) \
        .agg(
            F.count("o.order_id").alias("total_orders"),
            F.sum("o.amount").alias("total_spent"),
            F.max("o.order_time").alias("last_order_time")
        )

    return result



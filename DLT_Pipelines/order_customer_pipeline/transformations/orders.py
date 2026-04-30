
from pyspark import pipelines as dp

# streaming table for orders

@dp.table(
    name = "order_bronze",
    comment="creating streaming table for orders"
)
def fun():
    df = spark.readStream.table("demo.bronze.bronze_orders")
    return df

# joined two table and create materialzie view

@dp.materialized_view(name = "order_customer_join")
def joined_vw():
    df_c = spark.read.table("customer_scd2_bronze")
    df_o = spark.read.table("order_bronze")

    df = df_o.join(df_c, df_o.customer_id == df_c.customer_id, "left").drop(df_c.customer_id)
    return df
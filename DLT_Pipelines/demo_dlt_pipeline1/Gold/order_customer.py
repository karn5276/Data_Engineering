from pyspark import pipelines as dp

@dp.view(
name="dim_vw_customer_staging",
comment="creating temporary view to join two tables"
)
def fun():
    df = spark.read.table("demo.silver.dim_customers_scd2")
    return df

@dp.view(
    name="dim_vw_orders_staging",
    comment="creating temporary view to join two tables"
)
def fun():
    df = spark.readStream.table("demo.silver.main_orders")
    return df


@dp.table(
    name="demo.gold.main_customer_order",
    comment="created joined customer_order_table"
)
def fun():
    df_c = spark.read.table("dim_vw_customer_staging")
    df_o = spark.readStream.table("dim_vw_orders_staging")
    df = df_c.join(df_o, df_c.customer_id == df_o.customer_id).drop(df_c.customer_id)
    return df
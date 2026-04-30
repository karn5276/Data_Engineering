from pyspark import pipelines as dp


# Please edit the sample below


dp.create_sink(
    name="delta_sink",
    format="delta",
    options={"tableName": "`customer_orders`.`default`.order_customer_delta_sink"}
)


@dp.append_flow(target="delta_sink")
def append_to_delta_sink():
    return spark.readStream.table("order_customer_join")
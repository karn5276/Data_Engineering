from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

# =====================================================
# 6. GOLD LAYER (JOIN CUSTOMERS + ORDERS)
# =====================================================
@dp.materialized_view(name="gold_customer_orders")
def gold_customer_orders():
    return spark.sql("""
        SELECT 
            c.customer_id,
            c.name,
            COUNT(o.order_id) AS total_orders,
            SUM(o.amount) AS total_spent,
            MAX(o.order_time) AS last_order_time
        FROM customer_orders.autoloader_demo.silver_customers_scd2 c
        LEFT JOIN customer_orders.autoloader_demo.silver_orders_scd1 o
        ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name
    """)
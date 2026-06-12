from src.ingestion.read_hdfs import spark
spark.sql("""
    WITH order_groups AS (
        SELECT
            Order_ID,
            Ship_Mode,
            Sales,
            Profit,
            Shipping_Cost,
            CASE
                WHEN Sales < 100 THEN 'Low Value'
                WHEN Sales < 500 THEN 'Medium Value'
                ELSE 'High Value'
            END AS order_group
        FROM superstore
    ),
    ship_stats AS (
        SELECT
            order_group,
            Ship_Mode,
            COUNT(*) AS total_orders,
            ROUND(AVG(Sales),2) AS avg_sales,
            ROUND(AVG(Profit),2) AS avg_profit,
            ROUND(AVG(Shipping_Cost),2) AS avg_shipping_cost
        FROM order_groups
        GROUP BY
            order_group,
            Ship_Mode
    )
    SELECT *
     FROM (
        SELECT *,
            DENSE_RANK() OVER (
                PARTITION BY order_group
                ORDER BY total_orders DESC
            ) AS ranking
        FROM ship_stats
    )
    WHERE ranking <= 2
""").show(20,truncate=False)

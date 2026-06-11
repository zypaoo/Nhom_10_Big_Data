from src.ingestion.read_hdfs import spark

spark.sql("""
WITH customer_sales AS (

    SELECT
        Customer_ID,
        Customer_Name,
        ROUND(SUM(Sales), 2) AS total_sales
    FROM superstore
    GROUP BY
        Customer_ID,
        Customer_Name
),
ranked_customers AS (
    SELECT
        Customer_ID,
        Customer_Name,
        total_sales,
        SUM(total_sales) OVER (
            ORDER BY total_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW
        ) AS running_sales,
        SUM(total_sales) OVER () AS grand_total,
        ROW_NUMBER() OVER (
            ORDER BY total_sales DESC
        ) AS customer_rank
    FROM customer_sales
)
SELECT
    Customer_ID,
    Customer_Name,
    total_sales,
    ROUND(running_sales, 2) AS running_sales,
    ROUND(
        running_sales * 100.0 / grand_total,
        2
    ) AS cumulative_sales_pct,
    customer_rank
FROM ranked_customers
ORDER BY total_sales DESC
""").show(20,truncate=False)
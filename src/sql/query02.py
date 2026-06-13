from src.ingestion.read_hdfs import spark

spark.sql("""
    SELECT * FROM (
        SELECT Sub_Category, ROUND(SUM(CAST(Profit AS DOUBLE)), 2) AS total_profit, 'Top High' AS group_label
        FROM superstore
        WHERE Profit IS NOT NULL AND Sub_Category IS NOT NULL
        GROUP BY Sub_Category
        ORDER BY total_profit DESC
        LIMIT 5
    ) AS t1

    UNION ALL

    SELECT * FROM (
        SELECT Sub_Category, ROUND(SUM(CAST(Profit AS DOUBLE)), 2) AS total_profit, 'Top Low' AS group_label
        FROM superstore
        WHERE Profit IS NOT NULL AND Sub_Category IS NOT NULL
        GROUP BY Sub_Category
        ORDER BY total_profit ASC
        LIMIT 5
    ) AS t2

    ORDER BY total_profit DESC
""").show(20, truncate=False)

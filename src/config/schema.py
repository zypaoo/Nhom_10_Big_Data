from pyspark.sql.types import *

SUPERSTORE_SCHEMA = StructType([
    StructField("Category", StringType()),
    StructField("City", StringType()),
    StructField("Country", StringType()),
    StructField("Customer_ID", StringType()),
    StructField("Customer_Name", StringType()),
    StructField("Discount", DoubleType()),
    StructField("Market", StringType()),
    StructField("Order_Date", DateType()),
    StructField("Order_ID", StringType()),
    StructField("Order_Priority", StringType()),
    StructField("Product_ID", StringType()),
    StructField("Product_Name", StringType()),
    StructField("Profit", DoubleType()),
    StructField("Quantity", IntegerType()),
    StructField("Region", StringType()),
    StructField("Sales", DoubleType()),
    StructField("Segment", StringType()),
    StructField("Ship_Date", DateType()),
    StructField("Ship_Mode", StringType()),
    StructField("Shipping_Cost", DoubleType()),
    StructField("State", StringType()),
    StructField("Sub_Category", StringType()),
    StructField("Year", IntegerType()),
    StructField("weeknum", IntegerType())
])
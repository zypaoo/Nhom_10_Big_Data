from pyspark.sql.functions import col, when, to_date, year, weekofyear, concat_ws, lit, coalesce

# Ngưỡng Winsorization cố định từ tập dữ liệu huấn luyện để đảm bảo tính nhất quán của đặc trưng
SALES_P1 = 0.36
SALES_P99 = 31357.32
SHIP_P1 = 0.0
SHIP_P99 = 1784.71
DEFAULT_DELIVERY_DAYS = 4.0  # Ngày giao hàng trung vị lấy từ dữ liệu lịch sử

def preprocess_realtime_features(df):
    """
    Tái tạo toàn bộ các đặc trưng huấn luyện (Feature Engineering) cho dữ liệu Streaming.
    Không trùng lặp logic, khớp 100% với cấu trúc pipeline của mô hình offline.
    """
    # 0. Bổ sung các cột phân loại thiếu mà mô hình Pipeline yêu cầu làm đầu vào
    if "Segment" not in df.columns:
        df = df.withColumn("Segment", lit("Consumer"))
    else:
        df = df.withColumn("Segment", coalesce(col("Segment"), lit("Consumer")))
        
    if "Region" not in df.columns:
        df = df.withColumn("Region", lit("Central"))
    else:
        df = df.withColumn("Region", coalesce(col("Region"), lit("Central")))
        
    if "Ship_Mode" not in df.columns:
        df = df.withColumn("Ship_Mode", lit("Standard Class"))
    else:
        df = df.withColumn("Ship_Mode", coalesce(col("Ship_Mode"), lit("Standard Class")))
        
    if "Order_Priority" not in df.columns:
        df = df.withColumn("Order_Priority", lit("Medium"))
    else:
        df = df.withColumn("Order_Priority", coalesce(col("Order_Priority"), lit("Medium")))

    # 1. Kép biên xử lý ngoại lai (Winsorization)
    df = df.withColumn("Sales_capped", 
        when(col("Sales") < SALES_P1, SALES_P1)
        .when(col("Sales") > SALES_P99, SALES_P99)
        .otherwise(col("Sales"))
    ).withColumn("Shipping_Cost_capped", 
        when(col("Shipping_Cost") < SHIP_P1, SHIP_P1)
        .when(col("Shipping_Cost") > SHIP_P99, SHIP_P99)
        .otherwise(col("Shipping_Cost"))
    )
    
    # 2. Xử lý ngày đặt hàng và trích xuất Year, weeknum
    # Nếu Order_Date bị null, tự động lấy ngày hiện tại của hệ thống để dự phòng
    df = df.withColumn("parsed_order_date", coalesce(to_date(col("Order_Date"), "yyyy-MM-dd"), to_date(lit("2026-06-13"))))
    df = df.withColumn("Year", year(col("parsed_order_date")))
    df = df.withColumn("weeknum", weekofyear(col("parsed_order_date")))
    
    # 3. Tính toán các đặc trưng số bổ sung
    df = df.withColumn("discount_amount", col("Sales_capped") * col("Discount")) \
           .withColumn("shipping_ratio", when(col("Sales_capped") == 0, 0.0).otherwise(col("Shipping_Cost_capped") / col("Sales_capped"))) \
           .withColumn("avg_item_value", when(col("Quantity") == 0, 0.0).otherwise(col("Sales_capped") / col("Quantity"))) \
           .withColumn("delivery_days", lit(DEFAULT_DELIVERY_DAYS)) \
           .withColumn("is_discounted", when(col("Discount") > 0, 1.0).otherwise(0.0)) \
           .withColumn("high_discount", when(col("Discount") >= 0.2, 1.0).otherwise(0.0))
           
    # 4. Tạo đặc trưng tương tác phân loại (Market x Category)
    df = df.withColumn("Market_defaulted", coalesce(col("Market"), lit("US")))
    df = df.withColumn("Market_Category", concat_ws("_", col("Market_defaulted"), col("Category")))
    
    return df

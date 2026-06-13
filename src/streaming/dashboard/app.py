import streamlit as st
import pandas as pd
import json
import threading
import time
import os
import sys
import random
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.config.cluster_config import KAFKA_BOOTSTRAP_SERVER, MODE
from src.streaming.kafka.order_producer import OrderProducer
from kafka import KafkaConsumer

# Set Page Config
st.set_page_config(
    page_title="Realtime Profit Intelligence Platform",
    page_icon=None,
    layout="wide"
)

# Thư mục chứa ảnh và logo (tùy chọn)
IMAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "images", "ml", "ml2"))
os.makedirs(IMAGE_DIR, exist_ok=True)

# Khởi tạo hai luồng chạy ngầm để lắng nghe Kafka Request Topic và Result Topic
@st.cache_resource
def start_kafka_consumers():
    requests = []
    predictions = []
    lock = threading.Lock()
    
    def consume_requests():
        try:
            print(f"Connecting to Kafka Request Broker at {KAFKA_BOOTSTRAP_SERVER}...")
            consumer = KafkaConsumer(
                "profit_prediction_requests",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=1000
            )
            print("Kafka Request Consumer Thread successfully started.")
            while True:
                for message in consumer:
                    payload = message.value
                    with lock:
                        requests.append(payload)
                        # Giới hạn lưu 100 bản tin gần nhất
                        if len(requests) > 100:
                            requests.pop(0)
                time.sleep(0.5)
        except Exception as e:
            print(f"Error in Kafka request consumer thread: {e}")
            
    def consume_predictions():
        try:
            print(f"Connecting to Kafka Result Broker at {KAFKA_BOOTSTRAP_SERVER}...")
            consumer = KafkaConsumer(
                "profit_prediction_results",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=1000
            )
            print("Kafka Result Consumer Thread successfully started.")
            while True:
                for message in consumer:
                    payload = message.value
                    with lock:
                        predictions.append(payload)
                        # Giới hạn lưu 100 bản tin gần nhất
                        if len(predictions) > 100:
                            predictions.pop(0)
                time.sleep(0.5)
        except Exception as e:
            print(f"Error in Kafka result consumer thread: {e}")
            
    t1 = threading.Thread(target=consume_requests, daemon=True)
    t2 = threading.Thread(target=consume_predictions, daemon=True)
    t1.start()
    t2.start()
    return requests, predictions, lock

# Khởi chạy các consumer
requests_list, predictions_list, data_lock = start_kafka_consumers()

# Cấu hình danh mục sản phẩm và mặt hàng tương ứng
CATEGORY_SUBCAT_MAP = {
    "Office Supplies": ["Paper", "Binders", "Appliances", "Art", "Envelopes", "Fasteners", "Labels", "Storage", "Supplies"],
    "Technology": ["Phones", "Accessories", "Copiers", "Machines"],
    "Furniture": ["Bookcases", "Chairs", "Furnishings", "Tables"]
}

# --- GIAO DIỆN WEB ---

# Custom CSS for Premium Light Theme with explicit element color overrides to prevent invisible text
st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
    
    /* Global Styles */
    html, body, [data-testid="stAppViewContainer"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        background-color: #F8FAFC !important;
        color: #0F172A !important;
    }
    
    /* Force Light Theme Colors on widget labels to avoid invisible/white text on light background */
    label, [data-testid="stWidgetLabel"] p, .stMarkdown p, span {
        color: #0F172A !important;
        font-weight: 500 !important;
    }
    
    /* Force inputs, dropdowns to be highly visible with white background and dark grey border */
    input, select, div[role="combobox"], div[data-baseweb="select"], div[data-baseweb="input"], .stNumberInput input {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
        border: 1px solid #CBD5E1 !important;
    }
    
    /* Specific select option values */
    div[role="listbox"] div {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
    }
    
    /* Sidebar specific input labels */
    section[data-testid="stSidebar"] label {
        color: #0F172A !important;
        font-weight: 600 !important;
    }
    
    /* Header Style */
    .header-container {
        background: linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%);
        border: 1px solid #BFDBFE;
        border-radius: 16px;
        padding: 30px;
        margin-bottom: 30px;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .header-title {
        color: #1E3A8A !important;
        font-weight: 800;
        font-size: 2.25rem;
        margin: 0 0 10px 0;
        letter-spacing: -0.025em;
    }
    .header-subtitle {
        color: #2563EB !important;
        font-weight: 500;
        font-size: 1.1rem;
        margin: 0;
    }
    
    /* Sidebar CSS customization */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E2E8F0 !important;
    }
    section[data-testid="stSidebar"] [data-testid="stForm"] {
        background-color: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
        box-shadow: none !important;
        border-radius: 12px;
        padding: 20px;
    }
    
    /* Card Styles for Metrics */
    div[data-testid="stMetric"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 16px !important;
        padding: 20px 24px !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.03), 0 2px 4px -1px rgba(0, 0, 0, 0.02) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-4px) !important;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.03) !important;
        border-color: #BFDBFE !important;
    }
    
    /* Metric Text colors */
    div[data-testid="stMetricLabel"] {
        color: #64748B !important;
    }
    div[data-testid="stMetricValue"] {
        color: #0F172A !important;
    }
    
    /* Dataframe wrapper styling */
    .stDataFrame {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 10px;
        box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.02);
    }
    
    /* Custom divider */
    hr {
        margin: 2rem 0 !important;
        border: 0 !important;
        border-top: 1px solid #E2E8F0 !important;
    }
    
    /* Form Submit Button Gradient */
    .stButton>button {
        background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%) !important;
        color: white !important;
        border: none !important;
        padding: 10px 24px !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 6px -1px rgba(37, 99, 235, 0.2) !important;
        transition: all 0.2s ease !important;
        width: 100% !important;
    }
    .stButton>button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 8px -1px rgba(37, 99, 235, 0.3) !important;
        background: linear-gradient(135deg, #2563EB 0%, #1D4ED8 100%) !important;
    }
    .stButton>button:active {
        transform: translateY(1px) !important;
    }
    
    /* Warning alerts styling */
    div[data-testid="stAlert"] {
        border-radius: 12px !important;
        border: 1px solid #FEF3C7 !important;
    }
    
    /* Keep terminal fonts from getting overridden by general label text colors */
    .terminal-box p, .terminal-box span, .terminal-box strong {
        color: inherit !important;
        font-weight: normal !important;
    }
    </style>
""", unsafe_allow_html=True)

# Render Title Container
st.markdown("""
    <div class="header-container">
        <h1 class="header-title">REALTIME PROFIT INTELLIGENCE PLATFORM</h1>
        <p class="header-subtitle">Real-time Profit Prediction Pipeline using Hadoop HDFS, Spark Structured Streaming, Kafka, and Random Forest Model</p>
    </div>
""", unsafe_allow_html=True)

# Kiem tra va hien thi trang thai suc khoe cua Cum (Cluster Node Health Status)
if MODE == "cluster":
    import socket
    node_health = {}
    for name, ip in [("master", "26.97.56.101"), ("worker1 (you)", "26.105.196.249"), ("worker2", "26.155.115.30")]:
        try:
            port = 9000 if name == "master" else 9866
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((ip, port))
            node_health[name] = "ONLINE"
            s.close()
        except Exception:
            node_health[name] = "OFFLINE"
    
    status_items = []
    for name, state in node_health.items():
        color = "#10B981" if state == "ONLINE" else "#EF4444"
        status_items.append(f"<span style='font-weight:bold;'>{name.upper()}:</span> <span style='color: {color}; font-weight:bold;'>[{state}]</span>")
    
    status_bar = " | ".join(status_items)
    st.markdown(f"<div style='background-color: #FFFFFF; border: 1px solid #E2E8F0; padding: 12px; border-radius: 12px; text-align: center; font-size: 0.85rem; margin-bottom: 20px; color: #0F172A; box-shadow: 0 1px 2px 0 rgba(0,0,0,0.05);'>Cluster Health Status: {status_bar}</div>", unsafe_allow_html=True)
else:
    st.markdown("<div style='background-color: #FFFFFF; border: 1px solid #E2E8F0; padding: 12px; border-radius: 12px; text-align: center; font-size: 0.85rem; margin-bottom: 20px; color: #0F172A; box-shadow: 0 1px 2px 0 rgba(0,0,0,0.05);'>Cluster Health Status: <span style='font-weight:bold;'>LOCAL STANDALONE:</span> <span style='color: #10B981; font-weight:bold;'>[ONLINE]</span></div>", unsafe_allow_html=True)

st.write("---")

# Chia Layout: Sidebar làm Web Form nhập đơn hàng, Main Area hiển thị Dashboard
st.sidebar.markdown("<h2 style='color: #1E3A8A; font-weight:700;'>Order Entry</h2>", unsafe_allow_html=True)

# Hiển thị chế độ vận hành (Local / Cluster)
if MODE == "cluster":
    st.sidebar.markdown(
        "<div style='padding: 8px 12px; background-color: #DBEAFE; color: #1E40AF; border-radius: 8px; font-weight: 600; margin-bottom: 20px; font-size: 0.9rem; text-align: center; border: 1px solid #BFDBFE;'>"
        "MODE: CLUSTER (Network)"
        "</div>",
        unsafe_allow_html=True
    )
else:
    st.sidebar.markdown(
        "<div style='padding: 8px 12px; background-color: #D1FAE5; color: #065F46; border-radius: 8px; font-weight: 600; margin-bottom: 20px; font-size: 0.9rem; text-align: center; border: 1px solid #A7F3D0;'>"
        "MODE: LOCAL (Standalone)"
        "</div>",
        unsafe_allow_html=True
    )

with st.sidebar.form(key="order_form", clear_on_submit=False):
    # Tự động tạo Order ID ngẫu nhiên làm gợi ý
    suggested_order_id = f"ORD-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    order_id = st.text_input("Order ID", value=suggested_order_id)
    
    # Dropdown Customer ID
    customer_id = st.selectbox("Customer ID", [
        "LS-172304", "MV-174854", "CS-121304", "AP-109154", "JO-152804", "BD-113204"
    ])
    
    # Phân cấp chọn Category -> Sub_Category
    category = st.selectbox("Category", list(CATEGORY_SUBCAT_MAP.keys()))
    sub_category = st.selectbox("Sub-Category", CATEGORY_SUBCAT_MAP[category])
    
    # Các thông số tài chính đơn hàng
    sales = st.number_input("Sales (USD)", min_value=1.0, value=150.0, step=10.0)
    discount = st.slider("Discount", min_value=0.0, max_value=0.85, value=0.0, step=0.05)
    quantity = st.number_input("Quantity", min_value=1, max_value=20, value=1)
    shipping_cost = st.number_input("Shipping Cost (USD)", min_value=0.0, value=15.0, step=5.0)
    
    # Thị trường khu vực
    market = st.selectbox("Market", ["US", "EU", "APAC", "LATAM", "Africa", "Canada"])
    
    submit_button = st.form_submit_button(label="Publish Order")

if submit_button:
    # Xây dựng payload gửi lên Kafka
    order_payload = {
        "Order_ID": order_id,
        "Customer_ID": customer_id,
        "Order_Date": datetime.now().strftime("%Y-%m-%d"),
        "Category": category,
        "Sub_Category": sub_category,
        "Sales": float(sales),
        "Discount": float(discount),
        "Quantity": int(quantity),
        "Shipping_Cost": float(shipping_cost),
        "Market": market,
        "Publish_Time": float(time.time())
    }
    
    # Gửi qua Producer
    try:
        producer = OrderProducer()
        success, msg = producer.send_order(order_payload)
        producer.close()
        
        if success:
            st.sidebar.success(f"Order {order_id} published to Kafka successfully!")
            # Lưu log gửi dữ liệu để hiển thị lên Terminal log ở Main Area
            if "producer_logs" not in st.session_state:
                st.session_state["producer_logs"] = []
            log_entry = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "order_id": order_id,
                "sales": float(sales),
                "discount": float(discount),
                "payload": order_payload
            }
            st.session_state["producer_logs"].append(log_entry)
            if len(st.session_state["producer_logs"]) > 5:
                st.session_state["producer_logs"].pop(0)
        else:
            st.sidebar.error(f"Error publishing order: {msg}")
    except Exception as e:
        st.sidebar.error(f"Broker connection error: {e}")

# 2. Main Area - Dashboard Realtime

# Định nghĩa fragment tự động làm mới phần Dashboard mỗi 3 giây một cách bất đồng bộ (non-blocking)
@st.fragment(run_every=3)
def render_realtime_dashboard():
    # 1. Khởi tạo/Đọc dữ liệu logs gửi từ Session State
    if "producer_logs" not in st.session_state:
        st.session_state["producer_logs"] = []

    # Đọc bản sao dữ liệu nhận được từ hai Consumer thread dưới cơ chế khóa an toàn
    with data_lock:
        df_data = list(predictions_list)
        df_reqs = list(requests_list)

    # 2. Vẽ BA Terminal giám sát ở phía trên cùng side-by-side (Live Pipeline Inspector)
    st.markdown("<h3 style='color: #1E3A8A; font-size:1.4rem; font-weight:700; margin-bottom:15px;'>Live Pipeline Latency Inspector</h3>", unsafe_allow_html=True)
    
    col_term1, col_term2, col_term3 = st.columns(3)
    
    # TERMINAL 1: Client Publish Output
    with col_term1:
        st.markdown("<h4 style='color: #2563EB; font-size:1.05rem; font-weight:600; margin-bottom:8px;'>Kafka Producer Log</h4>", unsafe_allow_html=True)
        term1_html = "<div class='terminal-box' style='background-color: #0F172A; color: #38BDF8; font-family: monospace; padding: 15px; border-radius: 12px; height: 180px; overflow-y: auto; font-size: 0.8rem; border: 1px solid #334155;'>"
        
        prod_logs = st.session_state["producer_logs"]
        if len(prod_logs) == 0:
            term1_html += "<p style='color: #64748B; margin: 0;'>[System] Waiting for submissions...</p>"
        else:
            for log in reversed(prod_logs):
                term1_html += f"<p style='margin: 4px 0;'><span style='color: #60A5FA;'>[PUBLISH]</span> ({log['time']}) Published order <strong style='color: #FFFFFF;'>{log['order_id']}</strong>. Sales: <strong style='color: #34D399;'>${log['sales']:.2f}</strong>, Discount: {log['discount']:.0%}</p>"
                
        term1_html += "</div>"
        st.markdown(term1_html, unsafe_allow_html=True)
        
    # TERMINAL 2: Kafka Queue Broker Ingestion
    with col_term2:
        st.markdown("<h4 style='color: #D97706; font-size:1.05rem; font-weight:600; margin-bottom:8px;'>Kafka Broker Log</h4>", unsafe_allow_html=True)
        term2_html = "<div class='terminal-box' style='background-color: #0F172A; color: #FBBF24; font-family: monospace; padding: 15px; border-radius: 12px; height: 180px; overflow-y: auto; font-size: 0.8rem; border: 1px solid #334155;'>"
        
        if len(df_reqs) == 0:
            term2_html += "<p style='color: #64748B; margin: 0;'>[System] Waiting for messages in queue...</p>"
        else:
            for log in reversed(df_reqs):
                order_id_val = log.get("Order_ID", "N/A")
                pub_time_val = log.get("Publish_Time", 0.0)
                sales_val = log.get("Sales", 0.0)
                category_val = log.get("Category", "N/A")
                
                time_str = datetime.fromtimestamp(pub_time_val).strftime("%H:%M:%S") if pub_time_val else "N/A"
                term2_html += f"<p style='margin: 4px 0;'><span style='color: #FBBF24;'>[QUEUE RECEIVE]</span> ({time_str}) Ingested order <strong style='color: #FFFFFF;'>{order_id_val}</strong>. Sales: ${sales_val:.2f}, Category: {category_val}</p>"
                
        term2_html += "</div>"
        st.markdown(term2_html, unsafe_allow_html=True)
        
    # TERMINAL 3: Spark ML Output Consumer
    with col_term3:
        st.markdown("<h4 style='color: #10B981; font-size:1.05rem; font-weight:600; margin-bottom:8px;'>Spark Streaming Log</h4>", unsafe_allow_html=True)
        term3_html = "<div class='terminal-box' style='background-color: #0F172A; color: #34D399; font-family: monospace; padding: 15px; border-radius: 12px; height: 180px; overflow-y: auto; font-size: 0.8rem; border: 1px solid #334155;'>"
        
        if len(df_data) == 0:
            term3_html += "<p style='color: #64748B; margin: 0;'>[System] Waiting for Spark calculations...</p>"
        else:
            df_temp = pd.DataFrame(df_data)
            recent_predictions = df_temp.sort_values(by="prediction_time", ascending=False).head(5)
            
            for _, row in recent_predictions.iterrows():
                order_id_log = row["order_id"]
                pred_time = row["prediction_time"]
                pub_time = row.get("publish_time", None)
                
                if pub_time is not None:
                    latency = time.time() - float(pub_time)
                    if latency >= 0 and latency < 600:
                        latency_str = f" [Latency: {latency:.2f}s - Real-time]"
                    else:
                        latency_str = " [History]"
                else:
                    latency_str = " [N/A]"
                    
                term3_html += f"<p style='margin: 4px 0;'><span style='color: #10B981;'>[RESULT RECEIVE]</span> ({pred_time}) Predicted profit for <strong style='color: #FFFFFF;'>{order_id_log}</strong>: <strong style='color: #F87171;'>${row['prediction_usd']:.2f}</strong>.{latency_str}</p>"
                
        term3_html += "</div>"
        st.markdown(term3_html, unsafe_allow_html=True)
        
    st.write("---")

    # Nếu chưa có đơn hàng nào được dự báo
    if len(df_data) == 0:
        st.info("Waiting for real-time predictions from Spark Structured Streaming...")
        st.write("Please input and publish an order from the Sidebar to activate.")
        st.write("Tip: Ensure Hadoop HDFS, Spark Master, Kafka, and the Spark Streaming Job are started.")
        return
        
    # Chuyển đổi dữ liệu sang Pandas DataFrame
    df = pd.DataFrame(df_data)
    
    # Tính toán các chỉ số thống kê (KPIs)
    total_orders = len(df)
    avg_profit = df["prediction_usd"].mean()
    loss_orders = len(df[df["prediction_usd"] < 0])
    loss_percentage = (loss_orders / total_orders) * 100
    
    # Tạo các cột KPI
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Orders", f"{total_orders}", delta="Realtime")
    with col2:
        st.metric("Average Predicted Profit", f"${avg_profit:.2f}", 
                  delta="Positive" if avg_profit > 0 else "Warning")
    with col3:
        st.metric("Loss Warning Orders", f"{loss_orders}", 
                  delta=f"{loss_percentage:.1f}% of total", delta_color="inverse")
    with col4:
        high_profit_count = len(df[df["risk_level"] == "HIGH PROFIT OPPORTUNITY"])
        st.metric("High Profit Opportunities (> $500)", f"{high_profit_count}", delta="Excellent")
        
    st.write("---")
    
    # Phần biểu đồ phân tích Realtime
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown("<h3 style='color: #1E3A8A; font-size:1.25rem; font-weight:600; margin-bottom:15px;'>Real-time Profit Prediction History</h3>", unsafe_allow_html=True)
        df["prediction_time_dt"] = pd.to_datetime(df["prediction_time"])
        chart_data = df.sort_values(by="prediction_time_dt")[["prediction_time", "prediction_usd"]]
        chart_data.columns = ["Prediction Time", "Predicted Profit ($)"]
        st.line_chart(chart_data.set_index("Prediction Time"), color="#2563EB")
        
    with col_chart2:
        st.markdown("<h3 style='color: #1E3A8A; font-size:1.25rem; font-weight:600; margin-bottom:15px;'>Profit Risk Classification (Rule Engine)</h3>", unsafe_allow_html=True)
        risk_counts = df["risk_level"].value_counts().reset_index()
        risk_counts.columns = ["Risk Level", "Order Count"]
        st.bar_chart(risk_counts.set_index("Risk Level"), color="#10B981")
        
    st.write("---")
    
    # Hàng 3: Thống kê chi tiết & Bảng dữ liệu thô
    st.markdown("<h3 style='color: #1E3A8A; font-size:1.4rem; font-weight:700; margin-bottom:20px;'>Recent Predictions Detail</h3>", unsafe_allow_html=True)
    
    # Format hiển thị bảng đẹp mắt
    display_df = df.copy()
    display_df = display_df[[
        "prediction_time", "order_id", "customer_id", "category", "sub_category", 
        "sales", "discount", "prediction_usd", "risk_level"
    ]]
    display_df.columns = [
        "Prediction Time", "Order ID", "Customer ID", 
        "Category", "Sub-Category", "Sales ($)", "Discount (%)", "Predicted Profit ($)", "Risk Level"
    ]
    
    # Định dạng tiền tệ và phần trăm
    display_df["Sales ($)"] = display_df["Sales ($)"].map("${:,.2f}".format)
    display_df["Predicted Profit ($)"] = display_df["Predicted Profit ($)"].map("${:,.2f}".format)
    display_df["Discount (%)"] = display_df["Discount (%)"].map("{:.1%}".format)
    
    # Show dataframe
    st.dataframe(display_df.sort_values(by="Prediction Time", ascending=False), use_container_width=True)

# Gọi fragment để vẽ giao diện realtime
render_realtime_dashboard()

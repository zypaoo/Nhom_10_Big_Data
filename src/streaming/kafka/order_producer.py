import json
import sys
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.config.cluster_config import KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC

def init_topics():
    """Tự động kiểm tra và khởi tạo các topic cần thiết trên Kafka Broker"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, request_timeout_ms=5000)
        existing_topics = admin_client.list_topics()
        topics_to_create = []
        for topic in [INPUT_TOPIC, "profit_prediction_results"]:
            if topic not in existing_topics:
                topics_to_create.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Created Kafka topics: {[t.name for t in topics_to_create]}")
        admin_client.close()
    except Exception as e:
        print(f"Warning: Could not initialize topics: {e}")

class OrderProducer:
    """Producer để đẩy các đơn hàng mới vào hàng đợi Kafka"""
    def __init__(self):
        init_topics()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='all',
            retries=3
        )

    def send_order(self, order_data):
        """Đẩy dữ liệu đơn hàng dạng dict lên Kafka"""
        try:
            future = self.producer.send(INPUT_TOPIC, value=order_data)
            result = future.get(timeout=10)
            print(f"Successfully sent order {order_data.get('Order_ID')} to topic {INPUT_TOPIC} (partition {result.partition}, offset {result.offset})")
            return True, f"Sent successfully (offset {result.offset})"
        except Exception as e:
            print(f"Error sending order to Kafka: {e}")
            return False, str(e)

    def close(self):
        self.producer.close()

if __name__ == "__main__":
    # Gửi thử nghiệm một đơn hàng mẫu
    print("=== KAFKA ORDER PRODUCER TEST ===")
    producer = OrderProducer()
    mock_order = {
        "Order_ID": "TEST-2026-9999",
        "Customer_ID": "LS-172304",  # Lycoris Saunders from dataset
        "Order_Date": "2026-06-13",
        "Category": "Office Supplies",
        "Sub_Category": "Paper",
        "Sales": 150.0,
        "Discount": 0.0,
        "Quantity": 5,
        "Shipping_Cost": 12.5,
        "Market": "US"
    }
    print("Sending mock order to Kafka...")
    success, msg = producer.send_order(mock_order)
    print(f"Result: {success} - {msg}")
    producer.close()

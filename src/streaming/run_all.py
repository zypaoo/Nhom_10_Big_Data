import os
os.environ["HADOOP_USER_NAME"] = "vudua"
import sys
import platform
import subprocess
import time
import shutil
import socket

# Thiết lập encoding UTF-8 cho console tránh lỗi ký tự tiếng Việt hoặc Emoji trên Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

def get_local_ip():
    """Tự động phát hiện IP nội bộ của máy này kết nối với cụm mạng Radmin VPN"""
    try:
        # Thử kết nối nhanh tới IP của master để xác định interface mạng đang dùng (VPN)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("26.97.56.101", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            # Thử phân giải IP của hostname 'master' trước
            master_ip = socket.gethostbyname('master')
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((master_ip, 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            # Lấy IP cục bộ tương ứng với hostname của máy
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return "127.0.0.1"

def select_run_mode():
    """Hỏi ý kiến người dùng để chọn chế độ chạy (Local hoặc Cluster)"""
    print("======================================================================")
    print("HỆ THỐNG REALTIME PROFIT INTEL PLATFORM")
    print("======================================================================")
    print("Vui lòng chọn chế độ vận hành hệ thống:")
    print("1. Chạy Local (Mặc định - Cục bộ hoàn toàn trên máy tính này)")
    print("2. Chạy Cluster (Chạy trên cụm máy kết nối Radmin VPN và HDFS)")
    print("Nhấn Enter để chọn mặc định [Local], hoặc nhập '2' để chọn [Cluster]")
    try:
        choice = input("Lựa chọn của bạn [1/2] (Mặc định 1): ").strip()
        if choice == "2":
            return "cluster"
    except Exception:
        pass
    return "local"

# Chọn chế độ trước khi load bất kỳ config nào
RUN_MODE = select_run_mode()
os.environ["RUN_MODE"] = RUN_MODE

# Tự động nhận diện IP nội bộ để chạy cluster
LOCAL_IP = get_local_ip()

# Thiết lập mặc định cho các biến môi trường dựa trên chế độ chọn
if RUN_MODE == "cluster":
    if "KAFKA_BOOTSTRAP_SERVER" not in os.environ:
        os.environ["KAFKA_BOOTSTRAP_SERVER"] = f"{LOCAL_IP}:9092"
    if "SPARK_MASTER" not in os.environ:
        os.environ["SPARK_MASTER"] = "spark://master:7077"
else:
    if "KAFKA_BOOTSTRAP_SERVER" not in os.environ:
        os.environ["KAFKA_BOOTSTRAP_SERVER"] = "localhost:9092"
    if "SPARK_MASTER" not in os.environ:
        os.environ["SPARK_MASTER"] = "local[*]"

# Thêm project root vào sys.path để đọc cấu hình (di chuyển vào src/streaming)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
try:
    from src.config.cluster_config import (
        KAFKA_HOME,
        KAFKA_BOOTSTRAP_SERVER,
        SPARK_MASTER,
        DEFAULT_SHUFFLE_PARTITIONS,
        CHECKPOINT_PATH,
        PREDICTIONS_OUTPUT_PATH
    )
except ImportError as e:
    import traceback
    print(f"Error: Không thể import cấu hình. Chi tiết lỗi: {e}")
    traceback.print_exc()
    sys.exit(1)

def get_kafka_home():
    # 1. Thử đọc từ biến môi trường hệ thống hoặc config
    if KAFKA_HOME and os.path.exists(KAFKA_HOME):
        return os.path.abspath(KAFKA_HOME)

    # 2. Thử tự động phát hiện qua biến môi trường PATH
    script_name = "kafka-server-start.bat" if platform.system() == "Windows" else "kafka-server-start.sh"
    script_path = shutil.which(script_name)
    if script_path:
        abs_path = os.path.abspath(script_path)
        parts = abs_path.split(os.sep)
        if "bin" in parts:
            bin_idx = parts.index("bin")
            return os.sep.join(parts[:bin_idx])

    # 3. Các đường dẫn mặc định làm dự phòng
    fallback_paths = [
        r"D:\java\kafka_2.13-4.1.2",
        r"C:\kafka",
        r"/opt/kafka",
        r"/usr/local/kafka"
    ]
    for path in fallback_paths:
        if os.path.exists(path):
            return os.path.abspath(path)
            
    return None

def run_command_in_new_window(cmd_list, title):
    """Mở một cửa sổ Terminal/CMD mới để chạy lệnh chạy ngầm"""
    current_os = platform.system()
    if current_os == "Windows":
        cmd_str = " ".join(cmd_list)
        subprocess.Popen(f'start "{title}" cmd /k "{cmd_str}"', shell=True)
    else:
        cmd_str = " ".join(cmd_list)
        subprocess.Popen(["gnome-terminal", "--title", title, "--", "bash", "-c", f"{cmd_str}; exec bash"])

def configure_kafka_properties(kafka_home, kafka_host):
    """Tự động cập nhật file server.properties để thiết lập log.dirs và advertised.listeners tuyệt đối"""
    properties_file = os.path.join(kafka_home, "config", "server.properties")
    if not os.path.exists(properties_file):
        print(f"Warning: Không tìm thấy file cấu hình {properties_file}")
        return None
        
    with open(properties_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    # Tạo đường dẫn absolute cho log.dirs nằm bên trong KAFKA_HOME/tmp để tránh lỗi quyền ghi đĩa
    abs_log_dir = os.path.abspath(os.path.join(kafka_home, "tmp", "kraft-combined-logs"))
    abs_log_dir_java = abs_log_dir.replace("\\", "/") # Dùng forward-slash cho Java properties tương thích Windows
    
    log_dirs_modified = False
    adv_listeners_modified = False
    new_lines = []
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("log.dirs="):
            new_lines.append(f"log.dirs={abs_log_dir_java}\n")
            log_dirs_modified = True
        elif stripped.startswith("advertised.listeners="):
            # Thay thế host
            new_lines.append(f"advertised.listeners=PLAINTEXT://{kafka_host}:9092,CONTROLLER://localhost:9093\n")
            adv_listeners_modified = True
        else:
            new_lines.append(line)
            
    if not log_dirs_modified:
        new_lines.append(f"\nlog.dirs={abs_log_dir_java}\n")
    if not adv_listeners_modified:
        new_lines.append(f"\nadvertised.listeners=PLAINTEXT://{kafka_host}:9092,CONTROLLER://localhost:9093\n")
        
    with open(properties_file, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    print(f"✅ Đã tự động cấu hình log.dirs trong server.properties thành: {abs_log_dir_java}")
    print(f"✅ Đã tự động cấu hình advertised.listeners thành: PLAINTEXT://{kafka_host}:9092")
    return abs_log_dir

def format_kraft_storage_if_needed(kafka_home, kafka_host):
    """Tự động kiểm tra và định dạng thư mục KRaft log nếu chưa có meta.properties"""
    properties_file = os.path.join(kafka_home, "config", "server.properties")
    if not os.path.exists(properties_file):
        print(f"Warning: Không tìm thấy file cấu hình {properties_file}")
        return

    # Đồng bộ hóa cấu hình log.dirs và advertised.listeners tuyệt đối
    log_dirs = configure_kafka_properties(kafka_home, kafka_host)
    if not log_dirs:
        return

    meta_file = os.path.join(log_dirs, "meta.properties")
    if not os.path.exists(meta_file):
        print(f"\n📂 Tự động định dạng thư mục KRaft log tại: {log_dirs}...")
        import uuid
        import base64
        raw_uuid = uuid.uuid4().bytes
        cluster_id = base64.urlsafe_b64encode(raw_uuid).decode("utf-8").rstrip("=")
        
        if platform.system() == "Windows":
            storage_tool = os.path.join(kafka_home, "bin", "windows", "kafka-storage.bat")
        else:
            storage_tool = os.path.join(kafka_home, "bin", "kafka-storage.sh")
            
        # Thử định dạng với --standalone trước (dành cho Kafka 4.x/3.x không dùng ZooKeeper và quorum bootstrap)
        format_cmd = [storage_tool, "format", "-t", cluster_id, "-c", properties_file, "--standalone"]
        print(f"Đang chạy lệnh định dạng KRaft: {' '.join(format_cmd)}")
        res = subprocess.run(format_cmd, capture_output=True, text=True, encoding="utf-8")
        
        if res.returncode != 0 and "unrecognized option" in res.stderr.lower():
            # Fallback nếu không hỗ trợ --standalone (ở các phiên bản cũ)
            format_cmd = [storage_tool, "format", "-t", cluster_id, "-c", properties_file]
            print(f"Không hỗ trợ --standalone, thử lại lệnh gốc: {' '.join(format_cmd)}")
            res = subprocess.run(format_cmd, capture_output=True, text=True, encoding="utf-8")
            
        if res.returncode == 0:
            print(f"✅ Định dạng KRaft thành công! Cluster ID: {cluster_id}")
        else:
            print(f"❌ Định dạng KRaft thất bại.")
            print(f"Chi tiết stdout: {res.stdout}")
            print(f"Chi tiết stderr: {res.stderr}")

def main():
    print(f"Hệ điều hành hiện tại: {platform.system()}")
    print(f"🔔 Chế độ vận hành đã chọn: {RUN_MODE.upper()}")
    if RUN_MODE == "cluster":
        print(f"🔍 Đã xác định IP nội bộ Radmin VPN của máy này là: {LOCAL_IP}")
    print(f"⚙️ Thiết lập Kafka Bootstrap Server: {KAFKA_BOOTSTRAP_SERVER}")
    print(f"⚙️ Thiết lập Spark Master: {SPARK_MASTER}")
    print(f"⚙️ Đường dẫn Spark Checkpoint: {CHECKPOINT_PATH}")
    print(f"⚙️ Đường dẫn Spark Parquet Output: {PREDICTIONS_OUTPUT_PATH}")
    
    KAFKA_HOME_DIR = get_kafka_home()
    print(f"Đường dẫn Kafka tự động phát hiện: {KAFKA_HOME_DIR}")

    if not KAFKA_HOME_DIR or not os.path.exists(KAFKA_HOME_DIR):
        print(f"❌ Lỗi: Không tìm thấy thư mục cài đặt Kafka.")
        return

    # Xác định kafka host để bind/advertise
    kafka_host = "localhost" if RUN_MODE == "local" else LOCAL_IP

    # Tự động định dạng KRaft trước khi chạy (dành cho Kafka 4.x/3.x không dùng ZooKeeper)
    format_kraft_storage_if_needed(KAFKA_HOME_DIR, kafka_host)

    # 1. Khởi động Kafka Broker (Không cần bật ZooKeeper vì bản 4.1.2 chạy KRaft)
    print("\n[1/3] Đang khởi động Kafka Broker (chế độ KRaft) trong cửa sổ mới...")
    if platform.system() == "Windows":
        kafka_cmd = [f"cd /d {KAFKA_HOME_DIR}", "&&", r"bin\windows\kafka-server-start.bat", r"config\server.properties"]
    else:
        kafka_cmd = [f"cd {KAFKA_HOME_DIR}", "&&", "./bin/kafka-server-start.sh", "./config/server.properties"]
    run_command_in_new_window(kafka_cmd, "Kafka-Broker-KRaft")

    # Đợi Kafka Broker lên hình
    print("⏳ Chờ Kafka Broker khởi động (5 giây)...")
    time.sleep(5)

    # 2. Khởi động Spark Structured Streaming Job
    print("\n[2/3] Đang khởi động Spark Structured Streaming Job...")
    
    # Thiết lập biến môi trường chạy Python cho Spark để kế thừa tự động
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if "local" in SPARK_MASTER.lower():
        os.environ["PYSPARK_PYTHON"] = sys.executable
        
    script_dir = os.path.dirname(os.path.abspath(__file__))
    profit_stream_path = os.path.join(script_dir, "profit_stream.py")
    dashboard_app_path = os.path.join(script_dir, "dashboard", "app.py")

    spark_submit_cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
        "--conf", f"spark.sql.shuffle.partitions={DEFAULT_SHUFFLE_PARTITIONS}",
        "--conf", "spark.executorEnv.HADOOP_USER_NAME=vudua",
        profit_stream_path
    ]
    run_command_in_new_window(spark_submit_cmd, "Spark-Structured-Streaming")
    
    # Đợi Spark kết nối
    print("⏳ Chờ Spark Streaming kết nối Kafka và tải mô hình (10 giây)...")
    time.sleep(10)

    # 3. Khởi động Streamlit Dashboard
    print("\n[3/3] Đang khởi động Streamlit Realtime Dashboard...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", dashboard_app_path])

if __name__ == "__main__":
    main()



#!/bin/bash
# ==============================================================================
# Script khởi chạy Streamlit Dashboard
# ==============================================================================

# Xác định đường dẫn thư mục dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "------------------------------------------------------------"
echo "Đang khởi chạy Dashboard..."
echo "------------------------------------------------------------"

# Khởi chạy streamlit trỏ trực tiếp đến app.py
streamlit run "$PROJECT_DIR/src/dashboard/app.py"

#!/bin/bash

# Dừng script nếu có lỗi xảy ra
set -e

echo "=================================================="
echo "BẮT ĐẦU QUÁ TRÌNH NỘP FLINK JOB..."
echo "=================================================="

# Chờ 15 giây để JobManager khởi động hoàn toàn (dù docker-compose đã có depends_on)
echo "Đang chờ JobManager sẵn sàng..."
sleep 15

# Nộp job PyFlink lên cụm Flink
# -m jobmanager:8081 : Trỏ tới địa chỉ của JobManager trong mạng Docker
# -py ... : Đường dẫn tới file code Python chính
echo "Đang nộp Job: job_kafka_to_clickhouse.py..."
flink run -m jobmanager:8081 -py /opt/flink/usrlib/src/job_kafka_to_clickhouse.py

echo "=================================================="
echo "NỘP JOB THÀNH CÔNG! Vào http://localhost:8081 để xem trạng thái."
echo "=================================================="

# Giữ cho container tiếp tục chạy (không bị tắt) để bạn có thể xem log
tail -f /dev/null
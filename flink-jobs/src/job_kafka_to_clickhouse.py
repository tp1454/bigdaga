import logging
import clickhouse_connect
from functools import partial
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction

import config
import transforms

log = logging.getLogger(__name__)

# ==============================================================================
# 1. CUSTOM SINK (Sử dụng MapFunction)
# ==============================================================================
class ClickHouseSink(MapFunction):
    def __init__(self, batch_size=1000):
        self.client = None
        self.batch_size = batch_size
        self.columns_map = {table: cols for table, cols, _ in transforms.TOPICS.values()}
        self.buffers = {table: [] for table in self.columns_map.keys()}

    def open(self, runtime_context):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB
        )
        log.info("Đã kết nối thành công tới ClickHouse!")

    def map(self, value):
        if value is None:
            return True
            
        table_name, row_data = value
        if table_name not in self.buffers:
            return True

        self.buffers[table_name].append(row_data)
        
        if len(self.buffers[table_name]) >= self.batch_size:
            self.flush_table(table_name)
            
        return True  # Trả về True để thỏa mãn yêu cầu kiểu dữ liệu

    def flush_table(self, table_name):
        rows = self.buffers[table_name]
        if not rows:
            return
        try:
            self.client.insert(
                table=table_name,
                data=rows,
                column_names=self.columns_map[table_name]
            )
            log.info(f"Đã ghi {len(rows)} dòng vào bảng {table_name}")
            self.buffers[table_name] = []
        except Exception as e:
            log.error(f"Lỗi ghi bảng {table_name}: {e}")

    def close(self):
        if self.client:
            log.info("Đang xả nốt dữ liệu còn lại trong bộ đệm...")
            for table_name in self.buffers.keys():
                self.flush_table(table_name)
            self.client.close()
            log.info("Đã đóng kết nối ClickHouse.")


# ==============================================================================
# 2. MAIN
# ==============================================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 

    all_streams = []
    
    for topic in config.KAFKA_TOPICS:
        log.info(f"Đang cấu hình Source cho topic: {topic}")
        
        source = KafkaSource.builder() \
            .set_bootstrap_servers(config.KAFKA_BROKER) \
            .set_topics(topic) \
            .set_group_id(config.KAFKA_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        # SỬA LỖI: Dùng WatermarkStrategy.no_watermarks() thay vì None
        ds = env.from_source(
            source, 
            watermark_strategy=WatermarkStrategy.no_watermarks(), 
            source_name=f"Kafka_{topic}"
        )
        
        parsed_ds = ds.flat_map(partial(transforms.parse_kafka_message, topic))
        
        all_streams.append(parsed_ds)

    if not all_streams:
        log.error("Không có topic nào được cấu hình.")
        return
        
    merged_stream = all_streams[0]
    for stream in all_streams[1:]:
        merged_stream = merged_stream.union(stream)

    # SỬA LỖI: Khai báo output_type cho hàm map
    merged_stream.map(ClickHouseSink(batch_size=1000), output_type=Types.BOOLEAN())

    log.info("Bắt đầu chạy Job: Kafka to ClickHouse...")
    env.execute("Kafka_to_ClickHouse_Pipeline")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
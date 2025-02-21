import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # สร้าง Streaming Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    # สร้าง Table Environment
    tbl_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings
    )

    # เพิ่ม Kafka Connector JAR (ไม่จำเป็นต้องใช้ถ้ารันผ่าน Flink Cluster)
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka_2.11-1.13.0.jar')

    tbl_env.get_config() \
        .get_configuration() \
        .set_string("pipeline.jars", f"file://{kafka_jar}")

    ###############################################################
    # ✅ ใช้ SQL DDL สร้าง Kafka Source Table
    ###############################################################
    tbl_env.execute_sql("""
        CREATE TABLE productsales_source (
            data1 INT,
            data2 INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'k_datamicdemo1',
            'properties.bootstrap.servers' = 'localhost:19092',
            'properties.group.id' = 'source-demo',
            'format' = 'json'
        )
    """)

    ###############################################################
    # ✅ สร้าง Blackhole Sink Table
    ###############################################################
    tbl_env.execute_sql("""
        CREATE TABLE blackhole (
            data1 INT,
            data2 INT
        ) WITH (
            'connector' = 'blackhole'
        )
    """)

    # แสดง Schema
    print("\nProduct Sales Kafka Source Schema")
    tbl_env.from_path('productsales_source').print_schema()

    # ✅ ใช้ `execute_sql` แทน `insert_into`
    tbl_env.execute_sql("INSERT INTO blackhole SELECT * FROM productsales_source")

    # ✅ รัน Job
    tbl_env.execute('kafka-source-demo')

if __name__ == '__main__':
    main()

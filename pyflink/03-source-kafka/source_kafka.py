from pyflink.table import TableEnvironment, EnvironmentSettings

def streaming():
    streaming_setting = EnvironmentSettings.in_streaming_mode()
    streaming_tb_env = TableEnvironment.create(streaming_setting)

    jar_path = "E:\\iiot-kafka\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"

    streaming_tb_env.get_config().get_configuration().set_string("pipeline.jars", "file:///" + jar_path)

    source_kafka1= """
    CREATE TABLE source_table_1 (
        topic VARCHAR,
        data1 INT,
        data2 INT,
        data3 INT,
        data4 INT,
        data5 INT,
        data6 INT,
        data7 INT,
        data8 INT,
        data9 INT,
        data10 INT,
        ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'k_datamicdemo1',
        'properties.bootstrap.servers' = '192.168.1.30:29092,192.168.1.30:39092,192.168.1.30:49092',
        'properties.group.id' = 'test_1',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    streaming_tb_env.execute_sql(source_kafka1)
    source_kafka1 = streaming_tb_env.from_path('source_table_1')

    # print schema
    print("schema")
    source_kafka1.print_schema()

    # query data
    sql_query = """
    SELECT * FROM source_table_1
    """
    # excute query
    result_table = streaming_tb_env.sql_query(sql_query)

    # print result
    result_table.execute().print()

def main():
    streaming()

if __name__ == '__main__':
    main()
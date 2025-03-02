from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import lit,col
from pyflink.table.window import Tumble

def streaming():
    # create streaming envionment
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table envionment
    tb_env =StreamTableEnvironment.create(stream_execution_environment=stream_env,
                                          environment_settings=stream_settings)

    jar_path = "E:\\iiot-kafka\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"

    tb_env.get_config().get_configuration().set_string("pipeline.jars", "file:///" + jar_path)

    # create kafka source table
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
        ts AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'k_datamicdemo1',
        'properties.bootstrap.servers' = '192.168.1.30:29092,192.168.1.30:39092,192.168.1.30:49092',
        'properties.group.id' = 'test_2',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """
    tb_env.execute_sql(source_kafka1)

    # create and initiate loading of source Table
    tb_kafka_stream = tb_env.from_path('source_table_1')

    # print schema
    print("schema")
    tb_kafka_stream.print_schema()

    # Define Tumbling Window Aggregate for every 30 second
    tumbling_window = tb_kafka_stream.window(Tumble.over(lit(10).seconds)
                                                 .on(tb_kafka_stream.ts)
                                                 .alias('w'))\
                                            .group_by(col('w'),tb_kafka_stream.topic)\
                                            .select(tb_kafka_stream.topic,
                                                    col('w').start.alias('window_start'),
                                                    col('w').end.alias('window_end'),
                                                    (tb_kafka_stream.data1).max.alias('window_sales'))

    # sink print out
    tumbling_window.execute().print()
    tb_env.execute('tb-api-tumbling-windows')

    # sink kafka
    # sink_table = """
    #     CREATE TABLE print_sink (
    #         topic VARCHAR, 
    #         window_start TIMESTAMP_LTZ(3),
    #         window_end TIMESTAMP_LTZ(3),
    #         window_sales BIGINT
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = 'test111',
    #         'properties.bootstrap.servers' = '192.168.1.30:29092,192.168.1.30:39092,192.168.1.30:49092',
    #         'format' = 'json'
    #     )
    #     """
    
    # tb_env.execute_sql(sink_table)
    # tumbling_window.execute_insert("print_sink").wait()
    

def main():
    streaming()

if __name__ == '__main__':
    main()
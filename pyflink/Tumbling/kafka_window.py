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

    jar_path = "D:\\iiot-kafka\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"
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
        ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'k_datamicdemo1',
        'properties.bootstrap.servers' = '192.168.0.179:29092,192.168.0.179:39092,192.168.0.179:49092',
        'properties.group.id' = 'test_1',
        'scan.startup.mode' = 'latest-offset',
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
    tunbling_window_sql = tb_kafka_stream.window(Tumble.over(lit(30).seconds)
                                                 .on(tb_kafka_stream.ts)
                                                 .alias('w'))\
                                            .group_by(col('w'),tb_kafka_stream.topic)\
                                            .select(tb_kafka_stream.topic,
                                                    col('w').start.alias('window_start'),
                                                    col('w').end.alias('window_end'),
                                                    (tb_kafka_stream.data1).sum.alias('window_sales'))
    tunbling_window_sql.execute_sql()                                                 
    # excute query
    result_table = streaming_tb_env.sql_query(tunbling_window_sql)
    # print result
    result_table.execute().print()
    
def main():
    streaming()

if __name__ == '__main__':
    main()
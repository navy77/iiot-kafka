from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Create the StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# Create the TableEnvironment with streaming mode
settings = EnvironmentSettings.in_streaming_mode()
table_env = StreamTableEnvironment.create(environment_settings=settings)

# Add the necessary JAR file to the table environment
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///D:/iiot-kafka/pyflink/flink-sql-connector-kafka-3.4.0-1.20.jar")

# Define source table DDL for Kafka
source_ddl_1 = """
    CREATE TABLE source_table_1 (
        topic VARCHAR,
        data1 INT,
        data2 INT,
        data3 INT,
        time_stamp BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'k_datamicdem01',
        'properties.bootstrap.servers' = '192.168.0.179:29092,192.168.0.179:39092,192.168.0.179:49092',
        'properties.group.id' = 'test_1',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

# Execute the DDL statement to create the source table
table_env.execute_sql(source_ddl_1)

# Retrieve the source table
source_table_1 = table_env.from_path('source_table_1')

# Print the schema of the source table
print("Source Table 1 Schema:")
source_table_1.print_schema()

# Define the SQL query for tumbling window aggregation
tumbling_w_sql = """
SELECT 
    SUM(data1) AS sum_data1,
    SUM(data2) AS sum_data2,
    SUM(data3) AS sum_data3
FROM source_table_1
GROUP BY
    TUMBLE(TO_TIMESTAMP(FROM_UNIXTIME(time_stamp)), INTERVAL '30' SECOND),
    topic
"""

# Execute the query and retrieve the result table
result_table = table_env.sql_query(tumbling_w_sql)

# Print the result table to the console
result_table.execute().print()

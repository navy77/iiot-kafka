import os
from pyflink.table import TableEnvironment, EnvironmentSettings
# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

jar_path = "D:\\iiot-kafka\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///" + jar_path)

# Define source tables DDL for multiple Kafka topics
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

source_ddl_2 = """
    CREATE TABLE source_table_2 (
        topic VARCHAR,
        data1 INT,
        data2 INT,
        data3 INT,
        time_stamp BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'k_datamicdem02',
        'properties.bootstrap.servers' = '192.168.0.179:29092,192.168.0.179:39092,192.168.0.179:49092',
        'properties.group.id' = 'test_2',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

# Execute DDL statements to create the source tables
table_env.execute_sql(source_ddl_1)
table_env.execute_sql(source_ddl_2)

# Retrieve the source tables
source_table_1 = table_env.from_path('source_table_1')
source_table_2 = table_env.from_path('source_table_2')

print("Source Table 1 Schema:")
source_table_1.print_schema()

print("Source Table 2 Schema:")
source_table_2.print_schema()

# Define a SQL query to select all columns from both source tables
sql_query = """
    SELECT * FROM source_table_1
    UNION ALL
    SELECT * FROM source_table_2
"""

# Execute the query and retrieve the result table
result_table = table_env.sql_query(sql_query)

# Print the result table to the console
result_table.execute().print()

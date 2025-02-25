from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Create stream TableEnvironment from DataStream ENV
datastream_settings = StreamExecutionEnvironment.get_execution_environment()
datastream_tb_env = StreamTableEnvironment.create(datastream_settings)
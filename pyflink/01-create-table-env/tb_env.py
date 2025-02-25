from pyflink.table import TableEnvironment,EnvironmentSettings

# Create batch TableEnvironment from table ENV
batch_setting = EnvironmentSettings.in_batch_mode()
batch_tb_env = TableEnvironment.create(batch_setting) 


# Create stream TableEnvironment from table ENV
stream_setting = EnvironmentSettings.in_streaming_mode()
stream_tb_env = TableEnvironment.create(stream_setting) 

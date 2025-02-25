from pyflink.table import TableEnvironment,EnvironmentSettings

def batch():
    # create batch TableEnvironment from table ENV
    batch_setting = EnvironmentSettings.in_batch_mode()
    batch_tb_env = TableEnvironment.create(batch_setting)

    # sample data
    products = [
        ('bearing_S',1.50),
        ('bearing_M',2.50),
        ('bearing_L',3.50),
    ]

    # without column name
    tb1 = batch_tb_env.from_elements(products)
    # show schema
    print('tb1 schema')
    tb1.print_schema()
    print("###########################")
    # show data
    print('tb1 data')
    print(tb1.to_pandas())
    print("###########################")

    # with column name
    col_names = ['product', 'price'] # assign column name
    tb1_col = batch_tb_env.from_elements(products, col_names)
    print('tb1_col schema')
    tb1_col.print_schema()
    print("###########################")
    print('tb1_col data')
    print(tb1_col.to_pandas())
    print("###########################")

def stream():
    # create stream TableEnvironment from table ENV
    stream_setting = EnvironmentSettings.in_streaming_mode()
    stream_tb_env = TableEnvironment.create(stream_setting) 

    # sample data
    products = [
        ('bearing_S',1.50),
        ('bearing_M',2.50),
        ('bearing_L',3.50),
    ]

    # without column name
    tb1_stream = stream_tb_env.from_elements(products)
    # show schema
    print('tb1_stream schema')
    tb1_stream.print_schema()
    print("###########################")
    # show data
    print('tb1_stream data')
    print(tb1_stream.to_pandas())
    print("###########################")

    # with column name
    col_names = ['product', 'price'] # assign column name
    tb1_stream_col = stream_tb_env.from_elements(products, col_names)
    print('tb1_stream_col schema')
    tb1_stream_col.print_schema()
    print("###########################")
    print('tb1_stream_col data')
    print(tb1_stream_col.to_pandas())
    print("###########################")


def main():
    print("################# batch #################")
    batch()
    print("################# stream #################")
    stream()

if __name__ == '__main__':
    main()
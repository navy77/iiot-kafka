from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    # Create stream TableEnvironment from DataStream ENV
    datastream_settings = StreamExecutionEnvironment.get_execution_environment()
    datastream_tb_env = StreamTableEnvironment.create(datastream_settings)

    # sample data
    products = [
        ('bearing_S',1.50),
        ('bearing_M',2.50),
        ('bearing_L',3.50),
    ]

    # without column name
    tb2 = datastream_tb_env.from_elements(products)
    # show schema
    print('tb2 stream schema')
    tb2.print_schema()
    print("###########################")
    # show data
    print('tb2 data')
    print(tb2.to_pandas())
    print("###########################")

    # with column name
    col_names = ['product', 'price'] # assign column name
    tb2_col = datastream_tb_env.from_elements(products, col_names)
    print('tb2_col schema')
    tb2_col.print_schema()
    print("###########################")
    print('tb2_col data')
    print(tb2_col.to_pandas())
    print("###########################")
    
if __name__ == '__main__':
    main()
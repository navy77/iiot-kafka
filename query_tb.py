from pyflink.table import EnvironmentSettings, TableEnvironment


# batch mode
env_settings = EnvironmentSettings.in_batch_mode()
table_env =  TableEnvironment.create(environment_settings=env_settings)

table = table_env.from_elements(
    [('somchai', "Thailand",1000), 
     ('somsak', "Laos",1500),
     ('somchai', "Thailand",1800),
     ],
    schema=["name","country",'revenue'])

table1 = table_env.execute_sql(f"select * from {table}")
if __name__ == "__main__":
    print(table1.get_table_schema())
    table1.print()
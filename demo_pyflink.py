from pyflink.table import EnvironmentSettings, TableEnvironment

setting = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=setting)

table = table_env.from_elements(
    [(1, "Thailand"), (2, "Laos")],
    schema=["id","country"])

if __name__ == "__main__":
    print(table.get_schema())
    table.execute().print()


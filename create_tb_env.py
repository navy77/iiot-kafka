from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col,call

# batch mode
env_settings = EnvironmentSettings.in_batch_mode()
table_env =  TableEnvironment.create(environment_settings=env_settings)

table = table_env.from_elements(
    [('somchai', "Thailand",1000), 
     ('somsak', "Laos",1500),
     ('somchai', "Thailand",1800),
     ],
    schema=["name","country",'revenue'])

if __name__ == "__main__":
    table.select(col("name"),table.country,table.revenue) \
    .where(table.country == 'Thailand') \
    .group_by(table.name) \
    .select(table.name,call("sum",table.revenue).alias("revenue_total")) \
    .execute() \
    .print()
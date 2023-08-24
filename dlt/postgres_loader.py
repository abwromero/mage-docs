import dlt
from sql_database import sql_database, sql_table

pipeline = dlt.pipeline(pipeline_name="car_sales_dashboard",
                        destination="postgres",
                        dataset_name="dev")

source = sql_database(schema="raw",
                     table_names=["raw_values"])

info = pipeline.run(data=source,
                    write_disposition="merge",
                    primary_key="id",
                    table_name="raw_values")
print(info)
for i in info.load_packages:
    print(i)
print(pipeline.last_trace)
print(info.has_failed_jobs)
# print all the new tables/columns in
for package in info.load_packages:
    for table_name, table in package.schema_update.items():
        print(f"Table {table_name}: {table.get('description')}")
        for column_name, column in table["columns"].items():
            print(f"\tcolumn {column_name}: {column['data_type']}")
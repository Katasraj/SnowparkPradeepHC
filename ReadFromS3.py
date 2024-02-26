from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {
    'user': "Mossad1972",
    'password': "Katasraj2025%",
    'role':"ACCOUNTADMIN",
    'account': "jalvukt-gy82682",
    'warehouse': "COMPUTE_WH",
    'database': "CONTROL_DB",
    'schema': "EXTERNAL_STAGES"
}

session = Session.builder.configs(connection_parameters).create()
#employee_s3 = session.read.csv('@my_s3_stage/emp/')

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage')
employee_s3.show()

employee_s3_1 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage')
employee_s3_1.show()
print(type(employee_s3_1))

employee_s3_1_pandas = employee_s3_1.toPandas()
print(employee_s3_1_pandas)


# employee_s3_2 = employee_s3_1.cache_result()
# print(employee_s3_2.is_cached)

#print(employee_s3_1.queries)
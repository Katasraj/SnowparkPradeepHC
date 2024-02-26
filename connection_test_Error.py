import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.connector import connect

connection_parameters = connect(
        user="Mossad1972",
        password="Katasraj2025%",
        account="jalvukt-gy82682",
        warehouse="COMPUTE_WH",
        database="SNOWPARK",
        schema="PARK"
)

test_session = Session.builder.configs(connection_parameters).create()

print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

# connection_parameters = {"account":"jalvukt-gy82682",
# "user":"MOSSAD1972",
# "password": "Katasraj2025",
# "role":"ACCOUNTADMIN",
# "warehouse":"COMPUTE_WH",
# "database":"SNOWPARK",
# "schema":"PARK"
# }
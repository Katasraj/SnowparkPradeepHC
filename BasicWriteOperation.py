import os
import pandas as pd
from time import time
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.connector import connect
import connection_test
from connection_test import Conn
from snowflake.snowpark.functions import col

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
session = Session.builder.configs(Conn.connect_to_snf()).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()


customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")

customer = customer.filter(col("C_NATIONKEY")=='23').select("C_NAME")
#customer.show()

#customerwrt = customer.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.SNOW_CUSTOMER")

customerwrt = customer.write.mode("append").save_as_table("DEMO_DB.PUBLIC.SNOW_CUSTOMER")




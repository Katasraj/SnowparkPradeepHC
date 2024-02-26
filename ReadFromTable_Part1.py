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

test1 = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")
test1.show(20)
print(test1.count())

test2 = session.sql("select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER where C_NATIONKEY='23'")
test2.show()






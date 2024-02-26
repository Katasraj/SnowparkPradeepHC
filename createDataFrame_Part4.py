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

test = session.create_dataframe(pd.DataFrame([(1,2,3,4,5,6)],columns=["a","b","c","d","e","f"]))
test.show()
print(type(test))

# test2 = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_H6EF0HMT8C")
# test2.show()




import os
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

test = session.create_dataframe([[1,2,3,{'a':'hi'}],[1,2,3,None],[1,2,3,{'a':'hello'}],[1,2,3,{'a':'namaste'}]], schema=["a","b","c","d"])
print(type(test))

test1 = test.cache_result()
print(type(test1))
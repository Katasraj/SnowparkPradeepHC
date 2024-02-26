import os
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

# Snowflake will infer schema by itself when schema is not specified

# test = session.create_dataframe({1,2,3,4}, schema=["a"])
# test.show()

# test = session.create_dataframe([1,2,3,4], schema=["a"])
# test.show()

# test = session.create_dataframe([[1,2,3,'KBC'],[1,2,3,'ABC'],[1,2,3,'HPC'],[1,2,3,'EMD']], schema=["a","b","c","d"])
# test.show()

# test = session.create_dataframe([[1,2,3,1.2],[1,2,3,2.2],[1,2,3,3.2],[1,2,3,4.2]], schema=["a","b","c","d"])
# test.show()

test = session.create_dataframe([[1,2,3,{'a':'hi'}],[1,2,3,None],[1,2,3,{'a':'bye'}],[1,2,3,{'a':'hello'}]], schema=["a","b","c","d"])
test.show()








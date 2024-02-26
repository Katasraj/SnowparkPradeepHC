import os
from time import time
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.connector import connect
import connection_test
from connection_test import Conn
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
session = Session.builder.configs(Conn.connect_to_snf()).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()

schema = StructType([StructField("one",IntegerType()),
         StructField("two", IntegerType()),
         StructField("three", IntegerType()),
         StructField("four", DateType()),])

# test = session.create_dataframe([[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26']], schema=["a","b","c","d"])
# test.show()

test = session.create_dataframe([[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26'],[1,2,3,'2022-01-26']], schema=schema)
test.show()


import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.connector import connect
import connection_test
from connection_test import Conn
from snowflake.snowpark.functions import col,min,max,avg

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
session = Session.builder.configs(Conn.connect_to_snf()).create()

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")
supplier_min = supplier.select(min("s_acctbal"))
supplier_min.show()

acct_bal = supplier_min.collect()
print(type(acct_bal))
supplier_min = acct_bal[0]['MIN("S_ACCTBAL")']

supplier_name = supplier.select("s_name","s_acctbal").where(col("s_acctbal")==supplier_min)
print(type(supplier_name))
supplier_name.show()



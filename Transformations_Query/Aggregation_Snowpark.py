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
supplier.show()

# supplier_avg = supplier.select(min("s_acctbal"),max("s_acctbal"),avg("s_acctbal"))
# supplier_avg.show()

agg_input = [("s_acctbal","min"),("s_acctbal","max"),("s_acctbal","avg")]
supplier_avg = supplier.agg(agg_input)
supplier_avg.show()

# supplier_avg = supplier.agg([{'s_acctbal':'min'}])
# supplier_avg.show()





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

supplier_inner = supplier.select("s_suppkey","s_nationkey").filter(col("s_nationkey")==17)
#supplier_inner.show()

supplier.filter(supplier["s_suppkey"].in_(supplier_inner.select("s_suppkey"))).show()


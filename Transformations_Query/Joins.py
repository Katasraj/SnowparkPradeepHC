from snowflake.snowpark import Session
from snowflake.connector import connect
import connection_test
from connection_test import Conn
from snowflake.snowpark.functions import col,min,max,avg

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
session = Session.builder.configs(Conn.connect_to_snf()).create()


customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
orders = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
lineitem = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")

cust_orders = customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner")

# cust_orders_lineitem = cust_orders.join(lineitem,lineitem["l_orderkey"]==cust_orders["o_orderkey"],"inner")
# cust_orders_lineitem.show()

cust_orders = customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner").\
              join(lineitem,lineitem["l_orderkey"]==cust_orders["o_orderkey"],"inner")

cust_orders.show()



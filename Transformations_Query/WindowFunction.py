from snowflake.snowpark import Session,Window
from snowflake.connector import connect
import connection_test
from connection_test import Conn
from snowflake.snowpark.functions import col,min,max,avg,rank

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
session = Session.builder.configs(Conn.connect_to_snf()).create()

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

supplier_rank = supplier.select("s_name","s_acctbal",rank().over(Window.order_by("s_acctbal")).\
                                as_("RANK")).where(col("RANK")==1)
print(type(supplier_rank))
supplier_rank.show()


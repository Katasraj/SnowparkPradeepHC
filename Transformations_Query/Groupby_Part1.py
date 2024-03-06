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

employee = session.table("DEMO_DB.PUBLIC.EMPLOYEE_CSV")
employee.show()

employee_grp = employee.select("FIRST_NAME").group_by("FIRST_NAME")
employee_grp_cnt = employee_grp.count()
employee_grp_cnt.show()

# employee_dups = employee_grp_cnt.where(col("count")>1)
# employee_dups.show()

employee_dups = employee.select("FIRST_NAME").group_by("FIRST_NAME").count().where(col("count")>1)
employee_dups.show()
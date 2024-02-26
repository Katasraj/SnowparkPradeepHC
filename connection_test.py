import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.connector import connect

class Conn:

    @staticmethod
    def connect_to_snf():
        # Snowflake connection parameters
        connection_parameters = {
            'user': "Mossad1972",
            'password': "Katasraj2025%",
            'account': "jalvukt-gy82682",
            'warehouse': "COMPUTE_WH",
            'database': "DEMO_DB",
            'schema': "PUBLIC"
        }
        return connection_parameters

# Establish a connection using snowflake.connector
connection = connect(**Conn.connect_to_snf())
# Create a Snowpark session
test_session = Session.builder.configs(Conn.connect_to_snf()).create()

# Execute a simple query using the Snowpark session
result = test_session.sql("select current_warehouse(), current_database(), current_schema()").collect()

# Print the result
print(result)


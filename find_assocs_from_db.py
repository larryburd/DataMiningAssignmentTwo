#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260215
#
# Purpose: to retrieve data from the retail_store database and find
#           associations between various dimensions and facts
#############################################################################

# REGION: IMPORTS
import pandas as pd
import psycopg2

# REGION: RETRIEVE DATA
# Connect to the dataase
conn = psycopg2.connect(
    database="retail_store",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

# Get the data
df = pd.read_sql_query('SELECT * FROM select_by_cust_transaction()', conn)

print(df.info())
print(df.head())

conn.close()

# REGION: PERFORM ASSOCIATIVE ANALYSIS
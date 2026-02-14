#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260210
#
# Purpose: Fill the retail_store database with values retrieved from
#          retail_store_sales.csv
#############################################################################

import psycopg2

conn = psycopg2.connect(
    database="retail_store",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

cur.execute("SELECT * FROM Dim_Customers")
rows = cur.fetchall();

for row in rows:
    print(row)

import psycopg2

# Connect to the dataase
conn = psycopg2.connect(
    database="retail_store",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

cur.execute('TRUNCATE TABLE fact_sales, dim_customers, dim_dates, dim_products, dim_locations CASCADE')
conn.commit()
cur.close()
conn.close()
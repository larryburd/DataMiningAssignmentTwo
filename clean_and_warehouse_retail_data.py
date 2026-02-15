#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260210
#
# Purpose: to clean data from the retail_store_sale.csv dataset and insert
# the cleaned values into the database
#############################################################################

# REGION: IMPORTS
import pandas as pd
import numpy as np
import psycopg2
from datetime import date

# REGION: DATA CLEANING
# read in the data from the csv file
df = pd.read_csv('./retail_store_sales.csv')

# Remove duplicates
df = df.drop_duplicates(subset=['Transaction ID'])

# Replace all missing discount applied values as 0 (no discount)
df['Discount Applied'] = df['Discount Applied'].fillna('False');

# Find and drop all records that have more than two nulls
df = df[df.isnull().sum(axis=1) < 3]

# Convert data types 
df['Price Per Unit'] = pd.to_numeric(df['Price Per Unit'], errors='coerce')
df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
df['Total Spent'] = pd.to_numeric(df['Total Spent'], errors='coerce')
df['Discount Applied'].convert_dtypes(convert_boolean=True)
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])

# Set any strings to lower case.
df['Transaction ID'] = df['Transaction ID'].str.lower().str.strip()
df['Customer ID'] = df['Customer ID'].str.lower().str.strip()
df['Category'] = df['Category'].str.lower().str.strip()
df['Item'] = df['Item'].str.lower().str.strip()
df['Payment Method'] = df['Payment Method'].str.lower().str.strip()
df['Location'] = df['Location'].str.lower().str.strip()


# Validation of total and item columns
# Record whether <Price Per Unit> x <Quantity> = <Total Spent>
df['Total Is Correct'] = df['Price Per Unit'] * df['Quantity'] == df['Total Spent']

# Check if any invalid records exist
if len(df[df['Total Is Correct'] == False]) > 0:
    # Fix the records by determining price per unit by Total Spent/Quantity
    df['Price Per Unit'] = df.apply(
        lambda row: row['Total Spent'] / row['Quantity'] 
        if not row['Total Is Correct'] 
        else row['Price Per Unit'],
        axis=1
    )

# Fix missing Item entries by placing the item type that has the same category and price
no_empty_items = df.dropna()

def fill_item_col(row):
    # No change if item exists
    if pd.notna(row['Item']):
        return row['Item']
    
    # Attempt to match the price per unit and category to another item
    match = no_empty_items[(no_empty_items['Price Per Unit'] == row['Price Per Unit']) & (no_empty_items['Category'] == row['Category'])]

    # Return the item name if there is a match, else return not a number
    if not match.empty:
        return match.iloc[0]['Item']
    else:
        return np.nan

df['Item'] = df.apply(fill_item_col, axis=1)

# Calculated fields
# Get month name
df['Month Name'] = df['Transaction Date'].dt.month_name() # type: ignore

# Get FY
def get_fy(tran_date):
    full_year = tran_date.year
    month = tran_date.month

    # Add one to the year if we are in october or later
    if month > 10:
        full_year += 1

    return str(full_year)[-2:] # Returns the last two number of the year
    
df['FY'] = df['Transaction Date'].apply(get_fy)

# Get quarter
def get_quarter(tran_date):
    month = tran_date.month
    if month <= 3:
        return 1
    elif 3 < month <= 6:
        return 2
    elif 6 < month <= 9:
        return 3
    elif 9 < month <= 12:
        return 4
    else:
        return np.nan
    
df['fiscal_quarter'] = df['Transaction Date'].apply(get_quarter)

# Determine if date is on a weekend
def is_weekend(tran_date):
    day_name = tran_date.day_name()

    if day_name in ['Saturday', 'Sunday']:
        return True
    else:
        return False

df['Is Weekend'] = df['Transaction Date'].apply(is_weekend)

# REGION: DATABASE
# Get unique customer IDs
cust_ids = df['Customer ID'].unique()
# Get unique transaction dates and the calculated values
dates = df[['Transaction Date', 'Month Name', 'FY', 'fiscal_quarter', 'Is Weekend']].drop_duplicates()
dates['Transaction Date'] = dates['Transaction Date'].dt.strftime('%d-%m-%Y') # type: ignore
# Get unique products tuples
products = df[['Item', 'Category']].drop_duplicates()
# Get unique location
locations = df['Location'].unique()

# Connect to the dataase
conn = psycopg2.connect(
    database="retail_store",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# populate Dim_Customers table
# psycopg2 requires data values to be in tuple form
cust_strs = [(cust_id, ) for cust_id in cust_ids]
sql = "CALL insert_dim_customer(%s)"
cur.executemany(sql, cust_strs)

# populate Dim_Dates table
sql = "CALL insert_dim_date(%s, %s, %s, %s, %s)"
date_strs = [(date(int(dt[-4:]), int(dt[3:5]), int(dt[:2])), bool(is_weekend), str(month), int(quarter), int(fy)) for dt, month, fy, quarter, is_weekend in dates.values]
cur.executemany(sql, date_strs)

#populate dim_products table
sql = "CALL insert_dim_products(%s, %s)"
prod_strs = [(item, category) for category, item in products.values]
cur.executemany(sql, prod_strs)

# populate locations table
sql = "CALL insert_dim_locations(%s)"
loc_strs = [(loc, ) for loc in locations]
cur.executemany(sql, loc_strs)

# Populate fact_sales table
# Transform date
dates= df['Transaction Date'].dt.strftime('%d-%m-%Y') # type: ignore
df['Date'] = [date(int(dt[-4:]), int(dt[3:5]), int(dt[:2])) for dt in dates]

# Next retrieve the data from the database so that we have the IDs we want to use
# in the foreign key rows
# cur.execute("SELECT transactionDate FROM dim_dates")
# dates = cur.fetchall()

cur.execute("SELECT * FROM dim_products")
products = cur.fetchall()

cur.execute('SELECT * FROM dim_locations')
locations = cur.fetchall()

# Add foriegn key information to the dataframe
product_lookup = {p[1]: p[0] for p in products}
df['Prod ID'] = df['Item'].map(product_lookup)
print(df['Prod ID'].info())

loc_lookup = {l[1]: l[0] for l in locations}
df['Loc ID'] =df['Location'].map(loc_lookup)
print(df['Loc ID'].info())

print(df.info())

# Create value tuples and execute sale insert stored procedure
# sales_tuple = list(
#     df[['Customer ID', 'Date', 'Prod ID', 'Loc ID', 'Quantity', 'Price Per Unit', 'Total Spent']]
#     .itertuples(index=False, name=None)
# )
sales_tuple = [
    (
        str(r[1]),
        r[16],
        str(r[17]), 
        str(r[18]), 
        int(r[5]), 
        r[4],
        r[6]
    ) 
    for r in list(df.itertuples(index=False, name=None))
]
sql = "CALL insert_fact_sale(%s, %s, %s, %s, %s, %s, %s)"
cur.executemany(sql, sales_tuple)

# Commit and close connections
conn.commit()
cur.close()
conn.close()

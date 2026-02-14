#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260210
#
# Purpose: to clean data from the retail_store_sale.csv dataset
#############################################################################

import pandas as pd
import psycopg2

# read in the data from the csv file
df = pd.read_csv('./retail_store_sales.csv')

# MISSING VALUES STEP
# Find and drop all records that have more than two nulls
df = df[df.isnull().sum(axis=1) < 2]

# VALIDATION STEP
# Ensure that <Price Per Unit> x <Quantity> = <Total Spent>
df['total_is_correct'] = df['Price Per Unit'] * df['Quantity'] == df['Total Spent']

# Check if validation is correct
if len(df) != df['total_is_correct'].sum():
    print("Error validating <Price Per Unit> x <Quantity> = <Total Spent>")
    exit()

# STANDARDIZATION
# Set any strings to lower case. keep any non-strings as the current value
df = df.apply(lambda x: x.lower() if isinstance(x, str) else x)

# Connect to the dataase
conn = psycopg2.connect(
    database="retail_store",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()
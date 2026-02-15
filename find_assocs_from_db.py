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
from itertools import combinations
from collections import Counter

# REGION: FUNCTIONS
def print_df_info(title, df):
    print(f"\n{title}")
    print(f"\nShape: {df.shape}")
    print(f"Coumns: {df.columns.to_list()}")
    print("\nInfo:")
    print(df.info())
    print("\nFirst 10 rows:")
    print(df.head(10))
    print("\nBasic Statistics:")
    print(df.describe())

def get_top_pairs(pair_counts):
    top_pairs = []
    for pair, count in pair_counts.most_common():
        if count > 2:
            top_pairs.append((pair, count))
    return top_pairs

def get_most_purchased(items):
    counter = Counter(items)
    most_common = counter.most_common(1)
    if most_common:
        return most_common[0][0]
    return None

def write_out(titles, frames):
    ofile = open('./associations_output.txt', 'w')
    for i in range(len(titles)):
        ofile.write(f"{titles[i]}")
        ofile.write(f"\n{'_' * 60}\n")
        ofile.write(f"  {frames[i]}")
        ofile.write(f"\n{'#' * 60}\n\n")
    ofile.close()

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
sales_by_cust = pd.read_sql_query('SELECT * FROM transactions_by_cust()', conn)
items_by_date = pd.read_sql_query('SELECT * FROM products_bought_by_date()', conn)
conn.close()

#print_df_info("Sales by customer dataframe information:", sales_by_cust)
#print_df_info("Items by date dataframe information:", items_by_date)


# REGION: PERFORM ASSOCIATIVE ANALYSIS
# Products Frequently purchased together
all_pairs = []
multi_item_trans = [items for items in sales_by_cust['items'] if len(list(items)) > 1]
for transaction in multi_item_trans:
    if len(transaction) >= 2:
        pairs = list(combinations(sorted(transaction), 2))
        all_pairs.extend(pairs)
pair_counts = Counter(all_pairs)
top_pairs = get_top_pairs(pair_counts)

# Products purchased by month
items_sold_per_month = items_by_date.groupby('month_name')['items'].apply(list).reset_index()
items_sold_per_month.columns = ['month_name', 'items']
items_sold_per_month['most_purchased_item'] = items_sold_per_month['items'].apply(lambda items: get_most_purchased(items[0]))

# Products by location
prods_by_loc_and_qtr = sales_by_cust.groupby(['loc_name', 'qtr'])['items'].count()

# sales income by month
sales_by_month = sales_by_cust.groupby('month_name')['total_sales'].sum()

# Income per fiscal year
sales_by_fy = sales_by_cust.groupby('fy')['total_sales'].sum()

# Products purchased by each customer each month
sales_by_cust_and_month = sales_by_cust.groupby(['month_name', 'cust_id'])['items'].count()

# Products purchased by quarter
sales_by_quarter = items_by_date.groupby('qtr')['items'].count()

# Aggregate sales by customer (money spent and number of transactions)
customer_agg = sales_by_cust.groupby('cust_id').agg({
    'total_sales': 'sum',
    'trans_date': 'count'
}).rename(columns={'trans_date': 'num_trans'})

# top ten customers by sales amount
top_custs = customer_agg.nlargest(10, 'total_sales')

titles = ['Top Pairings', 'Item Count by Month', 'Products by Locations and Quarter', 
          'Sales by Month', 'Sales by Fiscal Year', 'Sales by Customer and Month',
          'Sales by Quarter', 'Top Customers']

frames = [
    top_pairs, 
    #item_count_by_month, 
    items_sold_per_month,
    prods_by_loc_and_qtr, 
    sales_by_month, 
    sales_by_fy,
    sales_by_cust_and_month,
    sales_by_quarter,
    top_custs
]

write_out(titles, frames)

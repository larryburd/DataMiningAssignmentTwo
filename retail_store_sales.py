#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260210
#
# Purpose: to analyze the retail_store_sales.csv file, determine what
#           what cleaning steps are necassary, and to create a data pipeline
#############################################################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import io
import pprint

# Returns the current time
def timestamp():
    return datetime.datetime.now()

# Function to get unique values of each column of a dataframe
def create_dict_with_uniques(df):
    unique_vals = dict()
    columns = list(df.columns)

    for col in columns:
        print(f"Col: {col}")
        unique_vals[col] = list(df[col].unique())

    return unique_vals

# Read the data from the csv file
df = pd.read_csv('./retail_store_sales.csv')

# General inspection of the data
df_head = df.head()
buf = io.StringIO() # buffer to hold the pandas.info() output as a string
df.info(buf=buf)
df_info = buf.getvalue()
buf.flush()
df_num_unique = df.nunique()
df_unique = pprint.pformat(create_dict_with_uniques(df))

# output file for saving findings
ofile = open('./retail_store_analysis.txt', 'w')
ofile.write(f"TIMESTAMP: {timestamp()}\n\n")
ofile.write("DF HEAD: \n")

# Write the first 5 entries out
ofile.write(df_head.to_string())

# Write the column names, number of non-null values, and data types
ofile.write(f"\n\nDF INFO: \n")
ofile.write(df_info)

# Write the number of unique values in each column
ofile.write(f"\n\nUNIQUE VALUES: \n")
ofile.write(df_num_unique.to_string())

# Get all NA values
df_na = df.loc[df.isna().any(axis="columns")]
df_na.info(buf=buf)
df_na_info = buf.getvalue()
ofile.write(f"\n\nNA VALUES: \n")
ofile.write(f"{df_na_info}\n\n")

# list all unique values per column
ofile.write("UNIQUE VALUES PER COLUMN: \n")
ofile.write(str(df_unique))

ofile.close()

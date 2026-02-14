#############################################################################
# Author: Laurence Burden
# Class: Data Mining
# Date: 20260210
#
# Purpose: to perform linear regression to best fill in missing values of
#          the retail_store_sales.csv file
#############################################################################

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# Read the data from the csv file
df = pd.read_csv('./retail_store_sales.csv')

# drop all NA records
df = df.dropna()

# check that we still have a significant number of records
print(df.info())
print(df.nunique())

# Change item strings to categories
item_cat = pd.Categorical(df["Item"]).codes
cust_cat = pd.Categorical(df["Customer ID"]).codes

# Build data set of independant variables
# We will use "Quantity", "Price per Unit", "Item" as a category, and "Total Spent"
# x = [list(group) for group in zip(df["Price Per Unit"].values, 
#           df["Quantity"].values, df["Total Spent"].values)]

x = [list(group) for group in zip(df["Total Spent"].values,
    df["Price Per Unit"].values, df["Quantity"].values, cust_cat)]

# The dependant variable is "discount applied"
# We map True to 1 and False to 0
bool_list = df["Discount Applied"].values

x = np.array(x)
y = np.array(bool_list, dtype=int)

model = LogisticRegression().fit(x,y)

# Model stats
r_sq = model.score(x,y)
intercept = model.intercept_
coef = model.coef_

print(f"r squared: {r_sq}\nintercept: {intercept}\ncoefficents: {coef}")
# y_p_floats = model.predict(x)
# y_pred = np.round(y_p_floats).astype(int)
y_pred = model.predict_proba(x)
print(f"Predicted Responses: \n{y_pred}")

# Check if the predictions match the original values
#print((y==y_pred).sum())




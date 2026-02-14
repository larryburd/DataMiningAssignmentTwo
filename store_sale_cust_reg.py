import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# Read the data from the csv file
df = pd.read_csv('./retail_store_sales.csv')

# drop all NA records
df = df.dropna()

cust_cat = pd.Categorical(df["Customer ID"]).codes

print(cust_cat)

# The dependant variable is "discount applied"
# We map True to 1 and False to 0
bool_list = df["Discount Applied"].values

x = np.array(cust_cat).reshape((-1,1))
y = np.array(bool_list, dtype=int)

model = LogisticRegression().fit(x,y)

# Model stats
r_sq = model.score(x,y)
intercept = model.intercept_
coef = model.coef_

print(f"r squared: {r_sq}\nintercept: {intercept}\ncoefficents: {coef}")
y_pred = model.predict_proba(x)

print(f"Predicted Responses: \n{y_pred}")

# Check if the predictions match the original values
#print((y==y_pred).sum())
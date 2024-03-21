import pyodbc
import pandas as pd
from vars import *
from session import Db_Session
import os
from func import *
import numpy
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from sklearn.linear_model import LinearRegression



def encode(item_freq):
    res = 0
    if item_freq > 0:
        res = 1
    return res


def main():
    conn_str = make_connection_String(odbc_driver, db_server, db_name, db_username, db_password)
    session = Db_Session(conn_str=conn_str)
    

    sales_df = session.fetch_dataframe("SELECT * FROM factSales")
    calendar_df = session.fetch_dataframe("SELECT * FROM dimCalendar")
    products_df = session.fetch_dataframe("SELECT * FROM dimProduct")

    """ BASKET ANALYSIS """
    # Convert 'rowKey' in products_df to int64 to match the type of 'idProduct'
    products_df['rowKey'] = products_df['rowKey'].astype('int64')
    products_df['name'] = products_df['name'].astype('category')

    # Now perform the merge
    merged_df = sales_df.merge(products_df, left_on='idProduct', right_on='rowKey')

    subset = merged_df[['idCalendar', 'name', 'receipt']]
    mlPrep = pd.crosstab(subset['receipt'], subset['name'])
    basket_input = mlPrep.applymap(encode)

    frequent_itemsets = apriori(basket_input, min_support=0.01, use_colnames=True)
    rules = association_rules(frequent_itemsets, metric="lift")
    rules.sort_values(["support", "confidence","lift"],axis = 0, ascending = False).head(8)

    print(rules)

    """ ============================="""


    """ LINEAR REGRESSSION """
    sales_merged_df = sales_df.merge(calendar_df, left_on='idCalendar', right_on='date')

    sales_merged_df['month_year'] = pd.to_datetime(sales_df['idCalendar']).dt.to_period('M')

    
    sales_merged_df['date'] = pd.to_datetime(sales_merged_df['idCalendar'])  

    sales_merged_df['month_year'] = sales_merged_df['date'].dt.to_period('M')

    monthly_sales = sales_merged_df.groupby('month_year')['unitsSold'].sum().reset_index()

    monthly_sales['month_year_num'] = monthly_sales['month_year'].apply(lambda x: (x - monthly_sales['month_year'].min()).n).astype(int)

    X = monthly_sales[['month_year_num']]
    y = monthly_sales['unitsSold']

    model = LinearRegression()
    model.fit(X, y)

    predictions = model.predict(X)

    print(predictions)

    monthly_sales['month_year_datetime'] = monthly_sales['month_year'].dt.to_timestamp()

    plt.figure(figsize=(10, 6))  
    plt.scatter(monthly_sales['month_year_datetime'], y, label='Actual Sales', color='blue')

    plt.plot(monthly_sales['month_year_datetime'], predictions, label='Predicted Sales', color='red')

    plt.title('Monthly Sales and Linear Regression Prediction')
    plt.xlabel('Month-Year')
    plt.ylabel('Units Sold')
    plt.xticks(rotation=45)  
    plt.legend()

    plt.tight_layout()  
    plt.show()
    
    """ ============================="""
    

    session.terminate_connection()
 

main()
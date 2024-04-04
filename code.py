import pandas as pd
from sqlalchemy import create_engine

# Replace these with your actual database connection details
DATABASE_TYPE = 'mysql'
DBAPI = 'pymysql'
HOST = 'avlokita'
USER = 'avi_user1'
PASSWORD = 'singleshotbasketball'
DATABASE = 'mydatabase'
PORT = 3306
CONNECTION_STRING = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# Establishing a connection to the database
engine = create_engine(CONNECTION_STRING)

# SQL queries to extract sales and inventory data
sales_query = """
SELECT sales_date, product_id, region, sales_amount, units_sold
FROM sales_data
WHERE sales_date BETWEEN '2022-11-01' AND '2023-03-31';
"""

inventory_query = """
SELECT inventory_date, product_id, units_in_stock
FROM inventory_data
WHERE inventory_date BETWEEN '2022-11-01' AND '2023-03-31';
"""

# Using pandas to execute queries and load the data into DataFrames
sales_data = pd.read_sql(sales_query, engine)
inventory_data = pd.read_sql(inventory_query, engine)

# Example processing: Calculating total sales and average inventory by product
total_sales_by_product = sales_data.groupby('product_id')['sales_amount'].sum().reset_index()
average_inventory_by_product = inventory_data.groupby('product_id')['units_in_stock'].mean().reset_index()

# Merging total sales and average inventory data on product_id
merged_data = pd.merge(total_sales_by_product, average_inventory_by_product, on='product_id', how='inner')

# Saving the processed data to a CSV file for Tableau visualization
merged_data.to_csv('sales_performance_dashboard_data.csv', index=False)

print("Data processing complete. File saved for Tableau visualization.")

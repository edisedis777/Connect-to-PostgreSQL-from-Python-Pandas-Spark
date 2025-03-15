#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL queries on Pandas DataFrames using pandasql.
This script demonstrates how to use SQL syntax to query Pandas DataFrames.
"""

import pandas as pd
import numpy as np
import os
from pandasql import sqldf
from pandas_postgres import load_sample_data, create_connection, read_table_to_dataframe

# Helper function to run SQL queries on pandas DataFrames
def run_sql_query(query, local_vars=None):
    """
    Run SQL query on pandas DataFrames
    
    Parameters:
    -----------
    query : str
        SQL query to run
    local_vars : dict, optional
        Dictionary of local variables (DataFrames) to query
        
    Returns:
    --------
    DataFrame : pandas DataFrame with query results
    """
    if local_vars is None:
        local_vars = locals()
    return sqldf(query, local_vars)

def create_example_dataframes():
    """
    Create example DataFrames to demonstrate SQL queries
    """
    # Create a customers DataFrame
    customers = pd.DataFrame({
        'customer_id': range(1, 6),
        'name': ['John Smith', 'Emma Johnson', 'Michael Brown', 'Olivia Davis', 'William Wilson'],
        'email': ['john@example.com', 'emma@example.com', 'michael@example.com', 
                 'olivia@example.com', 'william@example.com'],
        'age': [35, 28, 42, 31, 45],
        'signup_date': pd.to_datetime(['2023-01-15', '2023-02-20', '2023-01-05', 
                                      '2023-03-10', '2023-02-01'])
    })
    
    # Create an orders DataFrame
    orders = pd.DataFrame({
        'order_id': range(101, 111),
        'customer_id': [1, 2, 3, 1, 4, 2, 5, 3, 4, 5],
        'order_date': pd.to_datetime(['2023-03-01', '2023-03-05', '2023-03-10', 
                                     '2023-03-15', '2023-03-20', '2023-03-25', 
                                     '2023-04-01', '2023-04-05', '2023-04-10', '2023-04-15']),
        'amount': [120.50, 85.20, 200.00, 65.75, 150.30, 95.60, 180.20, 110.40, 75.90, 220.10]
    })
    
    return customers, orders

def run_sqldf_examples():
    """
    Run example SQL queries on pandas DataFrames
    """
    # Create example DataFrames
    customers, orders = create_example_dataframes()
    
    print("Customers DataFrame:")
    print(customers.head())
    print("\nOrders DataFrame:")
    print(orders.head())
    
    # Example 1: Basic SELECT query
    query1 = """
    SELECT name, email, age 
    FROM customers 
    WHERE age > 30 
    ORDER BY age DESC;
    """
    result1 = run_sql_query(query1, locals())
    print("\nExample 1: Customers older than 30, ordered by age:")
    print(result1)
    
    # Example 2: JOIN operation
    query2 = """
    SELECT c.name, o.order_id, o.order_date, o.amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    ORDER BY o.order_date;
    """
    result2 = run_sql_query(query2, locals())
    print("\nExample 2: Join customers with their orders:")
    print(result2)
    
    # Example 3: Aggregation
    query3 = """
    SELECT c.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
    ORDER BY total_spent DESC;
    """
    result3 = run_sql_query(query3, locals())
    print("\nExample 3: Customer order counts and total spending:")
    print(result3)
    
    # Example 4: Subquery
    query4 = """
    SELECT name, age
    FROM customers
    WHERE customer_id IN (
        SELECT customer_id 
        FROM orders 
        GROUP BY customer_id 
        HAVING SUM(amount) > 200
    );
    """
    result4 = run_sql_query(query4, locals())
    print("\nExample 4: Customers who spent more than $200:")
    print(result4)
    
    # Example 5: Date filtering
    query5 = """
    SELECT c.name, o.order_date, o.amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2023-03-15'
    ORDER BY o.order_date;
    """
    result5 = run_sql_query(query5, locals())
    print("\nExample 5: Orders from March 15, 2023 and later:")
    print(result5)

def run_database_examples():
    """
    Run SQL queries on DataFrame loaded from PostgreSQL
    """
    try:
        # Create connection
        engine = create_connection()
        
        # Read data from PostgreSQL
        df = read_table_to_dataframe('sample_data', engine)
        
        if df is not None:
            print("\nDataFrame from PostgreSQL:")
            print(df.head())
            
            # Example SQL query on DataFrame from database
            query = """
            SELECT * 
            FROM df 
            ORDER BY id 
            LIMIT 5;
            """
            result = run_sql_query(query, locals())
            print("\nSQL query on DataFrame from database:")
            print(result)
    except Exception as e:
        print(f"Error running database examples: {e}")

if __name__ == "__main__":
    print("Running SQL queries on local DataFrames:")
    run_sqldf_examples()
    
    print("\nRunning SQL queries on DataFrame from PostgreSQL:")
    run_database_examples()

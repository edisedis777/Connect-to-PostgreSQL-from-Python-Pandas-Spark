#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pandas integration with PostgreSQL database.
This script demonstrates how to read from and write to a PostgreSQL database using pandas.
"""

import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from config import DATABASE_CONFIG

def create_connection():
    """
    Create a database connection to PostgreSQL using SQLAlchemy
    """
    try:
        conn_string = f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['dbname']}"
        engine = create_engine(conn_string)
        return engine
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        sys.exit(1)

def read_table_to_dataframe(table_name, engine=None, query=None):
    """
    Read data from PostgreSQL table into a pandas DataFrame
    
    Parameters:
    -----------
    table_name : str
        Name of the table to read from
    engine : SQLAlchemy engine, optional
        Database connection engine
    query : str, optional
        Custom SQL query instead of table name
        
    Returns:
    --------
    DataFrame : pandas DataFrame with the query results
    """
    if engine is None:
        engine = create_connection()
        
    try:
        if query:
            df = pd.read_sql(query, engine)
        else:
            df = pd.read_sql_table(table_name, engine)
        print(f"Successfully read data from '{table_name}' table")
        return df
    except Exception as e:
        print(f"Error reading from PostgreSQL: {e}")
        return None

def write_dataframe_to_table(df, table_name, engine=None, if_exists='replace'):
    """
    Write pandas DataFrame to PostgreSQL table
    
    Parameters:
    -----------
    df : pandas DataFrame
        DataFrame to write to database
    table_name : str
        Name of the table to write to
    engine : SQLAlchemy engine, optional
        Database connection engine
    if_exists : str, optional
        How to behave if the table already exists
        - 'fail': Raise a ValueError
        - 'replace': Drop the table before inserting new values
        - 'append': Insert new values to the existing table
    """
    if engine is None:
        engine = create_connection()
        
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"Successfully wrote DataFrame to '{table_name}' table")
        return True
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        return False

def load_sample_data():
    """
    Load sample data from CSV file
    """
    try:
        # Assuming script is run from project root directory
        sample_data_path = os.path.join('data', 'sample_data.csv')
        df = pd.read_csv(sample_data_path)
        print(f"Successfully loaded sample data from {sample_data_path}")
        return df
    except Exception as e:
        print(f"Error loading sample data: {e}")
        return None

def run_basic_examples():
    """
    Run basic examples showing Pandas-PostgreSQL integration
    """
    # Create connection
    engine = create_connection()
    
    # Load sample data
    sample_df = load_sample_data()
    if sample_df is None:
        return
    
    # Write sample data to PostgreSQL
    write_dataframe_to_table(sample_df, 'sample_data', engine)
    
    # Read data back from PostgreSQL
    result_df = read_table_to_dataframe('sample_data', engine)
    if result_df is not None:
        print("\nFirst 5 rows from database:")
        print(result_df.head())
    
    # Example of custom query
    custom_query = "SELECT * FROM sample_data ORDER BY id LIMIT 3"
    query_df = read_table_to_dataframe('sample_data', engine, query=custom_query)
    if query_df is not None:
        print("\nCustom query results:")
        print(query_df)

if __name__ == "__main__":
    run_basic_examples()

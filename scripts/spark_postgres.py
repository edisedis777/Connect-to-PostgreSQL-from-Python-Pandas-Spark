#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Spark integration with PostgreSQL.
This script demonstrates how to read from and write to PostgreSQL using Apache Spark.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count
import pandas as pd
from config import DATABASE_CONFIG

def create_spark_session():
    """
    Create a Spark session with PostgreSQL JDBC driver
    """
    try:
        # Create a Spark session with PostgreSQL JDBC driver
        spark = (SparkSession.builder
                .appName("PostgreSQL-Spark Integration")
                .config("spark.jars", "postgresql-42.5.1.jar")  # Make sure to download this JAR file
                .getOrCreate())
        
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        sys.exit(1)

def get_jdbc_url():
    """
    Create JDBC URL for PostgreSQL connection
    """
    return f"jdbc:postgresql://{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['dbname']}"

def get_connection_properties():
    """
    Get connection properties for PostgreSQL
    """
    return {
        "user": DATABASE_CONFIG['user'],
        "password": DATABASE_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

def read_table_to_spark_df(spark, table_name):
    """
    Read PostgreSQL table into a Spark DataFrame
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    table_name : str
        Name of the table to read
        
    Returns:
    --------
    DataFrame : Spark DataFrame with the table data
    """
    try:
        df = (spark.read
             .format("jdbc")
             .option("url", get_jdbc_url())
             .option("dbtable", table_name)
             .option("user", DATABASE_CONFIG['user'])
             .option("password", DATABASE_CONFIG['password'])
             .option("driver", "org.postgresql.Driver")
             .load())
        
        print(f"Successfully read '{table_name}' into Spark DataFrame")
        return df
    except Exception as e:
        print(f"Error reading table '{table_name}': {e}")
        return None

def load_sample_data_to_spark(spark):
    """
    Load sample data from CSV to Spark DataFrame
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
        
    Returns:
    --------
    DataFrame : Spark DataFrame with sample data
    """
    try:
        # Assuming script is run from project root directory
        sample_data_path = os.path.join('data', 'sample_data.csv')
        
        # Read CSV file into Spark DataFrame
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        print(f"Successfully loaded sample data from {sample_data_path}")
        return df
    except Exception as e:
        print(f"Error loading sample data: {e}")
        
        # Create dummy data if CSV file is not available
        print("Creating dummy data for demonstration purposes")
        pandas_df = pd.DataFrame({
            'id': range(1, 11),
            'category': ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'B', 'A', 'D'],
            'amount': [120.5, 85.2, 200.0, 65.7, 150.3, 95.6, 180.2, 110.4, 75.9, 220.1],
            'date': pd.date_range(start='2023-01-01', periods=10).strftime('%Y-%m-%d').tolist()
        })
        return spark.createDataFrame(pandas_df)

def write_spark_df_to_table(df, table_name, mode="overwrite"):
    """
    Write Spark DataFrame to PostgreSQL table
    
    Parameters:
    -----------
    df : Spark DataFrame
        DataFrame to write to database
    table_name : str
        Name of the table to write to
    mode : str, optional
        Write mode (overwrite, append, ignore, error)
    """
    try:
        (df.write
           .format("jdbc")
           .option("url", get_jdbc_url())
           .option("dbtable", table_name)
           .option("user", DATABASE_CONFIG['user'])
           .option("password", DATABASE_CONFIG['password'])
           .option("driver", "org.postgresql.Driver")
           .mode(mode)
           .save())
        
        print(f"Successfully wrote Spark DataFrame to '{table_name}'")
        return True
    except Exception as e:
        print(f"Error writing to table '{table_name}': {e}")
        return False

def demonstrate_spark_transformations(df):
    """
    Demonstrate various Spark DataFrame transformations
    
    Parameters:
    -----------
    df : Spark DataFrame
        DataFrame to transform
    """
    try:
        print("\nDemonstrating Spark DataFrame transformations:")
        
        # Print schema
        print("\nDataFrame Schema:")
        df.printSchema()
        
        # Show data
        print("\nFirst 5 rows of data:")
        df.show(5)
        
        # Select specific columns
        print("\nSelecting specific columns:")
        df.select("id", "category").show(5)
        
        # Filter data
        print("\nFiltering data:")
        df.filter(col("amount") > 100).show(5)
        
        # Aggregate data
        print("\nAggregating data:")
        df.groupBy("category").agg(
            count("id").alias("count"),
            avg("amount").alias("avg_amount"),
            sum("amount").alias("total_amount")
        ).show()
        
        # Sort data
        print("\nSorting data:")
        df.orderBy(col("amount").desc()).show(5)
        
    except Exception as e:
        print(f"Error during Spark transformations: {e}")
        print("Please adjust the transformations to match your actual data schema.")

def run_spark_sql_example(spark, df):
    """
    Demonstrate Spark SQL capabilities
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    df : Spark DataFrame
        DataFrame to query
    """
    # Register DataFrame as a temporary view
    df.createOrReplaceTempView("sample_data")
    
    # Run SQL query
    sql_result = spark.sql("""
        SELECT * 
        FROM sample_data 
        ORDER BY id 
        LIMIT 5
    """)
    
    print("\nSpark SQL query result:")
    sql_result.show()
    
    # More complex SQL query example - adjust column names based on your actual data
    try:
        # Assuming 'category' and 'amount' columns exist in your data
        advanced_query = spark.sql("""
            SELECT 
                category,
                COUNT(*) as count,
                AVG(amount) as avg_value
            FROM sample_data
            GROUP BY category
            ORDER BY count DESC
        """)
        
        print("\nAdvanced Spark SQL query result:")
        advanced_query.show()
    except Exception as e:
        print(f"Could not run advanced query, likely due to schema mismatch: {e}")
        print("Please adjust the query to match your actual data schema.")

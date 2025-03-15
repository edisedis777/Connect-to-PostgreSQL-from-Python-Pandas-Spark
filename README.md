# Connect to PostgreSQL from Python, Pandas & Spark

This repository demonstrates various methods to interact with PostgreSQL databases using Python, with a focus on data analysis tools like Pandas and Apache Spark.

## Project Structure

```
postgres-python-spark/
│
├── requirements.txt         # Package dependencies
├── README.md                # Project documentation
├── docker-compose.yml       # Docker setup for PostgreSQL
│
├── data/                    # Sample data files
│   └── sample_data.csv      # Sample CSV data to use in examples
│
├── notebooks/              
│   ├── 01_pandas_postgres.ipynb     # Pandas read/write with PostgreSQL
│   ├── 02_sqldf_pandas.ipynb        # Using sqldf with Pandas
│   └── 03_spark_postgres.ipynb      # Spark integration with PostgreSQL
│
└── scripts/                 # Python scripts for each use case
    ├── config.py            # Database configuration
    ├── pandas_postgres.py   # Pandas read/write operations
    ├── sqldf_demo.py        # SQL queries on Pandas DataFrames
    └── spark_postgres.py    # Spark integration with PostgreSQL
```

## Setup Instructions

1. Clone this repository
2. Install required dependencies: `pip install -r requirements.txt`
3. Start PostgreSQL using Docker: `docker-compose up -d`
4. Run the example notebooks or scripts

## Examples Included

- Reading a CSV file into a Pandas DataFrame
- Writing a Pandas DataFrame to PostgreSQL
- Reading a PostgreSQL table into a Pandas DataFrame
- Using the `pandasql`/`sqldf` package to query Pandas DataFrames using SQL
- Reading data from PostgreSQL into Spark DataFrames
- Using Spark SQL to query data

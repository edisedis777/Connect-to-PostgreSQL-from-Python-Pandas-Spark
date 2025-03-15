"""
Database configuration settings
"""

# PostgreSQL connection parameters
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'demo',
    'user': 'postgres',
    'password': 'postgres'
}

# SQLAlchemy connection string
PG_CONNECTION_STRING = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"

# Spark JDBC connection parameters
SPARK_JDBC_CONFIG = {
    'url': f"jdbc:postgresql://{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}",
    'driver': 'org.postgresql.Driver',
    'user': PG_CONFIG['user'],
    'password': PG_CONFIG['password'],
    'properties': {
        'user': PG_CONFIG['user'],
        'password': PG_CONFIG['password'],
        'driver': 'org.postgresql.Driver'
    }
}

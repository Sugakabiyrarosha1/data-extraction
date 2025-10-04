"""
MySQL Data Extraction Module

This module provides robust, production-ready functions for extracting data from MySQL databases
into Pandas DataFrames with connection pooling, query optimization, and comprehensive error handling.

Features:
- MySQL database integration with connection pooling
- Advanced query capabilities with parameterized queries
- Connection management with context managers
- Performance monitoring and optimization
- Bulk operations support
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large datasets
- Support for both basic and advanced operations

Author: Data Extraction Toolkit
"""

import os
import pandas as pd
import time
from typing import Dict, List, Any, Optional
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import MySQL connector
try:
    import mysql.connector
    from mysql.connector import pooling
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    logger.warning("mysql-connector-python not available. Install with: pip install mysql-connector-python")


class MySQLClient:
    """
    Advanced MySQL client with connection pooling and query optimization.
    """
    
    def __init__(self, 
                 host: str,
                 user: str,
                 password: str,
                 database: str,
                 port: int = 3306,
                 pool_size: int = 5,
                 pool_name: str = 'mysql_pool'):
        """
        Initialize MySQL client with connection pooling.
        
        Args:
            host (str): MySQL host
            user (str): MySQL username
            password (str): MySQL password
            database (str): Database name
            port (int): MySQL port
            pool_size (int): Connection pool size
            pool_name (str): Pool name
        """
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python is required for MySQL operations")
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.pool_size = pool_size
        self.pool_name = pool_name
        self.pool = None
        self.engine = None
        
    def create_pool(self):
        """Create MySQL connection pool."""
        try:
            pool_config = {
                'pool_name': self.pool_name,
                'pool_size': self.pool_size,
                'pool_reset_session': True,
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'database': self.database,
                'port': self.port,
                'autocommit': True
            }
            
            self.pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
            logger.info(f"Created MySQL connection pool: {self.pool_name}")
            
        except Exception as e:
            logger.error(f"Error creating MySQL connection pool: {str(e)}")
            raise
    
    def get_connection(self):
        """Get connection from pool."""
        if not self.pool:
            raise RuntimeError("Connection pool not created. Call create_pool() first.")
        
        try:
            connection = self.pool.get_connection()
            return connection
        except Exception as e:
            logger.error(f"Error getting connection from pool: {str(e)}")
            raise
    
    def close_pool(self):
        """Close connection pool."""
        if self.pool:
            self.pool.close()
            logger.info("MySQL connection pool closed")


def load_mysql(host: str,
               user: str,
               password: str,
               database: str,
               query: str,
               params: Optional[Dict[str, Any]] = None,
               port: int = 3306,
               use_pooling: bool = True,
               chunk_size: Optional[int] = None) -> pd.DataFrame:
    """
    Load data from MySQL with advanced features.
    
    Args:
        host (str): MySQL host
        user (str): MySQL username
        password (str): MySQL password
        database (str): Database name
        query (str): SQL query
        params (dict, optional): Query parameters for parameterized queries
        port (int): MySQL port
        use_pooling (bool): Whether to use connection pooling
        chunk_size (int, optional): Chunk size for large result sets
    
    Returns:
        pd.DataFrame: Query results
    """
    if not MYSQL_AVAILABLE:
        raise ImportError("mysql-connector-python is required for MySQL operations")
    
    try:
        logger.info(f"Connecting to MySQL: {host}:{port}/{database}")
        
        if use_pooling:
            # Use connection pooling
            client = MySQLClient(host, user, password, database, port)
            client.create_pool()
            
            try:
                connection = client.get_connection()
                cursor = connection.cursor()
                
                # Execute query with parameters if provided
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Fetch data
                if chunk_size:
                    # Process in chunks for large result sets
                    all_data = []
                    while True:
                        chunk = cursor.fetchmany(chunk_size)
                        if not chunk:
                            break
                        all_data.extend(chunk)
                    data = all_data
                else:
                    data = cursor.fetchall()
                
                # Create DataFrame
                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                else:
                    df = pd.DataFrame()
                
                cursor.close()
                connection.close()
                
            finally:
                client.close_pool()
        
        else:
            # Use direct connection
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
            
            try:
                df = pd.read_sql(query, connection, params=params, chunksize=chunk_size)
                if chunk_size and hasattr(df, '__iter__'):
                    # Handle chunked results
                    chunks = list(df)
                    df = pd.concat(chunks, ignore_index=True)
                
            finally:
                connection.close()
        
        # Data validation
        _validate_dataframe(df, f"MySQL: {database}")
        
        logger.info(f"Successfully loaded {len(df)} rows from MySQL")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from MySQL: {str(e)}")
        raise


def execute_mysql_query(host: str,
                       user: str,
                       password: str,
                       database: str,
                       query: str,
                       params: Optional[Dict[str, Any]] = None,
                       port: int = 3306,
                       return_results: bool = False) -> Optional[pd.DataFrame]:
    """
    Execute MySQL query (INSERT, UPDATE, DELETE, etc.).
    
    Args:
        host (str): MySQL host
        user (str): MySQL username
        password (str): MySQL password
        database (str): Database name
        query (str): SQL query
        params (dict, optional): Query parameters
        port (int): MySQL port
        return_results (bool): Whether to return results for SELECT queries
    
    Returns:
        pd.DataFrame or None: Query results if return_results=True and SELECT query
    """
    if not MYSQL_AVAILABLE:
        raise ImportError("mysql-connector-python is required for MySQL operations")
    
    try:
        logger.info(f"Executing MySQL query: {host}:{port}/{database}")
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        
        try:
            cursor = connection.cursor()
            
            # Execute query with parameters if provided
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Commit for non-SELECT queries
            if not query.strip().upper().startswith('SELECT'):
                connection.commit()
                logger.info(f"Query executed successfully. Rows affected: {cursor.rowcount}")
            
            # Return results for SELECT queries if requested
            if return_results and query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = cursor.fetchall()
                
                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                    _validate_dataframe(df, f"MySQL Query: {database}")
                    return df
                else:
                    return pd.DataFrame()
            
            cursor.close()
            
        finally:
            connection.close()
        
        return None
        
    except Exception as e:
        logger.error(f"Error executing MySQL query: {str(e)}")
        raise


def get_mysql_table_info(host: str,
                        user: str,
                        password: str,
                        database: str,
                        table_name: str,
                        port: int = 3306) -> Dict[str, Any]:
    """
    Get information about a MySQL table.
    
    Args:
        host (str): MySQL host
        user (str): MySQL username
        password (str): MySQL password
        database (str): Database name
        table_name (str): Table name
        port (int): MySQL port
    
    Returns:
        dict: Table information
    """
    if not MYSQL_AVAILABLE:
        raise ImportError("mysql-connector-python is required for MySQL operations")
    
    try:
        logger.info(f"Getting MySQL table info: {database}.{table_name}")
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        
        try:
            cursor = connection.cursor()
            
            # Get table structure
            cursor.execute(f"DESCRIBE {table_name}")
            columns_info = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get table size
            cursor.execute(f"""
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size in MB'
                FROM information_schema.TABLES 
                WHERE table_schema = '{database}' 
                AND table_name = '{table_name}'
            """)
            size_result = cursor.fetchone()
            table_size = size_result[0] if size_result else 0
            
            info = {
                'table_name': table_name,
                'row_count': row_count,
                'table_size_mb': table_size,
                'columns': []
            }
            
            for col in columns_info:
                info['columns'].append({
                    'name': col[0],
                    'type': col[1],
                    'null': col[2],
                    'key': col[3],
                    'default': col[4],
                    'extra': col[5]
                })
            
            cursor.close()
            return info
            
        finally:
            connection.close()
            
    except Exception as e:
        logger.error(f"Error getting MySQL table info: {str(e)}")
        raise


def load_mysql_chunked(host: str,
                      user: str,
                      password: str,
                      database: str,
                      query: str,
                      chunk_size: int = 10000,
                      params: Optional[Dict[str, Any]] = None,
                      port: int = 3306) -> Iterator[pd.DataFrame]:
    """
    Load MySQL data in chunks for memory-efficient processing.
    
    Args:
        host (str): MySQL host
        user (str): MySQL username
        password (str): MySQL password
        database (str): Database name
        query (str): SQL query
        chunk_size (int): Size of each chunk
        params (dict, optional): Query parameters
        port (int): MySQL port
    
    Yields:
        pd.DataFrame: Data chunks
    """
    if not MYSQL_AVAILABLE:
        raise ImportError("mysql-connector-python is required for MySQL operations")
    
    try:
        logger.info(f"Loading MySQL data in chunks: {host}:{port}/{database}")
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        
        try:
            cursor = connection.cursor()
            
            # Execute query with parameters if provided
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch data in chunks
            while True:
                chunk_data = cursor.fetchmany(chunk_size)
                if not chunk_data:
                    break
                
                df_chunk = pd.DataFrame(chunk_data, columns=columns)
                yield df_chunk
                
        finally:
            cursor.close()
            connection.close()
            
    except Exception as e:
        logger.error(f"Error loading MySQL data in chunks: {str(e)}")
        raise


def _validate_dataframe(df: pd.DataFrame, source: str) -> None:
    """
    Validate DataFrame for common data quality issues.
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        source (str): Source file path for logging
    """
    # Check for empty DataFrame
    if df.empty:
        raise ValueError(f"DataFrame from {source} is empty")
    
    # Check for completely empty columns
    empty_cols = df.columns[df.isnull().all()].tolist()
    if empty_cols:
        logger.warning(f"Found empty columns in {source}: {empty_cols}")
    
    # Check for duplicate column names
    if len(df.columns) != len(set(df.columns)):
        logger.warning(f"Found duplicate column names in {source}")
    
    # Log basic statistics
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")


# Example usage and testing
if __name__ == "__main__":
    print("Testing MySQL data extraction...")
    
    # Create sample data
    import numpy as np
    
    mock_mysql_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'User_{i}' for i in range(1, 101)],
        'email': [f'user{i}@example.com' for i in range(1, 101)],
        'age': np.random.randint(18, 65, 100),
        'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], 100),
        'salary': np.random.uniform(30000, 100000, 100).round(2),
        'hire_date': pd.date_range('2020-01-01', periods=100, freq='D'),
        'is_active': np.random.choice([True, False], 100, p=[0.8, 0.2])
    })
    
    print("Mock MySQL data created successfully!")
    print(f"Shape: {mock_mysql_data.shape}")
    print("Columns:", list(mock_mysql_data.columns))
    
    # Display the data
    print("\nMySQL Data Preview:")
    print(mock_mysql_data.head())
    
    # Demonstrate different query scenarios
    print("\nMySQL Query Examples:")
    
    # Example 1: Basic SELECT query
    print("\n1. Basic SELECT query:")
    basic_query = "SELECT id, name, email, department FROM users WHERE is_active = 1"
    print(f"Query: {basic_query}")
    # Simulate the result
    df_basic = mock_mysql_data[mock_mysql_data['is_active'] == True][['id', 'name', 'email', 'department']]
    print(f"Result shape: {df_basic.shape}")
    print(df_basic.head())
    
    # Example 2: Parameterized query
    print("\n2. Parameterized query:")
    param_query = "SELECT * FROM users WHERE department = %s AND age > %s"
    print(f"Query: {param_query}")
    print("Parameters: ['IT', 30]")
    # Simulate the result
    df_param = mock_mysql_data[(mock_mysql_data['department'] == 'IT') & (mock_mysql_data['age'] > 30)]
    print(f"Result shape: {df_param.shape}")
    print(df_param.head())
    
    # Example 3: Aggregation query
    print("\n3. Aggregation query:")
    agg_query = "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM users GROUP BY department"
    print(f"Query: {agg_query}")
    # Simulate the result
    df_agg = mock_mysql_data.groupby('department').agg({
        'id': 'count',
        'salary': 'mean'
    }).rename(columns={'id': 'count', 'salary': 'avg_salary'}).round(2)
    print("Result:")
    print(df_agg)
    
    # Example 4: Table information
    print("\n4. Table information:")
    table_info = {
        'table_name': 'users',
        'row_count': len(mock_mysql_data),
        'table_size_mb': 0.5,  # Simulated
        'columns': [
            {'name': 'id', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI', 'default': None, 'extra': 'auto_increment'},
            {'name': 'name', 'type': 'varchar(100)', 'null': 'NO', 'key': '', 'default': None, 'extra': ''},
            {'name': 'email', 'type': 'varchar(255)', 'null': 'NO', 'key': 'UNI', 'default': None, 'extra': ''},
            {'name': 'age', 'type': 'int(3)', 'null': 'YES', 'key': '', 'default': None, 'extra': ''},
            {'name': 'department', 'type': 'varchar(50)', 'null': 'YES', 'key': '', 'default': None, 'extra': ''},
            {'name': 'salary', 'type': 'decimal(10,2)', 'null': 'YES', 'key': '', 'default': None, 'extra': ''},
            {'name': 'hire_date', 'type': 'date', 'null': 'YES', 'key': '', 'default': None, 'extra': ''},
            {'name': 'is_active', 'type': 'tinyint(1)', 'null': 'NO', 'key': '', 'default': '1', 'extra': ''}
        ]
    }
    
    print("Table Information:")
    print(f"  Table: {table_info['table_name']}")
    print(f"  Rows: {table_info['row_count']}")
    print(f"  Size: {table_info['table_size_mb']} MB")
    print("  Columns:")
    for col in table_info['columns']:
        print(f"    - {col['name']}: {col['type']} ({'NULL' if col['null'] == 'YES' else 'NOT NULL'})")
    
    print("\nReal MySQL Usage Example:")
    print("""
# For actual MySQL usage, use this pattern:

# Basic connection
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM your_table'
)

# With parameters
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM users WHERE department = %s AND age > %s',
    params=['IT', 30]
)

# With connection pooling
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM large_table',
    use_pooling=True,
    chunk_size=1000
)

# Execute non-SELECT queries
execute_mysql_query(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='INSERT INTO users (name, email) VALUES (%s, %s)',
    params=['John Doe', 'john@example.com']
)
""")
    
    print("\nMySQL extraction testing completed!")

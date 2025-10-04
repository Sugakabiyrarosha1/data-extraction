# MySQL Data Extraction

This folder contains robust, production-ready functions for extracting data from MySQL databases into Pandas DataFrames with connection pooling, query optimization, and comprehensive error handling.

## Features

- **MySQL Database Integration**: Full support for MySQL databases
- **Connection Pooling**: Efficient connection management with configurable pool sizes
- **Advanced Query Capabilities**: Support for parameterized queries and complex SQL
- **Performance Monitoring**: Built-in performance tracking and optimization
- **Bulk Operations**: Support for bulk insert, update, and delete operations
- **Chunked Loading**: Memory-efficient processing for large datasets
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Memory Management**: Efficient processing for large datasets
- **Table Information**: Get detailed table structure and statistics

## Files

- `mysql_extraction.py`: Main module containing all MySQL extraction functions

## Classes

### `MySQLClient`

Advanced MySQL client with connection management and query optimization.

**Parameters:**
- `host` (str): MySQL host
- `user` (str): MySQL username
- `password` (str): MySQL password
- `database` (str): Database name
- `port` (int): MySQL port
- `pool_size` (int): Connection pool size
- `pool_name` (str): Pool name

**Methods:**
- `create_pool()`: Create MySQL connection pool
- `get_connection()`: Get connection from pool
- `close_pool()`: Close connection pool

## Functions

### `load_mysql(host, user, password, database, query, params=None, port=3306, use_pooling=True, chunk_size=None)`

Main function for loading data from MySQL with advanced features.

**Parameters:**
- `host` (str): MySQL host
- `user` (str): MySQL username
- `password` (str): MySQL password
- `database` (str): Database name
- `query` (str): SQL query
- `params` (dict, optional): Query parameters for parameterized queries
- `port` (int): MySQL port
- `use_pooling` (bool): Whether to use connection pooling
- `chunk_size` (int, optional): Chunk size for large result sets

**Returns:**
- `pd.DataFrame`: Query results

### `execute_mysql_query(host, user, password, database, query, params=None, port=3306, return_results=False)`

Execute MySQL query (INSERT, UPDATE, DELETE, etc.).

**Parameters:**
- `host` (str): MySQL host
- `user` (str): MySQL username
- `password` (str): MySQL password
- `database` (str): Database name
- `query` (str): SQL query
- `params` (dict, optional): Query parameters
- `port` (int): MySQL port
- `return_results` (bool): Whether to return results for SELECT queries

**Returns:**
- `pd.DataFrame` or `None`: Query results if return_results=True and SELECT query

### `get_mysql_table_info(host, user, password, database, table_name, port=3306)`

Get information about a MySQL table.

**Parameters:**
- `host` (str): MySQL host
- `user` (str): MySQL username
- `password` (str): MySQL password
- `database` (str): Database name
- `table_name` (str): Table name
- `port` (int): MySQL port

**Returns:**
- `dict`: Table information including:
  - `table_name`: Table name
  - `row_count`: Number of rows
  - `table_size_mb`: Table size in MB
  - `columns`: List of column information

### `load_mysql_chunked(host, user, password, database, query, chunk_size=10000, params=None, port=3306)`

Load MySQL data in chunks for memory-efficient processing.

**Parameters:**
- `host` (str): MySQL host
- `user` (str): MySQL username
- `password` (str): MySQL password
- `database` (str): Database name
- `query` (str): SQL query
- `chunk_size` (int): Size of each chunk
- `params` (dict, optional): Query parameters
- `port` (int): MySQL port

**Returns:**
- `Iterator[pd.DataFrame]`: Data chunks

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic MySQL Loading

```python
from mysql_extraction import load_mysql

# Basic connection and data loading
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM your_table'
)
print(f"Loaded {len(df)} rows")
```

### Loading with Parameters

```python
# Parameterized query for security
query = "SELECT * FROM users WHERE department = %s AND age > %s"
params = ['IT', 30]

df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query=query,
    params=params
)
```

### Loading with Connection Pooling

```python
# Use connection pooling for better performance
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM large_table',
    use_pooling=True,
    chunk_size=1000
)
```

### Chunked Loading

```python
from mysql_extraction import load_mysql_chunked

# Load large dataset in chunks
for chunk in load_mysql_chunked(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM large_table',
    chunk_size=5000
):
    print(f"Processing chunk with {len(chunk)} rows")
    # Process chunk here
```

### Getting Table Information

```python
from mysql_extraction import get_mysql_table_info

# Get table structure and statistics
info = get_mysql_table_info(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    table_name='users'
)

print(f"Table: {info['table_name']}")
print(f"Rows: {info['row_count']}")
print(f"Size: {info['table_size_mb']} MB")
print("Columns:")
for col in info['columns']:
    print(f"  - {col['name']}: {col['type']}")
```

### Executing Non-SELECT Queries

```python
from mysql_extraction import execute_mysql_query

# Insert data
execute_mysql_query(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='INSERT INTO users (name, email) VALUES (%s, %s)',
    params=['John Doe', 'john@example.com']
)

# Update data
execute_mysql_query(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='UPDATE users SET age = %s WHERE id = %s',
    params=[30, 1]
)
```

### Using MySQLClient Directly

```python
from mysql_extraction import MySQLClient

# Create client with connection pooling
client = MySQLClient(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    pool_size=10
)

# Create connection pool
client.create_pool()

try:
    # Get connection from pool
    connection = client.get_connection()
    cursor = connection.cursor()
    
    # Execute query
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
    
    # Process results
    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    
finally:
    # Close connection pool
    client.close_pool()
```

## Dependencies

- `mysql-connector-python`: MySQL Python driver
- `pandas`: For DataFrame operations
- `numpy`: For numerical operations (used in examples)

## Connection String Formats

### Local MySQL
```
host='localhost'
port=3306
user='your_username'
password='your_password'
database='your_database'
```

### Remote MySQL
```
host='your-server.com'
port=3306
user='your_username'
password='your_password'
database='your_database'
```

### MySQL with SSL
```python
# Add SSL parameters to connection
connection = mysql.connector.connect(
    host='your-server.com',
    user='your_username',
    password='your_password',
    database='your_database',
    ssl_disabled=False,
    ssl_ca='path/to/ca.pem',
    ssl_cert='path/to/client-cert.pem',
    ssl_key='path/to/client-key.pem'
)
```

## Query Examples

### Basic Queries
```python
# Simple SELECT
query = "SELECT * FROM users"

# Filtered SELECT
query = "SELECT * FROM users WHERE age > 25"

# Limited results
query = "SELECT * FROM users LIMIT 100"
```

### Parameterized Queries
```python
# Single parameter
query = "SELECT * FROM users WHERE department = %s"
params = ['IT']

# Multiple parameters
query = "SELECT * FROM users WHERE department = %s AND age > %s"
params = ['IT', 30]

# Date range
query = "SELECT * FROM orders WHERE order_date BETWEEN %s AND %s"
params = ['2024-01-01', '2024-12-31']
```

### Complex Queries
```python
# JOIN query
query = """
SELECT u.name, u.email, d.department_name
FROM users u
JOIN departments d ON u.department_id = d.id
WHERE u.is_active = 1
"""

# Aggregation
query = """
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM users
GROUP BY department
HAVING COUNT(*) > 10
ORDER BY avg_salary DESC
"""

# Subquery
query = """
SELECT * FROM users
WHERE department IN (
    SELECT department FROM users
    GROUP BY department
    HAVING COUNT(*) > 5
)
"""
```

## Performance Optimization

### Connection Pooling
```python
# Use connection pooling for multiple operations
df = load_mysql(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM users',
    use_pooling=True
)
```

### Chunked Loading
```python
# Load large datasets in chunks
for chunk in load_mysql_chunked(
    host='localhost',
    user='your_username',
    password='your_password',
    database='your_database',
    query='SELECT * FROM large_table',
    chunk_size=10000
):
    # Process chunk
    pass
```

### Query Optimization
```python
# Use LIMIT for large tables
query = "SELECT * FROM large_table LIMIT 1000"

# Use specific columns
query = "SELECT id, name, email FROM users"

# Use WHERE clauses
query = "SELECT * FROM users WHERE created_date >= '2024-01-01'"
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **Connection Errors**: Handles MySQL connection failures
2. **Authentication Errors**: Clear error messages for auth failures
3. **Query Errors**: Handles invalid SQL syntax
4. **Data Validation**: Checks for empty results and malformed data
5. **Memory Management**: Chunked loading for large datasets
6. **Network Issues**: Automatic retry and timeout handling

## Security Considerations

1. **Use parameterized queries** to prevent SQL injection
2. **Never hardcode credentials** in your code
3. **Use environment variables** for sensitive information
4. **Implement proper error handling** to avoid exposing sensitive data
5. **Use SSL connections** for remote databases
6. **Validate and sanitize** all user inputs

## Best Practices

1. **Use connection pooling** for multiple operations
2. **Implement proper error handling** for production applications
3. **Use parameterized queries** for security
4. **Monitor query performance** and optimize as needed
5. **Use chunked loading** for large datasets
6. **Validate data** before processing
7. **Use appropriate indexes** for better query performance

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python mysql_extraction.py
```

This will demonstrate various MySQL operations with mock data.

## Common Issues and Solutions

### Connection Timeout
- Check network connectivity
- Verify MySQL server is running
- Increase timeout values
- Check firewall settings

### Authentication Failed
- Verify username and password
- Check user permissions
- Ensure user can connect from your IP
- Check MySQL user table

### Query Performance
- Use appropriate indexes
- Optimize query structure
- Use LIMIT for large result sets
- Consider query caching

### Memory Issues
- Use chunked loading for large datasets
- Implement pagination
- Monitor memory usage
- Use connection pooling

## MySQL Setup

1. Install MySQL server
2. Create database and user
3. Grant appropriate permissions
4. Install Python MySQL connector
5. Test connection

## Installation

```bash
# Install MySQL connector
pip install mysql-connector-python

# Or with conda
conda install mysql-connector-python
```

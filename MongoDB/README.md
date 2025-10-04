# MongoDB Data Extraction

This folder contains robust, production-ready functions for extracting data from MongoDB into Pandas DataFrames with connection pooling, query optimization, and comprehensive error handling.

## Features

- **MongoDB Atlas Integration**: Full support for MongoDB Atlas cloud databases
- **Connection Pooling**: Efficient connection management with configurable pool sizes
- **Advanced Query Capabilities**: Support for filters, projections, sorting, and aggregation
- **Batch Processing**: Memory-efficient processing for large collections
- **Context Managers**: Automatic connection handling with proper cleanup
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Nested Document Support**: Handle complex nested MongoDB documents
- **Memory Management**: Efficient processing for large datasets

## Files

- `mongodb_extraction.py`: Main module containing all MongoDB extraction functions

## Classes

### `MongoDBClient`

Advanced MongoDB client with connection management and query optimization.

**Parameters:**
- `uri` (str): MongoDB connection URI
- `db_name` (str): Database name
- `max_pool_size` (int): Maximum connection pool size

**Methods:**
- `connect()`: Establish connection to MongoDB
- `disconnect()`: Close MongoDB connection
- `__enter__()`: Context manager entry
- `__exit__()`: Context manager exit

## Functions

### `load_mongodb(uri, db_name, collection_name, query=None, projection=None, sort=None, limit=None, skip=None, batch_size=1000, use_connection_pooling=True)`

Main function for loading data from MongoDB with advanced query capabilities.

**Parameters:**
- `uri` (str): MongoDB connection URI
- `db_name` (str): Database name
- `collection_name` (str): Collection name
- `query` (dict, optional): MongoDB query filter
- `projection` (dict, optional): Fields to include/exclude
- `sort` (list, optional): Sort specification [(field, direction)]
- `limit` (int, optional): Maximum number of documents to return
- `skip` (int, optional): Number of documents to skip
- `batch_size` (int): Batch size for processing large collections
- `use_connection_pooling` (bool): Whether to use connection pooling

**Returns:**
- `pd.DataFrame`: Loaded MongoDB data

### `get_mongodb_collection_info(uri, db_name, collection_name)`

Get information about a MongoDB collection.

**Parameters:**
- `uri` (str): MongoDB connection URI
- `db_name` (str): Database name
- `collection_name` (str): Collection name

**Returns:**
- `dict`: Collection information including:
  - `total_documents`: Number of documents in collection
  - `total_size_bytes`: Total size in bytes
  - `average_document_size`: Average document size
  - `indexes`: Number of indexes
  - `sample_fields`: Sample field names

### `load_mongodb_aggregation(uri, db_name, collection_name, pipeline, batch_size=1000)`

Load data from MongoDB using aggregation pipeline.

**Parameters:**
- `uri` (str): MongoDB connection URI
- `db_name` (str): Database name
- `collection_name` (str): Collection name
- `pipeline` (list): MongoDB aggregation pipeline
- `batch_size` (int): Batch size for processing large results

**Returns:**
- `pd.DataFrame`: Aggregated data

### `_load_mongodb_batch(collection, find_params, batch_size)`

Internal function for loading MongoDB data in batches.

**Parameters:**
- `collection`: MongoDB collection object
- `find_params` (dict): Query parameters
- `batch_size` (int): Batch size

**Returns:**
- `pd.DataFrame`: Combined data from all batches

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic MongoDB Loading

```python
from mongodb_extraction import load_mongodb

# Basic connection and data loading
mongodb_uri = 'mongodb+srv://username:password@cluster0.mongodb.net/'
db_name = 'your_database'
collection_name = 'your_collection'

df = load_mongodb(mongodb_uri, db_name, collection_name)
print(f"Loaded {len(df)} documents")
```

### Advanced Query with Filters

```python
# Query with filters, projection, and sorting
query = {
    "is_active": True,
    "profile.age": {"$gte": 25},
    "created_at": {"$gte": "2024-01-01"}
}

projection = {
    "name": 1,
    "email": 1,
    "profile.age": 1,
    "profile.location": 1
}

sort = [("created_at", -1)]  # Sort by creation date descending

df = load_mongodb(
    uri=mongodb_uri,
    db_name=db_name,
    collection_name=collection_name,
    query=query,
    projection=projection,
    sort=sort,
    limit=100
)
```

### Aggregation Pipeline

```python
from mongodb_extraction import load_mongodb_aggregation

# Complex aggregation pipeline
pipeline = [
    {"$match": {"is_active": True}},
    {"$unwind": "$orders"},
    {"$group": {
        "_id": "$profile.location",
        "total_orders": {"$sum": 1},
        "total_amount": {"$sum": "$orders.amount"},
        "avg_amount": {"$avg": "$orders.amount"}
    }},
    {"$sort": {"total_amount": -1}},
    {"$limit": 10}
]

df_aggregated = load_mongodb_aggregation(
    uri=mongodb_uri,
    db_name=db_name,
    collection_name=collection_name,
    pipeline=pipeline
)
```

### Getting Collection Information

```python
from mongodb_extraction import get_mongodb_collection_info

# Get collection statistics
info = get_mongodb_collection_info(mongodb_uri, db_name, collection_name)
print(f"Total documents: {info['total_documents']}")
print(f"Collection size: {info['total_size_bytes'] / 1024**2:.2f} MB")
print(f"Sample fields: {info['sample_fields']}")
```

### Using MongoDBClient Directly

```python
from mongodb_extraction import MongoDBClient

# Use context manager for automatic connection handling
with MongoDBClient(mongodb_uri, db_name) as mongo_client:
    collection = mongo_client.db[collection_name]
    
    # Perform custom operations
    cursor = collection.find({"status": "active"})
    data = list(cursor)
    df = pd.DataFrame(data)
```

## Dependencies

- `pymongo`: MongoDB Python driver
- `pandas`: For DataFrame operations
- `ssl`: For secure connections (built-in)
- `urllib.parse`: For URL parsing (built-in)

## Connection URI Formats

### MongoDB Atlas
```
mongodb+srv://username:password@cluster0.mongodb.net/database_name
```

### Local MongoDB
```
mongodb://localhost:27017/database_name
```

### MongoDB with Authentication
```
mongodb://username:password@localhost:27017/database_name
```

## Query Examples

### Basic Queries
```python
# Find all documents
query = {}

# Find active users
query = {"is_active": True}

# Find users by age range
query = {"profile.age": {"$gte": 18, "$lte": 65}}

# Find users by location
query = {"profile.location": {"$in": ["New York", "London", "Tokyo"]}}
```

### Complex Queries
```python
# Find users with specific order criteria
query = {
    "orders": {
        "$elemMatch": {
            "amount": {"$gte": 100},
            "date": {"$gte": "2024-01-01"}
        }
    }
}

# Find users with nested conditions
query = {
    "profile.preferences.theme": "dark",
    "profile.preferences.notifications": True
}
```

### Projection Examples
```python
# Include only specific fields
projection = {"name": 1, "email": 1, "profile.age": 1}

# Exclude specific fields
projection = {"_id": 0, "internal_notes": 0}

# Include nested fields
projection = {
    "name": 1,
    "profile.location": 1,
    "profile.preferences": 1
}
```

## Aggregation Pipeline Examples

### Group by Location
```python
pipeline = [
    {"$group": {
        "_id": "$profile.location",
        "count": {"$sum": 1},
        "avg_age": {"$avg": "$profile.age"}
    }},
    {"$sort": {"count": -1}}
]
```

### Unwind and Group
```python
pipeline = [
    {"$unwind": "$orders"},
    {"$group": {
        "_id": "$name",
        "total_spent": {"$sum": "$orders.amount"},
        "order_count": {"$sum": 1}
    }},
    {"$sort": {"total_spent": -1}}
]
```

### Date-based Aggregation
```python
pipeline = [
    {"$match": {"created_at": {"$gte": "2024-01-01"}}},
    {"$group": {
        "_id": {"$dateToString": {"format": "%Y-%m", "date": "$created_at"}},
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
]
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **Connection Errors**: Handles MongoDB connection failures
2. **Authentication Errors**: Clear error messages for auth failures
3. **Query Errors**: Handles invalid query syntax
4. **Data Validation**: Checks for empty results and malformed data
5. **Memory Management**: Batch processing for large collections
6. **Network Issues**: Automatic retry and timeout handling

## Performance Considerations

1. **Connection Pooling**: Use connection pooling for multiple operations
2. **Batch Processing**: Large collections are automatically processed in batches
3. **Projection**: Use projection to limit fields and reduce data transfer
4. **Indexing**: Ensure proper indexes for query performance
5. **Memory Usage**: Monitor memory usage for large datasets

## Best Practices

1. **Use connection pooling** for multiple operations
2. **Implement proper error handling** for production applications
3. **Use projection** to limit unnecessary data transfer
4. **Monitor query performance** and use appropriate indexes
5. **Handle large collections** with batch processing
6. **Validate data** before processing
7. **Use aggregation pipelines** for complex data transformations

## Security Considerations

1. **Never hardcode credentials** in your code
2. **Use environment variables** for sensitive information
3. **Use SSL/TLS** for secure connections
4. **Implement proper authentication** and authorization
5. **Validate and sanitize** all query inputs
6. **Use connection pooling** to limit concurrent connections

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python mongodb_extraction.py
```

This will demonstrate various MongoDB operations with mock data.

## MongoDB Atlas Setup

1. Create a MongoDB Atlas account
2. Create a new cluster
3. Create a database user
4. Whitelist your IP address
5. Get the connection string
6. Use the connection string in your code

## Common Issues and Solutions

### Connection Timeout
- Check network connectivity
- Verify IP whitelist settings
- Increase timeout values

### Authentication Failed
- Verify username and password
- Check database user permissions
- Ensure correct database name

### Query Performance
- Use appropriate indexes
- Limit result set size
- Use projection to reduce data transfer

### Memory Issues
- Use batch processing for large collections
- Implement pagination
- Monitor memory usage

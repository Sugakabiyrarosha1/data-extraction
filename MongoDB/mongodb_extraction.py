"""
MongoDB Data Extraction Module

This module provides robust, production-ready functions for extracting data from MongoDB
into Pandas DataFrames with connection pooling, query optimization, and comprehensive error handling.

Features:
- MongoDB Atlas integration with connection pooling
- Advanced query capabilities with filters, projections, and sorting
- Batch processing for large collections
- Connection management with context managers
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Support for complex nested documents
- Memory-efficient processing for large datasets

Author: Data Extraction Toolkit
"""

import pandas as pd
import ssl
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from urllib.parse import quote_plus
from typing import Optional, Dict, Any, List
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBClient:
    """
    Advanced MongoDB client with connection management and query optimization.
    """
    
    def __init__(self, uri: str, db_name: str, max_pool_size: int = 100):
        """
        Initialize MongoDB client with connection pooling.
        
        Args:
            uri (str): MongoDB connection URI
            db_name (str): Database name
            max_pool_size (int): Maximum connection pool size
        """
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.max_pool_size = max_pool_size
        
    def connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(
                self.uri,
                maxPoolSize=self.max_pool_size,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=20000,
                socketTimeoutMS=20000,
                retryWrites=True,
                ssl=True,
                ssl_cert_reqs=ssl.CERT_NONE
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            logger.info(f"Successfully connected to MongoDB database: {self.db_name}")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def load_mongodb(uri: str,
                 db_name: str,
                 collection_name: str,
                 query: Optional[Dict] = None,
                 projection: Optional[Dict] = None,
                 sort: Optional[List[tuple]] = None,
                 limit: Optional[int] = None,
                 skip: Optional[int] = None,
                 batch_size: int = 1000,
                 use_connection_pooling: bool = True) -> pd.DataFrame:
    """
    Load data from MongoDB with advanced query capabilities.
    
    Args:
        uri (str): MongoDB connection URI
        db_name (str): Database name
        collection_name (str): Collection name
        query (dict, optional): MongoDB query filter
        projection (dict, optional): Fields to include/exclude
        sort (list, optional): Sort specification [(field, direction)]
        limit (int, optional): Maximum number of documents to return
        skip (int, optional): Number of documents to skip
        batch_size (int): Batch size for processing large collections
        use_connection_pooling (bool): Whether to use connection pooling
    
    Returns:
        pd.DataFrame: Loaded MongoDB data
    """
    try:
        logger.info(f"Connecting to MongoDB: {db_name}.{collection_name}")
        
        # Use context manager for automatic connection handling
        with MongoDBClient(uri, db_name) as mongo_client:
            collection = mongo_client.db[collection_name]
            
            # Get collection statistics
            total_docs = collection.count_documents(query or {})
            logger.info(f"Total documents in collection: {total_docs}")
            
            if total_docs == 0:
                logger.warning("Collection is empty")
                return pd.DataFrame()
            
            # Build query parameters
            find_params = {}
            if query:
                find_params['filter'] = query
            if projection:
                find_params['projection'] = projection
            if sort:
                find_params['sort'] = sort
            if limit:
                find_params['limit'] = limit
            if skip:
                find_params['skip'] = skip
            
            # Process data in batches for large collections
            if total_docs > batch_size and not limit:
                logger.info(f"Large collection detected, processing in batches of {batch_size}")
                return _load_mongodb_batch(collection, find_params, batch_size)
            else:
                # Load all data at once for smaller collections
                cursor = collection.find(**find_params)
                data = list(cursor)
                
                if not data:
                    logger.warning("No documents found matching query")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                df = pd.DataFrame(data)
                
                # Remove MongoDB _id field if present (usually not needed)
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                
                # Data validation
                _validate_dataframe(df, f"MongoDB: {db_name}.{collection_name}")
                
                logger.info(f"Successfully loaded {len(df)} documents from MongoDB")
                return df
                
    except Exception as e:
        logger.error(f"Error loading data from MongoDB: {str(e)}")
        raise


def _load_mongodb_batch(collection, find_params: Dict, batch_size: int) -> pd.DataFrame:
    """
    Load MongoDB data in batches to handle large collections.
    
    Args:
        collection: MongoDB collection object
        find_params (dict): Query parameters
        batch_size (int): Batch size
    
    Returns:
        pd.DataFrame: Combined data from all batches
    """
    all_data = []
    skip = 0
    
    while True:
        # Add skip parameter for pagination
        batch_params = find_params.copy()
        batch_params['skip'] = skip
        batch_params['limit'] = batch_size
        
        # Fetch batch
        cursor = collection.find(**batch_params)
        batch_data = list(cursor)
        
        if not batch_data:
            break
        
        all_data.extend(batch_data)
        skip += batch_size
        
        logger.info(f"Processed {len(all_data)} documents so far...")
        
        # Break if we got fewer documents than batch size (last batch)
        if len(batch_data) < batch_size:
            break
    
    if not all_data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Remove MongoDB _id field if present
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    
    # Data validation
    _validate_dataframe(df, f"MongoDB batch: {collection.name}")
    
    logger.info(f"Successfully loaded {len(df)} documents in batches")
    return df


def get_mongodb_collection_info(uri: str, db_name: str, collection_name: str) -> Dict[str, Any]:
    """
    Get information about a MongoDB collection.
    
    Args:
        uri (str): MongoDB connection URI
        db_name (str): Database name
        collection_name (str): Collection name
    
    Returns:
        dict: Collection information
    """
    try:
        with MongoDBClient(uri, db_name) as mongo_client:
            collection = mongo_client.db[collection_name]
            
            # Get collection stats
            stats = mongo_client.db.command("collStats", collection_name)
            
            # Get sample document structure
            sample_doc = collection.find_one()
            
            info = {
                'total_documents': stats.get('count', 0),
                'total_size_bytes': stats.get('size', 0),
                'average_document_size': stats.get('avgObjSize', 0),
                'indexes': stats.get('nindexes', 0),
                'sample_fields': list(sample_doc.keys()) if sample_doc else []
            }
            
            return info
            
    except Exception as e:
        logger.error(f"Error getting MongoDB collection info: {str(e)}")
        raise


def load_mongodb_aggregation(uri: str,
                            db_name: str,
                            collection_name: str,
                            pipeline: List[Dict],
                            batch_size: int = 1000) -> pd.DataFrame:
    """
    Load data from MongoDB using aggregation pipeline.
    
    Args:
        uri (str): MongoDB connection URI
        db_name (str): Database name
        collection_name (str): Collection name
        pipeline (list): MongoDB aggregation pipeline
        batch_size (int): Batch size for processing large results
    
    Returns:
        pd.DataFrame: Aggregated data
    """
    try:
        logger.info(f"Running aggregation pipeline on {db_name}.{collection_name}")
        
        with MongoDBClient(uri, db_name) as mongo_client:
            collection = mongo_client.db[collection_name]
            
            # Execute aggregation pipeline
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            data = list(cursor)
            
            if not data:
                logger.warning("No data returned from aggregation pipeline")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Remove MongoDB _id field if present
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            # Data validation
            _validate_dataframe(df, f"MongoDB aggregation: {db_name}.{collection_name}")
            
            logger.info(f"Successfully loaded {len(df)} documents from aggregation")
            return df
            
    except Exception as e:
        logger.error(f"Error running MongoDB aggregation: {str(e)}")
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
    print("Testing MongoDB data extraction...")
    
    # Mock MongoDB data structure for demonstration
    mock_mongodb_data = [
        {
            "user_id": "507f1f77bcf86cd799439011",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "profile": {
                "age": 28,
                "location": "New York",
                "preferences": {
                    "theme": "dark",
                    "notifications": True
                }
            },
            "orders": [
                {"order_id": "ORD001", "product": "Laptop", "amount": 999.99, "date": "2024-01-15"},
                {"order_id": "ORD002", "product": "Mouse", "amount": 29.99, "date": "2024-01-20"}
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "is_active": True
        },
        {
            "user_id": "507f1f77bcf86cd799439012",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "profile": {
                "age": 35,
                "location": "London",
                "preferences": {
                    "theme": "light",
                    "notifications": False
                }
            },
            "orders": [
                {"order_id": "ORD003", "product": "Phone", "amount": 699.99, "date": "2024-01-18"}
            ],
            "created_at": "2024-01-02T00:00:00Z",
            "is_active": True
        },
        {
            "user_id": "507f1f77bcf86cd799439013",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "profile": {
                "age": 42,
                "location": "Tokyo",
                "preferences": {
                    "theme": "auto",
                    "notifications": True
                }
            },
            "orders": [],
            "created_at": "2024-01-03T00:00:00Z",
            "is_active": False
        }
    ]
    
    # Simulate MongoDB data loading
    print("Simulating MongoDB data extraction...")
    
    # Convert mock data to DataFrame (simulating MongoDB extraction)
    df_mongo = pd.DataFrame(mock_mongodb_data)
    
    # Remove MongoDB _id field (simulated)
    if '_id' in df_mongo.columns:
        df_mongo = df_mongo.drop('_id', axis=1)
    
    print(f"Successfully loaded {len(df_mongo)} documents from MongoDB")
    print(f"Shape: {df_mongo.shape}")
    print(f"Columns: {list(df_mongo.columns)}")
    
    # Display the data
    print("\nMongoDB Data Preview:")
    print(df_mongo.head())
    
    # Demonstrate different query scenarios
    print("\nMongoDB Query Examples:")
    
    # Example 1: Filter active users only
    print("\n1. Active users only:")
    active_users = df_mongo[df_mongo['is_active'] == True]
    print(f"Active users: {len(active_users)}")
    print(active_users[['name', 'email', 'is_active']])
    
    # Example 2: Users with orders
    print("\n2. Users with orders:")
    users_with_orders = df_mongo[df_mongo['orders'].apply(len) > 0]
    print(f"Users with orders: {len(users_with_orders)}")
    print(users_with_orders[['name', 'email', 'orders']])
    
    # Example 3: Flatten nested profile data
    print("\n3. Flattened profile data:")
    profile_data = []
    for _, row in df_mongo.iterrows():
        profile = row['profile'].copy()
        profile['user_id'] = row['user_id']
        profile['name'] = row['name']
        profile['email'] = row['email']
        profile_data.append(profile)
    
    df_profiles = pd.DataFrame(profile_data)
    print(df_profiles)
    
    # Example 4: Expand orders data
    print("\n4. Expanded orders data:")
    orders_data = []
    for _, row in df_mongo.iterrows():
        for order in row['orders']:
            order_data = order.copy()
            order_data['user_id'] = row['user_id']
            order_data['user_name'] = row['name']
            orders_data.append(order_data)
    
    if orders_data:
        df_orders = pd.DataFrame(orders_data)
        print(df_orders)
    else:
        print("No orders found")
    
    print("\nReal MongoDB Usage Example:")
    print("""
# For actual MongoDB usage, use this pattern:

mongodb_uri = 'mongodb+srv://username:password@cluster0.mongodb.net/'
db_name = 'your_database'
collection_name = 'your_collection'

# Basic query
df = load_mongodb(mongodb_uri, db_name, collection_name)

# Advanced query with filters
query = {"is_active": True, "profile.age": {"$gte": 25}}
projection = {"name": 1, "email": 1, "profile.age": 1}
sort = [("created_at", -1)]  # Sort by creation date descending

df_filtered = load_mongodb(
    uri=mongodb_uri,
    db_name=db_name,
    collection_name=collection_name,
    query=query,
    projection=projection,
    sort=sort,
    limit=100
)

# Aggregation pipeline
pipeline = [
    {"$match": {"is_active": True}},
    {"$group": {"_id": "$profile.location", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]

df_aggregated = load_mongodb_aggregation(
    uri=mongodb_uri,
    db_name=db_name,
    collection_name=collection_name,
    pipeline=pipeline
)
""")
    
    print("\nMongoDB extraction testing completed!")

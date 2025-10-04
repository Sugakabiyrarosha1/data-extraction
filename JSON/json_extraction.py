"""
JSON Data Extraction Module

This module provides robust, production-ready functions for extracting data from JSON files
into Pandas DataFrames with support for nested structures, arrays, and various JSON formats.

Features:
- JSON file processing with nested structure support
- Array handling with multiple strategies (expand, join, first, last)
- Streaming parser for large JSON files
- JSONL (JSON Lines) format support
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large files
- Flexible parsing options and configuration

Author: Data Extraction Toolkit
"""

import json
import os
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import ijson for streaming large files
try:
    import ijson
    IJSON_AVAILABLE = True
except ImportError:
    IJSON_AVAILABLE = False
    logger.warning("ijson not available. Large file streaming will be disabled.")


def load_json(path: str,
              encoding: str = 'utf-8',
              flatten_nested: bool = True,
              handle_arrays: str = 'expand',
              max_depth: int = 10) -> pd.DataFrame:
    """
    Load JSON data with advanced processing capabilities.
    
    Args:
        path (str): Path to the JSON file
        encoding (str): File encoding (default: 'utf-8')
        flatten_nested (bool): Whether to flatten nested structures
        handle_arrays (str): How to handle arrays:
            - 'expand': Expand arrays into separate rows
            - 'join': Join array elements with separator
            - 'first': Take first element only
            - 'last': Take last element only
        max_depth (int): Maximum depth for flattening nested structures
    
    Returns:
        pd.DataFrame: Processed JSON data
    """
    try:
        # Check if file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")
        
        logger.info(f"Loading JSON file: {path}")
        
        # Get file size to determine processing method
        file_size = os.path.getsize(path)
        logger.info(f"File size: {file_size / 1024**2:.2f} MB")
        
        # For large files, use streaming parser if available
        if file_size > 50 * 1024**2 and IJSON_AVAILABLE:  # 50MB threshold
            logger.info("Large file detected, using streaming parser...")
            return _load_json_streaming(path, encoding, flatten_nested, handle_arrays)
        
        # Load entire file for smaller files
        with open(path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Process the data
        df = _process_json_data(data, flatten_nested, handle_arrays, max_depth)
        
        # Data validation
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} records from JSON")
        return df
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading JSON file {path}: {str(e)}")
        raise


def _load_json_streaming(path: str, encoding: str, flatten_nested: bool, handle_arrays: str) -> pd.DataFrame:
    """
    Load large JSON files using streaming parser.
    
    Args:
        path (str): Path to JSON file
        encoding (str): File encoding
        flatten_nested (bool): Whether to flatten nested structures
        handle_arrays (str): How to handle arrays
    
    Returns:
        pd.DataFrame: Processed data
    """
    if not IJSON_AVAILABLE:
        raise ImportError("ijson is required for streaming large JSON files")
    
    try:
        records = []
        
        with open(path, 'rb') as f:
            # Parse JSON array items one by one
            parser = ijson.items(f, 'item')
            for item in parser:
                processed_item = _process_single_json_item(item, flatten_nested, handle_arrays)
                records.append(processed_item)
                
                # Limit memory usage for very large files
                if len(records) > 100000:  # 100k records limit
                    logger.warning("Reached 100k records limit, stopping...")
                    break
        
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"Error in streaming JSON parser: {str(e)}")
        raise


def _process_json_data(data: Any, flatten_nested: bool, handle_arrays: str, max_depth: int) -> pd.DataFrame:
    """
    Process JSON data into DataFrame format.
    
    Args:
        data: JSON data (dict, list, or other)
        flatten_nested (bool): Whether to flatten nested structures
        handle_arrays (str): How to handle arrays
        max_depth (int): Maximum depth for flattening
    
    Returns:
        pd.DataFrame: Processed data
    """
    # Handle different JSON structures
    if isinstance(data, list):
        # Array of objects - most common case
        if data and isinstance(data[0], dict):
            processed_data = [_process_single_json_item(item, flatten_nested, handle_arrays) for item in data]
            return pd.DataFrame(processed_data)
        else:
            # Array of primitives
            return pd.DataFrame({'value': data})
    
    elif isinstance(data, dict):
        # Single object or nested structure
        if flatten_nested:
            flattened = _flatten_dict(data, max_depth)
            return pd.DataFrame([flattened])
        else:
            return pd.DataFrame([data])
    
    else:
        # Primitive value
        return pd.DataFrame({'value': [data]})


def _process_single_json_item(item: dict, flatten_nested: bool, handle_arrays: str) -> dict:
    """
    Process a single JSON object item.
    
    Args:
        item (dict): JSON object to process
        flatten_nested (bool): Whether to flatten nested structures
        handle_arrays (str): How to handle arrays
    
    Returns:
        dict: Processed item
    """
    if not flatten_nested:
        return item
    
    processed = {}
    
    for key, value in item.items():
        if isinstance(value, dict):
            # Flatten nested dictionary
            flattened = _flatten_dict(value)
            for nested_key, nested_value in flattened.items():
                processed[f"{key}_{nested_key}"] = nested_value
        
        elif isinstance(value, list):
            # Handle arrays based on strategy
            if handle_arrays == 'expand' and value and isinstance(value[0], dict):
                # Expand array of objects
                for i, array_item in enumerate(value):
                    if isinstance(array_item, dict):
                        flattened = _flatten_dict(array_item)
                        for nested_key, nested_value in flattened.items():
                            processed[f"{key}_{i}_{nested_key}"] = nested_value
            elif handle_arrays == 'join':
                processed[key] = '|'.join(str(v) for v in value)
            elif handle_arrays == 'first':
                processed[key] = value[0] if value else None
            elif handle_arrays == 'last':
                processed[key] = value[-1] if value else None
            else:
                processed[key] = value
        
        else:
            processed[key] = value
    
    return processed


def _flatten_dict(d: dict, max_depth: int = 10, current_depth: int = 0) -> dict:
    """
    Recursively flatten a nested dictionary.
    
    Args:
        d (dict): Dictionary to flatten
        max_depth (int): Maximum recursion depth
        current_depth (int): Current recursion depth
    
    Returns:
        dict: Flattened dictionary
    """
    if current_depth >= max_depth:
        return d
    
    flattened = {}
    
    for key, value in d.items():
        if isinstance(value, dict):
            nested = _flatten_dict(value, max_depth, current_depth + 1)
            for nested_key, nested_value in nested.items():
                flattened[f"{key}_{nested_key}"] = nested_value
        else:
            flattened[key] = value
    
    return flattened


def load_jsonl(path: str, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Load JSON Lines (JSONL) file where each line is a separate JSON object.
    
    Args:
        path (str): Path to JSONL file
        encoding (str): File encoding
    
    Returns:
        pd.DataFrame: Loaded data
    """
    try:
        records = []
        
        with open(path, 'r', encoding=encoding) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        continue
        
        if not records:
            logger.warning("No valid JSON records found in file")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} records from JSONL file")
        return df
        
    except Exception as e:
        logger.error(f"Error loading JSONL file {path}: {str(e)}")
        raise


def load_json_with_schema(path: str, schema: Dict[str, Any], **kwargs) -> pd.DataFrame:
    """
    Load JSON file with predefined schema for data type enforcement.
    
    Args:
        path (str): Path to the JSON file
        schema (dict): Dictionary mapping column names to data types
        **kwargs: Additional arguments passed to load_json()
    
    Returns:
        pd.DataFrame: Loaded DataFrame with enforced schema
    """
    df = load_json(path, **kwargs)
    
    # Apply schema
    for column, dtype in schema.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except Exception as e:
                logger.warning(f"Could not convert column {column} to {dtype}: {e}")
    
    return df


def export_to_json(data: pd.DataFrame, path: str, orient: str = 'records', **kwargs) -> None:
    """
    Export DataFrame to JSON file.
    
    Args:
        data (pd.DataFrame): DataFrame to export
        path (str): Output JSON file path
        orient (str): JSON orientation ('records', 'index', 'values', 'table')
        **kwargs: Additional arguments passed to pd.DataFrame.to_json()
    """
    try:
        data.to_json(path, orient=orient, **kwargs)
        logger.info(f"Successfully exported DataFrame to {path}")
        
    except Exception as e:
        logger.error(f"Error exporting to JSON file {path}: {str(e)}")
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
    print("Testing JSON data extraction with various formats...")
    
    # Create sample JSON data for demonstration
    sample_json_data = {
        "users": [
            {
                "id": 1,
                "name": "Alice Johnson",
                "email": "alice@example.com",
                "profile": {
                    "age": 28,
                    "location": {
                        "city": "New York",
                        "country": "USA",
                        "coordinates": [40.7128, -74.0060]
                    },
                    "preferences": {
                        "theme": "dark",
                        "notifications": True
                    }
                },
                "orders": [
                    {"id": 101, "product": "Laptop", "price": 999.99},
                    {"id": 102, "product": "Mouse", "price": 29.99}
                ],
                "tags": ["premium", "tech-savvy", "early-adopter"]
            },
            {
                "id": 2,
                "name": "Bob Smith",
                "email": "bob@example.com",
                "profile": {
                    "age": 35,
                    "location": {
                        "city": "London",
                        "country": "UK",
                        "coordinates": [51.5074, -0.1278]
                    },
                    "preferences": {
                        "theme": "light",
                        "notifications": False
                    }
                },
                "orders": [
                    {"id": 201, "product": "Phone", "price": 699.99}
                ],
                "tags": ["business", "mobile-first"]
            }
        ],
        "metadata": {
            "total_users": 2,
            "last_updated": "2024-01-15T10:30:00Z"
        }
    }
    
    # Save sample JSON file
    json_path = 'sample_data.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(sample_json_data, f, indent=2)
    
    print(f"Created sample JSON file: {json_path}")
    
    try:
        # Test 1: Load with flattening (default)
        print("\n=== Testing JSON Loading with Nested Flattening ===")
        df_json_flat = load_json(json_path, flatten_nested=True, handle_arrays='expand')
        print(f"Shape: {df_json_flat.shape}")
        print("Columns:", list(df_json_flat.columns))
        print("\nFirst 3 rows:")
        print(df_json_flat.head(3))
        
        # Test 2: Load without flattening
        print("\n=== Testing JSON Loading without Flattening ===")
        df_json_nested = load_json(json_path, flatten_nested=False)
        print(f"Shape: {df_json_nested.shape}")
        print("\nFirst 3 rows:")
        print(df_json_nested.head(3))
        
        # Test 3: Different array handling strategies
        print("\n=== Testing Different Array Handling Strategies ===")
        
        # Join arrays
        df_json_join = load_json(json_path, handle_arrays='join')
        print("Array handling - 'join':")
        print(df_json_join[['tags']].head())
        
        # First element only
        df_json_first = load_json(json_path, handle_arrays='first')
        print("Array handling - 'first':")
        print(df_json_first[['tags']].head())
        
        # Test 4: Create and load JSONL file
        print("\n=== Testing JSONL Format ===")
        jsonl_path = 'sample_data.jsonl'
        
        # Create JSONL file
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for user in sample_json_data['users']:
                f.write(json.dumps(user) + '\n')
        
        # Load JSONL
        df_jsonl = load_jsonl(jsonl_path)
        print(f"JSONL Shape: {df_jsonl.shape}")
        print("\nFirst 3 rows:")
        print(df_jsonl.head(3))
        
        # Test 5: Load with schema
        print("\n=== Testing JSON Loading with Schema ===")
        schema = {
            'id': 'int64',
            'name': 'string',
            'email': 'string'
        }
        df_schema = load_json_with_schema(json_path, schema)
        print(f"Data types with schema:")
        print(df_schema.dtypes)
        
        # Test 6: Export to JSON
        print("\n=== Testing JSON Export ===")
        export_path = 'exported_data.json'
        export_to_json(df_json_flat, export_path, orient='records', indent=2)
        print(f"Exported data to {export_path}")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        # Clean up sample files
        for file_path in [json_path, jsonl_path, 'exported_data.json']:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Cleaned up sample file: {file_path}")
    
    print("\nJSON extraction testing completed!")

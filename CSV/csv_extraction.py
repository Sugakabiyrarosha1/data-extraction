"""
CSV Data Extraction Module

This module provides robust, production-ready functions for extracting data from CSV files
into Pandas DataFrames with advanced error handling, encoding detection, and data validation.

Features:
- Automatic encoding detection using chardet
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Flexible parsing options (delimiter, skip rows, etc.)
- Memory usage monitoring
- Logging for debugging and monitoring

Author: Data Extraction Toolkit
"""

import pandas as pd
import chardet
import os
from typing import Optional, Dict, Any
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_csv(path: str, 
             encoding: Optional[str] = None,
             delimiter: str = ',',
             skip_rows: int = 0,
             validate_data: bool = True,
             **kwargs) -> pd.DataFrame:
    """
    Load CSV data with advanced error handling and data validation.
    
    Args:
        path (str): Path to the CSV file
        encoding (str, optional): File encoding. Auto-detected if None
        delimiter (str): CSV delimiter (default: ',')
        skip_rows (int): Number of rows to skip from the beginning
        validate_data (bool): Whether to perform data validation
        **kwargs: Additional arguments passed to pd.read_csv()
    
    Returns:
        pd.DataFrame: Loaded and validated DataFrame
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If data validation fails
    """
    try:
        # Check if file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")
        
        # Auto-detect encoding if not provided
        if encoding is None:
            logger.info("Auto-detecting file encoding...")
            with open(path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                encoding_result = chardet.detect(raw_data)
                encoding = encoding_result['encoding']
                logger.info(f"Detected encoding: {encoding} (confidence: {encoding_result['confidence']:.2f})")
        
        # Load CSV with specified parameters
        logger.info(f"Loading CSV from {path}...")
        df = pd.read_csv(
            path, 
            encoding=encoding,
            delimiter=delimiter,
            skiprows=skip_rows,
            **kwargs
        )
        
        # Data validation
        if validate_data:
            logger.info("Performing data validation...")
            _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error loading CSV file {path}: {str(e)}")
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


def load_csv_with_schema(path: str, 
                        schema: Dict[str, Any],
                        **kwargs) -> pd.DataFrame:
    """
    Load CSV with predefined schema for data type enforcement.
    
    Args:
        path (str): Path to the CSV file
        schema (dict): Dictionary mapping column names to data types
        **kwargs: Additional arguments passed to load_csv()
    
    Returns:
        pd.DataFrame: Loaded DataFrame with enforced schema
    """
    # Add dtype parameter to kwargs
    kwargs['dtype'] = schema
    
    return load_csv(path, **kwargs)


def load_csv_chunked(path: str, 
                    chunk_size: int = 10000,
                    **kwargs) -> pd.DataFrame:
    """
    Load large CSV files in chunks to manage memory usage.
    
    Args:
        path (str): Path to the CSV file
        chunk_size (int): Number of rows per chunk
        **kwargs: Additional arguments passed to pd.read_csv()
    
    Returns:
        pd.DataFrame: Combined DataFrame from all chunks
    """
    try:
        logger.info(f"Loading CSV in chunks of {chunk_size} rows...")
        
        chunks = []
        for chunk in pd.read_csv(path, chunksize=chunk_size, **kwargs):
            chunks.append(chunk)
            logger.info(f"Loaded chunk with {len(chunk)} rows")
        
        # Combine all chunks
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"Successfully combined {len(chunks)} chunks into {len(df)} total rows")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading CSV file in chunks {path}: {str(e)}")
        raise


# Example usage and testing
if __name__ == "__main__":
    # Example usage with sample data
    import numpy as np
    
    # Create sample CSV data for demonstration
    sample_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Product_{i}' for i in range(1, 101)],
        'price': np.random.uniform(10, 100, 100).round(2),
        'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 100),
        'rating': np.random.uniform(1, 5, 100).round(1)
    })
    
    # Save sample data
    sample_path = 'sample_data.csv'
    sample_data.to_csv(sample_path, index=False)
    print(f"Created sample CSV file: {sample_path}")
    
    try:
        # Test basic CSV loading
        print("\n=== Testing Basic CSV Loading ===")
        df = load_csv(sample_path, validate_data=True)
        print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst 5 rows:")
        print(df.head())
        
        # Test with schema
        print("\n=== Testing CSV Loading with Schema ===")
        schema = {
            'id': 'int64',
            'name': 'string',
            'price': 'float64',
            'category': 'category',
            'rating': 'float64'
        }
        df_schema = load_csv_with_schema(sample_path, schema)
        print(f"Data types with schema:")
        print(df_schema.dtypes)
        
        # Test chunked loading
        print("\n=== Testing Chunked CSV Loading ===")
        df_chunked = load_csv_chunked(sample_path, chunk_size=25)
        print(f"Loaded {len(df_chunked)} rows in chunks")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        # Clean up sample file
        if os.path.exists(sample_path):
            os.remove(sample_path)
            print(f"\nCleaned up sample file: {sample_path}")

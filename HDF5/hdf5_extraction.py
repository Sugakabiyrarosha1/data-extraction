"""
HDF5 Data Extraction Module

This module provides robust, production-ready functions for extracting data from HDF5 files
into Pandas DataFrames with advanced querying capabilities, chunking support, and comprehensive error handling.

Features:
- Hierarchical data format support for large-scale scientific data
- Advanced querying with PyTables where clauses
- Chunked loading for memory-efficient processing
- File structure analysis and inspection
- Compression and chunking support
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large datasets
- Support for both h5py and PyTables

Author: Data Extraction Toolkit
"""

import os
import pandas as pd
from typing import List, Optional, Dict, Any, Iterator
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import h5py and tables for HDF5 support
try:
    import h5py
    H5PY_AVAILABLE = True
except ImportError:
    H5PY_AVAILABLE = False
    logger.warning("h5py not available. Some features will be limited.")

try:
    import tables
    TABLES_AVAILABLE = True
except ImportError:
    TABLES_AVAILABLE = False
    logger.warning("PyTables not available. Some features will be limited.")


def load_hdf5(path: str,
              key: Optional[str] = None,
              columns: Optional[List[str]] = None,
              where: Optional[str] = None,
              start: Optional[int] = None,
              stop: Optional[int] = None,
              chunksize: int = 10000) -> pd.DataFrame:
    """
    Load HDF5 data with advanced querying capabilities.
    
    Args:
        path (str): Path to HDF5 file
        key (str, optional): HDF5 key/table name
        columns (list, optional): Specific columns to load
        where (str, optional): PyTables where clause for filtering
        start (int, optional): Start row index
        stop (int, optional): Stop row index
        chunksize (int): Chunk size for large datasets
    
    Returns:
        pd.DataFrame: Loaded HDF5 data
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"HDF5 file not found: {path}")
        
        logger.info(f"Loading HDF5 file: {path}")
        
        # Get file info if h5py is available
        if H5PY_AVAILABLE:
            with h5py.File(path, 'r') as f:
                logger.info(f"HDF5 file structure:")
                _print_hdf5_structure(f)
        
        # Load data using pandas
        df = pd.read_hdf(
            path,
            key=key,
            columns=columns,
            where=where,
            start=start,
            stop=stop,
            chunksize=chunksize
        )
        
        # Data validation
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} rows from HDF5")
        return df
        
    except Exception as e:
        logger.error(f"Error loading HDF5 file {path}: {str(e)}")
        raise


def _print_hdf5_structure(group, indent=0):
    """Print HDF5 file structure recursively."""
    if not H5PY_AVAILABLE:
        return
    
    for key in group.keys():
        item = group[key]
        spaces = "  " * indent
        if isinstance(item, h5py.Group):
            print(f"{spaces}{key}/ (Group)")
            _print_hdf5_structure(item, indent + 1)
        else:
            print(f"{spaces}{key} (Dataset: {item.shape}, {item.dtype})")


def get_hdf5_info(path: str) -> Dict[str, Any]:
    """
    Get information about HDF5 file structure.
    
    Args:
        path (str): Path to HDF5 file
    
    Returns:
        dict: HDF5 file information
    """
    if not H5PY_AVAILABLE:
        raise ImportError("h5py is required for HDF5 structure analysis")
    
    try:
        info = {
            'file_size_bytes': os.path.getsize(path),
            'groups': [],
            'datasets': []
        }
        
        with h5py.File(path, 'r') as f:
            def visit_func(name, obj):
                if isinstance(obj, h5py.Group):
                    info['groups'].append(name)
                elif isinstance(obj, h5py.Dataset):
                    info['datasets'].append({
                        'name': name,
                        'shape': obj.shape,
                        'dtype': str(obj.dtype),
                        'size': obj.size
                    })
            
            f.visititems(visit_func)
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting HDF5 info: {str(e)}")
        raise


def save_hdf5(df: pd.DataFrame,
              path: str,
              key: str = 'data',
              format: str = 'table',
              compression: str = 'blosc',
              complevel: int = 9,
              complib: str = 'blosc',
              **kwargs) -> None:
    """
    Save DataFrame to HDF5 format with optimization options.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): Output HDF5 file path
        key (str): HDF5 key/table name
        format (str): HDF5 format ('table' or 'fixed')
        compression (str): Compression algorithm
        complevel (int): Compression level
        complib (str): Compression library
        **kwargs: Additional HDF5 parameters
    """
    try:
        logger.info(f"Saving DataFrame to HDF5: {path}")
        
        df.to_hdf(
            path,
            key=key,
            format=format,
            compression=compression,
            complevel=complevel,
            complib=complib,
            **kwargs
        )
        
        file_size = os.path.getsize(path)
        logger.info(f"Successfully saved {len(df)} rows to HDF5 "
                   f"({file_size / 1024**2:.2f} MB)")
        
    except Exception as e:
        logger.error(f"Error saving HDF5 file: {str(e)}")
        raise


def load_hdf5_chunked(path: str,
                     key: str,
                     chunk_size: int = 10000,
                     columns: Optional[List[str]] = None) -> Iterator[pd.DataFrame]:
    """
    Load HDF5 data in chunks for memory-efficient processing.
    
    Args:
        path (str): Path to HDF5 file
        key (str): HDF5 key/table name
        chunk_size (int): Size of each chunk
        columns (list, optional): Specific columns to load
    
    Yields:
        pd.DataFrame: Data chunks
    """
    try:
        logger.info(f"Loading HDF5 file in chunks: {path}")
        
        with pd.HDFStore(path, 'r') as store:
            # Get total number of rows
            total_rows = store.get_storer(key).nrows
            
            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                
                chunk = store.select(
                    key,
                    start=start,
                    stop=end,
                    columns=columns
                )
                
                if not chunk.empty:
                    yield chunk
                    
    except Exception as e:
        logger.error(f"Error loading HDF5 chunks: {str(e)}")
        raise


def get_hdf5_keys(path: str) -> List[str]:
    """
    Get all available keys in an HDF5 file.
    
    Args:
        path (str): Path to HDF5 file
    
    Returns:
        list: List of available keys
    """
    try:
        with pd.HDFStore(path, 'r') as store:
            return list(store.keys())
    except Exception as e:
        logger.error(f"Error getting HDF5 keys: {str(e)}")
        raise


def load_hdf5_with_query(path: str,
                        key: str,
                        query: str,
                        columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Load HDF5 data with PyTables query.
    
    Args:
        path (str): Path to HDF5 file
        key (str): HDF5 key/table name
        query (str): PyTables where clause
        columns (list, optional): Specific columns to load
    
    Returns:
        pd.DataFrame: Filtered data
    """
    try:
        logger.info(f"Loading HDF5 data with query: {query}")
        
        with pd.HDFStore(path, 'r') as store:
            df = store.select(key, where=query, columns=columns)
        
        # Data validation
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} rows with query")
        return df
        
    except Exception as e:
        logger.error(f"Error loading HDF5 data with query: {str(e)}")
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
    print("Testing HDF5 data extraction...")
    
    # Create sample data
    import numpy as np
    
    sample_data = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'Item_{i}' for i in range(1, 1001)],
        'category': np.random.choice(['A', 'B', 'C', 'D'], 1000),
        'value': np.random.uniform(0, 100, 1000),
        'timestamp': pd.date_range('2024-01-01', periods=1000, freq='H'),
        'active': np.random.choice([True, False], 1000)
    })
    
    # Save as HDF5
    hdf5_path = 'sample_data.h5'
    save_hdf5(sample_data, hdf5_path, key='data', compression='blosc')
    print(f"Created sample HDF5 file: {hdf5_path}")
    
    try:
        # Test 1: Load full HDF5 file
        print("\n=== Testing Full HDF5 Loading ===")
        df_hdf5 = load_hdf5(hdf5_path, key='data')
        print(f"Shape: {df_hdf5.shape}")
        print("Columns:", list(df_hdf5.columns))
        print("\nFirst 3 rows:")
        print(df_hdf5.head(3))
        
        # Test 2: Load specific columns
        print("\n=== Testing Column Selection ===")
        df_hdf5_cols = load_hdf5(hdf5_path, key='data', columns=['id', 'name', 'value'])
        print(f"Shape: {df_hdf5_cols.shape}")
        print("\nFirst 3 rows:")
        print(df_hdf5_cols.head(3))
        
        # Test 3: Load with query
        print("\n=== Testing HDF5 Query ===")
        df_hdf5_query = load_hdf5_with_query(
            hdf5_path, 
            key='data', 
            query='category == "A" and value > 50'
        )
        print(f"Query result shape: {df_hdf5_query.shape}")
        print("\nFirst 3 rows:")
        print(df_hdf5_query.head(3))
        
        # Test 4: Get HDF5 info
        if H5PY_AVAILABLE:
            print("\n=== Testing HDF5 Info ===")
            info = get_hdf5_info(hdf5_path)
            print(f"File size: {info['file_size_bytes'] / 1024**2:.2f} MB")
            print(f"Groups: {info['groups']}")
            print(f"Datasets: {len(info['datasets'])}")
        
        # Test 5: Get available keys
        print("\n=== Testing HDF5 Keys ===")
        keys = get_hdf5_keys(hdf5_path)
        print(f"Available keys: {keys}")
        
        # Test 6: Chunked loading
        print("\n=== Testing Chunked Loading ===")
        chunk_count = 0
        total_rows = 0
        for chunk in load_hdf5_chunked(hdf5_path, key='data', chunk_size=500):
            chunk_count += 1
            total_rows += len(chunk)
            if chunk_count <= 2:  # Show first 2 chunks
                print(f"Chunk {chunk_count} shape: {chunk.shape}")
        
        print(f"Total chunks: {chunk_count}, Total rows: {total_rows}")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        # Clean up sample file
        if os.path.exists(hdf5_path):
            os.remove(hdf5_path)
            print(f"\nCleaned up sample file: {hdf5_path}")
    
    print("\nHDF5 extraction testing completed!")

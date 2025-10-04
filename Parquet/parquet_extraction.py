"""
Parquet Data Extraction Module

This module provides robust, production-ready functions for extracting data from Parquet files
into Pandas DataFrames with advanced filtering, column selection, and metadata analysis.

Features:
- High-performance columnar data format support
- Advanced filtering and column selection
- Partitioned data loading
- Metadata analysis and inspection
- Compression and optimization options
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large datasets
- Support for multiple Parquet engines

Author: Data Extraction Toolkit
"""

import os
import pandas as pd
from typing import List, Optional, Dict, Any
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import PyArrow for better performance
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logger.warning("PyArrow not available. Using pandas default engine.")


def load_parquet(path: str,
                 columns: Optional[List[str]] = None,
                 filters: Optional[List] = None,
                 use_pandas_metadata: bool = True,
                 engine: str = 'pyarrow') -> pd.DataFrame:
    """
    Load Parquet data with advanced filtering and column selection.
    
    Args:
        path (str): Path to the Parquet file
        columns (list, optional): Specific columns to load
        filters (list, optional): PyArrow filters for row filtering
        use_pandas_metadata (bool): Whether to use pandas metadata
        engine (str): Engine to use ('pyarrow' or 'fastparquet')
    
    Returns:
        pd.DataFrame: Loaded Parquet data
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Parquet file not found: {path}")
        
        logger.info(f"Loading Parquet file: {path}")
        
        # Get file metadata if PyArrow is available
        if PYARROW_AVAILABLE and engine == 'pyarrow':
            parquet_file = pq.ParquetFile(path)
            metadata = parquet_file.metadata
            
            logger.info(f"Parquet file info:")
            logger.info(f"  Rows: {metadata.num_rows}")
            logger.info(f"  Columns: {metadata.num_columns}")
            logger.info(f"  Row groups: {metadata.num_row_groups}")
        
        # Load data
        df = pd.read_parquet(
            path,
            columns=columns,
            filters=filters,
            engine=engine
        )
        
        # Data validation
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} rows from Parquet")
        return df
        
    except Exception as e:
        logger.error(f"Error loading Parquet file {path}: {str(e)}")
        raise


def get_parquet_metadata(path: str) -> Dict[str, Any]:
    """
    Get detailed metadata about a Parquet file.
    
    Args:
        path (str): Path to Parquet file
    
    Returns:
        dict: Parquet file metadata
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("PyArrow is required for metadata analysis")
    
    try:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        schema = parquet_file.schema
        
        info = {
            'num_rows': metadata.num_rows,
            'num_columns': metadata.num_columns,
            'num_row_groups': metadata.num_row_groups,
            'file_size_bytes': os.path.getsize(path),
            'columns': [field.name for field in schema],
            'column_types': {field.name: str(field.type) for field in schema}
        }
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting Parquet metadata: {str(e)}")
        raise


def load_parquet_partitioned(base_path: str,
                           partition_columns: Optional[List[str]] = None,
                           filters: Optional[List] = None) -> pd.DataFrame:
    """
    Load partitioned Parquet data from a directory.
    
    Args:
        base_path (str): Base directory containing partitioned data
        partition_columns (list, optional): Partition column names
        filters (list, optional): Filters for partition pruning
    
    Returns:
        pd.DataFrame: Combined partitioned data
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("PyArrow is required for partitioned data loading")
    
    try:
        logger.info(f"Loading partitioned Parquet data from: {base_path}")
        
        # Use PyArrow dataset for partitioned data
        dataset = pq.ParquetDataset(
            base_path,
            filters=filters
        )
        
        df = dataset.read_pandas().to_pandas()
        
        # Data validation
        _validate_dataframe(df, f"Partitioned Parquet: {base_path}")
        
        logger.info(f"Successfully loaded {len(df)} rows from partitioned Parquet")
        return df
        
    except Exception as e:
        logger.error(f"Error loading partitioned Parquet: {str(e)}")
        raise


def save_parquet(df: pd.DataFrame, 
                 path: str,
                 compression: str = 'snappy',
                 index: bool = False,
                 partition_cols: Optional[List[str]] = None) -> None:
    """
    Save DataFrame to Parquet format with optimization options.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        path (str): Output path
        compression (str): Compression algorithm
        index (bool): Whether to include index
        partition_cols (list, optional): Columns to partition by
    """
    try:
        logger.info(f"Saving DataFrame to Parquet: {path}")
        
        df.to_parquet(
            path,
            compression=compression,
            index=index,
            partition_cols=partition_cols
        )
        
        file_size = os.path.getsize(path)
        logger.info(f"Successfully saved {len(df)} rows to Parquet ({file_size / 1024**2:.2f} MB)")
        
    except Exception as e:
        logger.error(f"Error saving Parquet file: {str(e)}")
        raise


def load_parquet_chunked(path: str,
                        chunk_size: int = 10000,
                        columns: Optional[List[str]] = None,
                        **kwargs) -> pd.DataFrame:
    """
    Load large Parquet files in chunks to manage memory usage.
    
    Args:
        path (str): Path to Parquet file
        chunk_size (int): Number of rows per chunk
        columns (list, optional): Specific columns to load
        **kwargs: Additional arguments passed to load_parquet()
    
    Returns:
        pd.DataFrame: Combined DataFrame from all chunks
    """
    try:
        logger.info(f"Loading Parquet in chunks of {chunk_size} rows...")
        
        chunks = []
        for chunk in pd.read_parquet(path, chunksize=chunk_size, columns=columns, **kwargs):
            chunks.append(chunk)
            logger.info(f"Loaded chunk with {len(chunk)} rows")
        
        # Combine all chunks
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"Successfully combined {len(chunks)} chunks into {len(df)} total rows")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading Parquet file in chunks {path}: {str(e)}")
        raise


def optimize_parquet(path: str, 
                    output_path: Optional[str] = None,
                    compression: str = 'snappy',
                    row_group_size: int = 100000) -> str:
    """
    Optimize Parquet file for better performance.
    
    Args:
        path (str): Path to input Parquet file
        output_path (str, optional): Path for optimized file
        compression (str): Compression algorithm
        row_group_size (int): Target row group size
    
    Returns:
        str: Path to optimized file
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("PyArrow is required for Parquet optimization")
    
    try:
        if output_path is None:
            output_path = path.replace('.parquet', '_optimized.parquet')
        
        logger.info(f"Optimizing Parquet file: {path}")
        
        # Read the original file
        table = pq.read_table(path)
        
        # Write optimized version
        pq.write_table(
            table,
            output_path,
            compression=compression,
            row_group_size=row_group_size
        )
        
        original_size = os.path.getsize(path)
        optimized_size = os.path.getsize(output_path)
        compression_ratio = (1 - optimized_size / original_size) * 100
        
        logger.info(f"Optimization complete:")
        logger.info(f"  Original size: {original_size / 1024**2:.2f} MB")
        logger.info(f"  Optimized size: {optimized_size / 1024**2:.2f} MB")
        logger.info(f"  Compression ratio: {compression_ratio:.1f}%")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error optimizing Parquet file: {str(e)}")
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
    print("Testing Parquet data extraction...")
    
    # Create sample data and save as Parquet
    import numpy as np
    
    sample_data = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'Product_{i}' for i in range(1, 1001)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 1000),
        'price': np.random.uniform(10, 1000, 1000).round(2),
        'rating': np.random.uniform(1, 5, 1000).round(1),
        'created_date': pd.date_range('2024-01-01', periods=1000, freq='H'),
        'is_active': np.random.choice([True, False], 1000)
    })
    
    # Save as Parquet
    parquet_path = 'sample_data.parquet'
    save_parquet(sample_data, parquet_path, compression='snappy')
    print(f"Created sample Parquet file: {parquet_path}")
    
    try:
        # Test 1: Load full Parquet file
        print("\n=== Testing Full Parquet Loading ===")
        df_parquet = load_parquet(parquet_path)
        print(f"Shape: {df_parquet.shape}")
        print("Columns:", list(df_parquet.columns))
        print("\nFirst 3 rows:")
        print(df_parquet.head(3))
        
        # Test 2: Load specific columns only
        print("\n=== Testing Column Selection ===")
        df_parquet_cols = load_parquet(parquet_path, columns=['id', 'name', 'price', 'category'])
        print(f"Shape: {df_parquet_cols.shape}")
        print("\nFirst 3 rows:")
        print(df_parquet_cols.head(3))
        
        # Test 3: Get Parquet metadata
        if PYARROW_AVAILABLE:
            print("\n=== Testing Parquet Metadata ===")
            metadata = get_parquet_metadata(parquet_path)
            for key, value in metadata.items():
                print(f"  {key}: {value}")
        
        # Test 4: Load with filters (if supported)
        print("\n=== Testing Parquet Filtering ===")
        try:
            # Filter for Electronics category only
            filters = [('category', '=', 'Electronics')]
            df_filtered = load_parquet(parquet_path, filters=filters)
            print(f"Filtered shape: {df_filtered.shape}")
            print("Category distribution:")
            print(df_filtered['category'].value_counts())
        except Exception as e:
            print(f"Filtering not supported or error: {e}")
            # Fallback to pandas filtering
            df_filtered = df_parquet[df_parquet['category'] == 'Electronics']
            print(f"Pandas filtered shape: {df_filtered.shape}")
        
        # Test 5: Chunked loading
        print("\n=== Testing Chunked Loading ===")
        df_chunked = load_parquet_chunked(parquet_path, chunk_size=500)
        print(f"Chunked loading result shape: {df_chunked.shape}")
        
        # Test 6: Optimization
        if PYARROW_AVAILABLE:
            print("\n=== Testing Parquet Optimization ===")
            optimized_path = optimize_parquet(parquet_path, compression='gzip')
            print(f"Optimized file created: {optimized_path}")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        # Clean up sample files
        for file_path in [parquet_path, 'sample_data_optimized.parquet']:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Cleaned up sample file: {file_path}")
    
    print("\nParquet extraction testing completed!")

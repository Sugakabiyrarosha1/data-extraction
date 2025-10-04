# Parquet Data Extraction

This folder contains robust, production-ready functions for extracting data from Parquet files into Pandas DataFrames with advanced filtering, column selection, and metadata analysis.

## Features

- **High-Performance Columnar Format**: Efficient storage and retrieval of structured data
- **Advanced Filtering**: PyArrow filters for row-level filtering
- **Column Selection**: Load only specific columns to reduce memory usage
- **Partitioned Data Support**: Load data from partitioned Parquet directories
- **Metadata Analysis**: Detailed file and schema information
- **Compression Support**: Multiple compression algorithms (snappy, gzip, brotli, etc.)
- **Optimization Tools**: File optimization for better performance
- **Chunked Loading**: Memory-efficient processing for large files
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation

## Files

- `parquet_extraction.py`: Main module containing all Parquet extraction functions

## Functions

### `load_parquet(path, columns=None, filters=None, use_pandas_metadata=True, engine='pyarrow')`

Main function for loading Parquet files with advanced filtering and column selection.

**Parameters:**
- `path` (str): Path to the Parquet file
- `columns` (list, optional): Specific columns to load
- `filters` (list, optional): PyArrow filters for row filtering
- `use_pandas_metadata` (bool): Whether to use pandas metadata
- `engine` (str): Engine to use ('pyarrow' or 'fastparquet')

**Returns:**
- `pd.DataFrame`: Loaded Parquet data

### `get_parquet_metadata(path)`

Get detailed metadata about a Parquet file.

**Parameters:**
- `path` (str): Path to Parquet file

**Returns:**
- `dict`: Parquet file metadata including:
  - `num_rows`: Number of rows
  - `num_columns`: Number of columns
  - `num_row_groups`: Number of row groups
  - `file_size_bytes`: File size in bytes
  - `columns`: List of column names
  - `column_types`: Column data types

### `load_parquet_partitioned(base_path, partition_columns=None, filters=None)`

Load partitioned Parquet data from a directory.

**Parameters:**
- `base_path` (str): Base directory containing partitioned data
- `partition_columns` (list, optional): Partition column names
- `filters` (list, optional): Filters for partition pruning

**Returns:**
- `pd.DataFrame`: Combined partitioned data

### `save_parquet(df, path, compression='snappy', index=False, partition_cols=None)`

Save DataFrame to Parquet format with optimization options.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to save
- `path` (str): Output path
- `compression` (str): Compression algorithm
- `index` (bool): Whether to include index
- `partition_cols` (list, optional): Columns to partition by

### `load_parquet_chunked(path, chunk_size=10000, columns=None, **kwargs)`

Load large Parquet files in chunks to manage memory usage.

**Parameters:**
- `path` (str): Path to Parquet file
- `chunk_size` (int): Number of rows per chunk
- `columns` (list, optional): Specific columns to load
- `**kwargs`: Additional arguments passed to `load_parquet()`

**Returns:**
- `pd.DataFrame`: Combined DataFrame from all chunks

### `optimize_parquet(path, output_path=None, compression='snappy', row_group_size=100000)`

Optimize Parquet file for better performance.

**Parameters:**
- `path` (str): Path to input Parquet file
- `output_path` (str, optional): Path for optimized file
- `compression` (str): Compression algorithm
- `row_group_size` (int): Target row group size

**Returns:**
- `str`: Path to optimized file

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic Parquet Loading

```python
from parquet_extraction import load_parquet

# Load Parquet file
df = load_parquet('data.parquet')
print(f"Loaded {len(df)} rows")
```

### Loading with Column Selection

```python
# Load only specific columns
columns = ['id', 'name', 'price', 'category']
df = load_parquet('data.parquet', columns=columns)
print(f"Columns: {list(df.columns)}")
```

### Loading with Filters

```python
# Load with PyArrow filters
filters = [
    ('category', '=', 'Electronics'),
    ('price', '>', 100)
]
df = load_parquet('data.parquet', filters=filters)
print(f"Filtered rows: {len(df)}")
```

### Getting Metadata

```python
from parquet_extraction import get_parquet_metadata

# Get file metadata
metadata = get_parquet_metadata('data.parquet')
print(f"Rows: {metadata['num_rows']}")
print(f"Columns: {metadata['columns']}")
print(f"File size: {metadata['file_size_bytes'] / 1024**2:.2f} MB")
```

### Loading Partitioned Data

```python
from parquet_extraction import load_parquet_partitioned

# Load partitioned data
df = load_parquet_partitioned('partitioned_data/', filters=[('year', '=', 2024)])
print(f"Loaded {len(df)} rows from partitioned data")
```

### Saving to Parquet

```python
from parquet_extraction import save_parquet

# Save DataFrame to Parquet
save_parquet(df, 'output.parquet', compression='snappy')
```

### Chunked Loading

```python
from parquet_extraction import load_parquet_chunked

# Load large file in chunks
df = load_parquet_chunked('large_data.parquet', chunk_size=5000)
print(f"Loaded {len(df)} rows in chunks")
```

### Optimizing Parquet Files

```python
from parquet_extraction import optimize_parquet

# Optimize Parquet file
optimized_path = optimize_parquet('data.parquet', compression='gzip')
print(f"Optimized file: {optimized_path}")
```

## Dependencies

- `pandas`: For DataFrame operations
- `pyarrow`: For high-performance Parquet operations (recommended)
- `fastparquet`: Alternative Parquet engine
- `numpy`: For numerical operations (used in examples)

## Compression Algorithms

Supported compression algorithms:

- `snappy`: Fast compression, good balance of speed and size
- `gzip`: Higher compression ratio, slower
- `brotli`: Very high compression ratio, slower
- `lz4`: Very fast compression, larger files
- `zstd`: Good balance of speed and compression

## Filtering Examples

### Simple Filters
```python
# Single condition
filters = [('category', '=', 'Electronics')]

# Multiple conditions
filters = [
    ('category', '=', 'Electronics'),
    ('price', '>', 100),
    ('rating', '>=', 4.0)
]
```

### Complex Filters
```python
# Date range
filters = [
    ('created_date', '>=', '2024-01-01'),
    ('created_date', '<', '2024-02-01')
]

# String operations
filters = [
    ('name', 'like', 'Product%'),
    ('description', 'in', ['High quality', 'Premium'])
]
```

## Partitioning Examples

### Directory Structure
```
data/
├── year=2023/
│   ├── month=01/
│   │   ├── part-00000.parquet
│   │   └── part-00001.parquet
│   └── month=02/
│       └── part-00000.parquet
└── year=2024/
    └── month=01/
        └── part-00000.parquet
```

### Loading Partitioned Data
```python
# Load specific partitions
filters = [
    ('year', '=', 2024),
    ('month', 'in', [1, 2, 3])
]
df = load_parquet_partitioned('data/', filters=filters)
```

## Performance Optimization

### Row Group Size
- **Small row groups** (10K-100K rows): Better for filtering, more metadata
- **Large row groups** (1M+ rows): Better compression, less metadata

### Column Selection
```python
# Load only needed columns
columns = ['id', 'name', 'price']  # Instead of all columns
df = load_parquet('data.parquet', columns=columns)
```

### Compression
```python
# Choose compression based on use case
save_parquet(df, 'data.parquet', compression='snappy')  # Fast I/O
save_parquet(df, 'data.parquet', compression='gzip')    # Storage efficiency
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when Parquet file doesn't exist
2. **Invalid Format**: Handles corrupted or invalid Parquet files
3. **Memory Management**: Chunked loading for large files
4. **Data Validation**: Checks for empty DataFrames and malformed data
5. **Engine Compatibility**: Handles different Parquet engines

## Best Practices

1. **Use PyArrow** for better performance and features
2. **Select specific columns** to reduce memory usage
3. **Use filters** to reduce data transfer
4. **Optimize row group size** based on your use case
5. **Choose appropriate compression** for your needs
6. **Use partitioning** for large datasets
7. **Monitor memory usage** for large files

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python parquet_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

## Supported File Formats

- `.parquet`: Standard Parquet format
- `.parq`: Alternative Parquet extension
- Partitioned directories with Parquet files

## Common Use Cases

### Data Warehousing
```python
# Load large datasets efficiently
df = load_parquet('warehouse_data.parquet', 
                  columns=['customer_id', 'order_date', 'amount'],
                  filters=[('order_date', '>=', '2024-01-01')])
```

### Analytics
```python
# Load specific metrics
metrics = ['revenue', 'profit', 'cost']
df = load_parquet('analytics.parquet', columns=metrics)
```

### Machine Learning
```python
# Load training data
df = load_parquet('training_data.parquet',
                  filters=[('split', '=', 'train')])
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Use chunked loading or column selection
2. **Slow Loading**: Check row group size and compression
3. **Filter Issues**: Verify filter syntax and column names
4. **Engine Errors**: Try different engines (pyarrow vs fastparquet)

### Performance Tips

1. **Use PyArrow** for better performance
2. **Optimize row group size** for your data
3. **Use appropriate compression** for your use case
4. **Partition large datasets** by common filter columns
5. **Select only needed columns** to reduce I/O

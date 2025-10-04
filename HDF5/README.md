# HDF5 Data Extraction

This folder contains robust, production-ready functions for extracting data from HDF5 files into Pandas DataFrames with advanced querying capabilities, chunking support, and comprehensive error handling.

## Features

- **Hierarchical Data Format**: Support for large-scale scientific data storage
- **Advanced Querying**: PyTables where clauses for complex filtering
- **Chunked Loading**: Memory-efficient processing for large datasets
- **File Structure Analysis**: Inspect HDF5 file organization
- **Compression Support**: Multiple compression algorithms (blosc, gzip, lzf, etc.)
- **Multiple Storage Formats**: Table and fixed format support
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Memory Management**: Efficient processing for large datasets
- **Key Management**: List and manage HDF5 keys/tables

## Files

- `hdf5_extraction.py`: Main module containing all HDF5 extraction functions

## Functions

### `load_hdf5(path, key=None, columns=None, where=None, start=None, stop=None, chunksize=10000)`

Main function for loading HDF5 data with advanced querying capabilities.

**Parameters:**
- `path` (str): Path to HDF5 file
- `key` (str, optional): HDF5 key/table name
- `columns` (list, optional): Specific columns to load
- `where` (str, optional): PyTables where clause for filtering
- `start` (int, optional): Start row index
- `stop` (int, optional): Stop row index
- `chunksize` (int): Chunk size for large datasets

**Returns:**
- `pd.DataFrame`: Loaded HDF5 data

### `get_hdf5_info(path)`

Get information about HDF5 file structure.

**Parameters:**
- `path` (str): Path to HDF5 file

**Returns:**
- `dict`: HDF5 file information including:
  - `file_size_bytes`: File size in bytes
  - `groups`: List of group names
  - `datasets`: List of dataset information

### `save_hdf5(df, path, key='data', format='table', compression='blosc', complevel=9, complib='blosc', **kwargs)`

Save DataFrame to HDF5 format with optimization options.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to save
- `path` (str): Output HDF5 file path
- `key` (str): HDF5 key/table name
- `format` (str): HDF5 format ('table' or 'fixed')
- `compression` (str): Compression algorithm
- `complevel` (int): Compression level
- `complib` (str): Compression library
- `**kwargs`: Additional HDF5 parameters

### `load_hdf5_chunked(path, key, chunk_size=10000, columns=None)`

Load HDF5 data in chunks for memory-efficient processing.

**Parameters:**
- `path` (str): Path to HDF5 file
- `key` (str): HDF5 key/table name
- `chunk_size` (int): Size of each chunk
- `columns` (list, optional): Specific columns to load

**Returns:**
- `Iterator[pd.DataFrame]`: Data chunks

### `get_hdf5_keys(path)`

Get all available keys in an HDF5 file.

**Parameters:**
- `path` (str): Path to HDF5 file

**Returns:**
- `list`: List of available keys

### `load_hdf5_with_query(path, key, query, columns=None)`

Load HDF5 data with PyTables query.

**Parameters:**
- `path` (str): Path to HDF5 file
- `key` (str): HDF5 key/table name
- `query` (str): PyTables where clause
- `columns` (list, optional): Specific columns to load

**Returns:**
- `pd.DataFrame`: Filtered data

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic HDF5 Loading

```python
from hdf5_extraction import load_hdf5

# Load HDF5 file
df = load_hdf5('data.h5', key='table_name')
print(f"Loaded {len(df)} rows")
```

### Loading with Column Selection

```python
# Load only specific columns
columns = ['id', 'name', 'value']
df = load_hdf5('data.h5', key='table_name', columns=columns)
print(f"Columns: {list(df.columns)}")
```

### Loading with Query

```python
# Load with PyTables where clause
query = "category == 'A' and value > 100"
df = load_hdf5('data.h5', key='table_name', where=query)
print(f"Filtered rows: {len(df)}")
```

### Getting HDF5 Information

```python
from hdf5_extraction import get_hdf5_info

# Get file structure information
info = get_hdf5_info('data.h5')
print(f"File size: {info['file_size_bytes'] / 1024**2:.2f} MB")
print(f"Datasets: {len(info['datasets'])}")
```

### Getting Available Keys

```python
from hdf5_extraction import get_hdf5_keys

# Get all available keys
keys = get_hdf5_keys('data.h5')
print(f"Available keys: {keys}")
```

### Chunked Loading

```python
from hdf5_extraction import load_hdf5_chunked

# Load large file in chunks
for chunk in load_hdf5_chunked('data.h5', key='table_name', chunk_size=5000):
    print(f"Processing chunk with {len(chunk)} rows")
    # Process chunk here
```

### Saving to HDF5

```python
from hdf5_extraction import save_hdf5

# Save DataFrame to HDF5
save_hdf5(df, 'output.h5', key='data', compression='blosc')
```

### Loading with Query Function

```python
from hdf5_extraction import load_hdf5_with_query

# Load with complex query
df = load_hdf5_with_query(
    'data.h5', 
    key='table_name', 
    query='category == "A" and value > 50 and active == True'
)
```

## Dependencies

- `pandas`: For DataFrame operations
- `h5py`: For HDF5 file operations (recommended)
- `tables`: For PyTables support (recommended)
- `numpy`: For numerical operations (used in examples)

## HDF5 Storage Formats

### Table Format
- **Advantages**: Supports queries, appending, and complex data types
- **Use Case**: When you need to query or append data
- **Example**: `format='table'`

### Fixed Format
- **Advantages**: Faster read/write, smaller file size
- **Use Case**: When you need maximum performance
- **Example**: `format='fixed'`

## Compression Options

Supported compression algorithms:

- `blosc`: Fast compression, good balance (recommended)
- `gzip`: Higher compression ratio, slower
- `lzf`: Very fast compression, larger files
- `zlib`: Standard compression
- `bzip2`: High compression ratio, slower

## Query Examples

### Simple Queries
```python
# Single condition
query = "category == 'A'"

# Multiple conditions
query = "category == 'A' and value > 100"

# String operations
query = "name.startswith('Product')"
```

### Complex Queries
```python
# Date range
query = "timestamp >= '2024-01-01' and timestamp < '2024-02-01'"

# Numeric operations
query = "value > 100 and value < 1000"

# Boolean operations
query = "active == True and category in ['A', 'B']"
```

### Advanced Queries
```python
# Mathematical operations
query = "value * 2 > 200"

# String functions
query = "name.upper().contains('PRODUCT')"

# Complex conditions
query = "(category == 'A' and value > 100) or (category == 'B' and value < 50)"
```

## Performance Optimization

### Compression Settings
```python
# High compression
save_hdf5(df, 'data.h5', compression='gzip', complevel=9)

# Fast compression
save_hdf5(df, 'data.h5', compression='blosc', complevel=1)

# Balanced
save_hdf5(df, 'data.h5', compression='blosc', complevel=6)
```

### Chunk Size
```python
# Small chunks for memory efficiency
chunk_size = 1000

# Large chunks for performance
chunk_size = 100000
```

### Format Selection
```python
# Use table format for querying
save_hdf5(df, 'data.h5', format='table')

# Use fixed format for performance
save_hdf5(df, 'data.h5', format='fixed')
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when HDF5 file doesn't exist
2. **Invalid Format**: Handles corrupted or invalid HDF5 files
3. **Memory Management**: Chunked loading for large files
4. **Data Validation**: Checks for empty DataFrames and malformed data
5. **Query Errors**: Handles invalid PyTables queries
6. **Key Errors**: Handles missing or invalid HDF5 keys

## Best Practices

1. **Use appropriate compression** based on your needs
2. **Choose the right format** (table vs fixed)
3. **Use chunked loading** for large files
4. **Optimize chunk size** for your use case
5. **Use queries** to filter data at the HDF5 level
6. **Monitor memory usage** for large datasets
7. **Validate data** before processing

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python hdf5_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

## Supported File Formats

- `.h5`: Standard HDF5 format
- `.hdf5`: Alternative HDF5 extension
- `.hdf`: Legacy HDF format

## Common Use Cases

### Scientific Data
```python
# Load large scientific datasets
df = load_hdf5('experiment_data.h5', key='measurements')
```

### Time Series Data
```python
# Load time series with date filtering
df = load_hdf5('timeseries.h5', key='data', 
               where="timestamp >= '2024-01-01'")
```

### Machine Learning
```python
# Load training data in chunks
for chunk in load_hdf5_chunked('training_data.h5', key='features'):
    # Process chunk for training
    pass
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Use chunked loading or column selection
2. **Slow Loading**: Check compression and format settings
3. **Query Errors**: Verify PyTables query syntax
4. **Key Errors**: Check available keys with `get_hdf5_keys()`

### Performance Tips

1. **Use blosc compression** for best balance
2. **Use table format** for querying
3. **Optimize chunk size** for your data
4. **Use column selection** to reduce I/O
5. **Use queries** to filter at the HDF5 level

## HDF5 vs Other Formats

### Advantages
- **Hierarchical structure**: Organize data in groups
- **Compression**: Built-in compression support
- **Cross-platform**: Works across different systems
- **Large datasets**: Handle very large files efficiently
- **Querying**: Built-in query capabilities

### When to Use
- Large scientific datasets
- Time series data
- Data that needs compression
- Data that needs querying
- Cross-platform data sharing

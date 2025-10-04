# CSV Data Extraction

This folder contains robust, production-ready functions for extracting data from CSV files into Pandas DataFrames.

## Features

- **Automatic Encoding Detection**: Uses `chardet` library to automatically detect file encoding
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Flexible Parsing**: Support for custom delimiters, skip rows, and other pandas options
- **Memory Management**: Chunked loading for large files
- **Schema Enforcement**: Predefined data type enforcement
- **Logging**: Detailed logging for debugging and monitoring

## Files

- `csv_extraction.py`: Main module containing all CSV extraction functions

## Functions

### `load_csv(path, encoding=None, delimiter=',', skip_rows=0, validate_data=True, **kwargs)`

Main function for loading CSV files with advanced features.

**Parameters:**
- `path` (str): Path to the CSV file
- `encoding` (str, optional): File encoding. Auto-detected if None
- `delimiter` (str): CSV delimiter (default: ',')
- `skip_rows` (int): Number of rows to skip from the beginning
- `validate_data` (bool): Whether to perform data validation
- `**kwargs`: Additional arguments passed to `pd.read_csv()`

**Returns:**
- `pd.DataFrame`: Loaded and validated DataFrame

**Raises:**
- `FileNotFoundError`: If the file doesn't exist
- `ValueError`: If data validation fails

### `load_csv_with_schema(path, schema, **kwargs)`

Load CSV with predefined schema for data type enforcement.

**Parameters:**
- `path` (str): Path to the CSV file
- `schema` (dict): Dictionary mapping column names to data types
- `**kwargs`: Additional arguments passed to `load_csv()`

**Returns:**
- `pd.DataFrame`: Loaded DataFrame with enforced schema

### `load_csv_chunked(path, chunk_size=10000, **kwargs)`

Load large CSV files in chunks to manage memory usage.

**Parameters:**
- `path` (str): Path to the CSV file
- `chunk_size` (int): Number of rows per chunk
- `**kwargs`: Additional arguments passed to `pd.read_csv()`

**Returns:**
- `pd.DataFrame`: Combined DataFrame from all chunks

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

**Validations:**
- Checks for empty DataFrames
- Identifies completely empty columns
- Detects duplicate column names
- Logs basic statistics and memory usage

## Usage Examples

### Basic CSV Loading

```python
from csv_extraction import load_csv

# Load CSV with automatic encoding detection
df = load_csv('data.csv', validate_data=True)
print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
```

### Loading with Custom Parameters

```python
# Load CSV with custom delimiter and skip rows
df = load_csv('data.csv', delimiter=';', skip_rows=2, encoding='utf-8')
```

### Loading with Schema

```python
# Define schema for data type enforcement
schema = {
    'id': 'int64',
    'name': 'string',
    'price': 'float64',
    'category': 'category'
}

df = load_csv_with_schema('data.csv', schema)
```

### Chunked Loading for Large Files

```python
# Load large CSV file in chunks
df = load_csv_chunked('large_data.csv', chunk_size=5000)
```

## Dependencies

- `pandas`: For DataFrame operations
- `chardet`: For automatic encoding detection
- `numpy`: For numerical operations (used in examples)

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when CSV file doesn't exist
2. **Encoding Issues**: Automatic detection and fallback options
3. **Data Validation**: Checks for empty DataFrames, empty columns, and duplicate column names
4. **Memory Management**: Chunked loading prevents memory overflow for large files

## Logging

The module uses Python's logging system to provide detailed information about:

- Encoding detection results
- File loading progress
- Data validation results
- Memory usage statistics
- Error messages and warnings

## Best Practices

1. **Always validate data** when loading from unknown sources
2. **Use chunked loading** for files larger than available memory
3. **Specify schema** when you know the expected data types
4. **Handle encoding issues** by letting the module auto-detect or specifying known encodings
5. **Monitor memory usage** for large datasets

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python csv_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

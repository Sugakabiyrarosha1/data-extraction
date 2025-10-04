# JSON Data Extraction

This folder contains robust, production-ready functions for extracting data from JSON files into Pandas DataFrames with support for nested structures, arrays, and various JSON formats.

## Features

- **JSON File Processing**: Load data from standard JSON files
- **Nested Structure Support**: Flatten complex nested JSON structures
- **Array Handling**: Multiple strategies for handling arrays (expand, join, first, last)
- **Streaming Parser**: Memory-efficient processing for large JSON files
- **JSONL Support**: Load JSON Lines format files
- **Schema Enforcement**: Predefined data type enforcement
- **Export Functionality**: Export DataFrames back to JSON format
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Memory Management**: Efficient processing for large datasets

## Files

- `json_extraction.py`: Main module containing all JSON extraction functions

## Functions

### `load_json(path, encoding='utf-8', flatten_nested=True, handle_arrays='expand', max_depth=10)`

Main function for loading JSON files with advanced processing capabilities.

**Parameters:**
- `path` (str): Path to the JSON file
- `encoding` (str): File encoding (default: 'utf-8')
- `flatten_nested` (bool): Whether to flatten nested structures
- `handle_arrays` (str): How to handle arrays:
  - `'expand'`: Expand arrays into separate rows
  - `'join'`: Join array elements with separator
  - `'first'`: Take first element only
  - `'last'`: Take last element only
- `max_depth` (int): Maximum depth for flattening nested structures

**Returns:**
- `pd.DataFrame`: Processed JSON data

### `load_jsonl(path, encoding='utf-8')`

Load JSON Lines (JSONL) file where each line is a separate JSON object.

**Parameters:**
- `path` (str): Path to JSONL file
- `encoding` (str): File encoding

**Returns:**
- `pd.DataFrame`: Loaded data

### `load_json_with_schema(path, schema, **kwargs)`

Load JSON file with predefined schema for data type enforcement.

**Parameters:**
- `path` (str): Path to the JSON file
- `schema` (dict): Dictionary mapping column names to data types
- `**kwargs`: Additional arguments passed to `load_json()`

**Returns:**
- `pd.DataFrame`: Loaded DataFrame with enforced schema

### `export_to_json(data, path, orient='records', **kwargs)`

Export DataFrame to JSON file.

**Parameters:**
- `data` (pd.DataFrame): DataFrame to export
- `path` (str): Output JSON file path
- `orient` (str): JSON orientation ('records', 'index', 'values', 'table')
- `**kwargs`: Additional arguments passed to `pd.DataFrame.to_json()`

### `_flatten_dict(d, max_depth=10, current_depth=0)`

Recursively flatten a nested dictionary.

**Parameters:**
- `d` (dict): Dictionary to flatten
- `max_depth` (int): Maximum recursion depth
- `current_depth` (int): Current recursion depth

**Returns:**
- `dict`: Flattened dictionary

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic JSON Loading

```python
from json_extraction import load_json

# Load JSON file with default settings
df = load_json('data.json')
print(f"Loaded {len(df)} records")
```

### Loading with Nested Flattening

```python
# Load and flatten nested JSON structures
df = load_json('complex_data.json', flatten_nested=True)
print(f"Columns after flattening: {list(df.columns)}")
```

### Different Array Handling Strategies

```python
# Expand arrays into separate rows
df_expand = load_json('data.json', handle_arrays='expand')

# Join array elements with separator
df_join = load_json('data.json', handle_arrays='join')

# Take first element only
df_first = load_json('data.json', handle_arrays='first')

# Take last element only
df_last = load_json('data.json', handle_arrays='last')
```

### Loading JSONL Files

```python
from json_extraction import load_jsonl

# Load JSON Lines format
df = load_jsonl('data.jsonl')
print(f"Loaded {len(df)} records from JSONL")
```

### Loading with Schema

```python
# Define schema for data type enforcement
schema = {
    'id': 'int64',
    'name': 'string',
    'age': 'int32',
    'created_at': 'datetime64[ns]'
}

df = load_json_with_schema('data.json', schema)
print(f"Data types: {df.dtypes}")
```

### Exporting to JSON

```python
from json_extraction import export_to_json

# Export DataFrame to JSON
export_to_json(df, 'output.json', orient='records', indent=2)
```

### Handling Large Files

```python
# For large JSON files (>50MB), streaming parser is automatically used
# if ijson is available
df = load_json('large_data.json')
```

## Dependencies

- `pandas`: For DataFrame operations
- `json`: For JSON parsing (built-in)
- `ijson`: For streaming large JSON files (optional)
- `numpy`: For numerical operations (used in examples)

## JSON Structure Support

The module handles various JSON structures:

### Array of Objects
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

### Nested Objects
```json
{
  "users": [
    {
      "id": 1,
      "profile": {
        "age": 28,
        "location": {
          "city": "New York",
          "country": "USA"
        }
      }
    }
  ]
}
```

### Mixed Data Types
```json
{
  "data": [
    {"id": 1, "tags": ["tag1", "tag2"]},
    {"id": 2, "tags": ["tag3"]}
  ]
}
```

## Array Handling Strategies

### Expand
Expands arrays of objects into separate rows:
```json
{"orders": [{"id": 1, "product": "A"}, {"id": 2, "product": "B"}]}
```
Becomes:
```
orders_0_id  orders_0_product  orders_1_id  orders_1_product
1           A                 2           B
```

### Join
Joins array elements with a separator:
```json
{"tags": ["tag1", "tag2", "tag3"]}
```
Becomes:
```
tags
tag1|tag2|tag3
```

### First/Last
Takes the first or last element:
```json
{"tags": ["tag1", "tag2", "tag3"]}
```
Becomes (first):
```
tags
tag1
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when JSON file doesn't exist
2. **Invalid JSON**: Handles malformed JSON with detailed error messages
3. **Memory Management**: Streaming parser for large files
4. **Data Validation**: Checks for empty DataFrames and malformed data
5. **Encoding Issues**: Support for different file encodings

## Performance Considerations

1. **Large Files**: Use streaming parser for files >50MB
2. **Memory Usage**: Monitor memory usage for large datasets
3. **Nested Structures**: Flattening can significantly increase column count
4. **Array Expansion**: Can create many columns for complex arrays

## Best Practices

1. **Use appropriate array handling** based on your data structure
2. **Set reasonable max_depth** to prevent infinite recursion
3. **Validate data** before processing
4. **Use streaming parser** for large files
5. **Handle encoding issues** by specifying correct encoding
6. **Monitor memory usage** for large datasets

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python json_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

## Supported File Formats

- `.json`: Standard JSON format
- `.jsonl`: JSON Lines format (one JSON object per line)
- `.jsonc`: JSON with comments (limited support)

## Streaming Parser

For large JSON files (>50MB), the module automatically uses a streaming parser if `ijson` is available:

```bash
pip install ijson
```

The streaming parser processes JSON files incrementally, reducing memory usage for large datasets.

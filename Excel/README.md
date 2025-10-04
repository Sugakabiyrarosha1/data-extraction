# Excel Data Extraction

This folder contains robust, production-ready functions for extracting data from Excel files into Pandas DataFrames with support for multiple sheets, data type inference, and comprehensive error handling.

## Features

- **Multiple Sheet Support**: Load single sheets, multiple sheets, or all sheets at once
- **Data Type Specification**: Enforce specific data types during loading
- **Sheet Information**: Get sheet metadata without loading data
- **Column Validation**: Validate required columns are present
- **Export Functionality**: Export DataFrames back to Excel format
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Flexible Parsing**: Support for custom header rows, skip rows, and other options
- **Engine Support**: Support for different Excel engines (openpyxl, xlrd)

## Files

- `excel_extraction.py`: Main module containing all Excel extraction functions

## Functions

### `load_excel(path, sheet_name=0, header_row=0, skip_rows=0, data_types=None, engine='openpyxl', **kwargs)`

Main function for loading Excel files with advanced features.

**Parameters:**
- `path` (str): Path to the Excel file
- `sheet_name`: Sheet name(s) to load. Can be:
  - `int`: Sheet index (0-based)
  - `str`: Sheet name
  - `list`: Multiple sheets (returns dict of DataFrames)
  - `None`: All sheets (returns dict of DataFrames)
- `header_row` (int): Row number to use as column headers
- `skip_rows` (int): Number of rows to skip from the beginning
- `data_types` (dict): Dictionary mapping column names to data types
- `engine` (str): Excel engine to use ('openpyxl' or 'xlrd')
- `**kwargs`: Additional arguments passed to `pd.read_excel()`

**Returns:**
- `pd.DataFrame` or `Dict[str, pd.DataFrame]`: Loaded DataFrame(s)

### `get_excel_sheet_info(path)`

Get information about Excel file sheets without loading data.

**Parameters:**
- `path` (str): Path to the Excel file

**Returns:**
- `dict`: Information about sheets in the Excel file
  - `max_row`: Maximum row number
  - `max_column`: Maximum column number
  - `dimensions`: String representation of sheet dimensions

### `load_excel_with_validation(path, sheet_name=0, required_columns=None, **kwargs)`

Load Excel file with column validation.

**Parameters:**
- `path` (str): Path to the Excel file
- `sheet_name`: Sheet name or index to load
- `required_columns` (list): List of required column names
- `**kwargs`: Additional arguments passed to `load_excel()`

**Returns:**
- `pd.DataFrame`: Loaded and validated DataFrame

**Raises:**
- `ValueError`: If required columns are missing

### `export_to_excel(data, path, sheet_name='Sheet1', **kwargs)`

Export DataFrame(s) to Excel file.

**Parameters:**
- `data`: DataFrame or dictionary of DataFrames to export
- `path` (str): Output Excel file path
- `sheet_name` (str): Sheet name for single DataFrame
- `**kwargs`: Additional arguments passed to `pd.ExcelWriter()`

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

### Basic Excel Loading

```python
from excel_extraction import load_excel

# Load single sheet
df = load_excel('data.xlsx', sheet_name='Sales')
print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
```

### Loading Multiple Sheets

```python
# Load all sheets
all_sheets = load_excel('data.xlsx', sheet_name=None)
for sheet_name, df in all_sheets.items():
    print(f"Sheet: {sheet_name}, Shape: {df.shape}")

# Load specific sheets
specific_sheets = load_excel('data.xlsx', sheet_name=['Sales', 'Products'])
```

### Loading with Data Type Specification

```python
# Define data types for columns
data_types = {
    'Date': 'datetime64[ns]',
    'Product': 'category',
    'Quantity': 'int32',
    'Price': 'float64'
}

df = load_excel('data.xlsx', sheet_name='Sales', data_types=data_types)
```

### Getting Sheet Information

```python
from excel_extraction import get_excel_sheet_info

# Get information about all sheets without loading data
sheet_info = get_excel_sheet_info('data.xlsx')
for sheet_name, info in sheet_info.items():
    print(f"{sheet_name}: {info['dimensions']}")
```

### Loading with Column Validation

```python
from excel_extraction import load_excel_with_validation

# Load with required columns validation
required_columns = ['Date', 'Product', 'Price']
df = load_excel_with_validation(
    'data.xlsx', 
    sheet_name='Sales', 
    required_columns=required_columns
)
```

### Exporting to Excel

```python
from excel_extraction import export_to_excel

# Export single DataFrame
export_to_excel(df, 'output.xlsx', sheet_name='Results')

# Export multiple DataFrames
data_dict = {'Sales': df_sales, 'Products': df_products}
export_to_excel(data_dict, 'output.xlsx')
```

## Dependencies

- `pandas`: For DataFrame operations
- `openpyxl`: For Excel file reading/writing (default engine)
- `xlrd`: Alternative engine for older Excel files
- `numpy`: For numerical operations (used in examples)

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when Excel file doesn't exist
2. **Sheet Not Found**: Handles invalid sheet names or indices
3. **Data Validation**: Checks for empty DataFrames, empty columns, and duplicate column names
4. **Column Validation**: Ensures required columns are present
5. **Engine Compatibility**: Handles different Excel file formats and engines

## Logging

The module uses Python's logging system to provide detailed information about:

- File loading progress
- Sheet discovery and loading
- Data validation results
- Memory usage statistics
- Error messages and warnings

## Best Practices

1. **Use appropriate engines**: openpyxl for .xlsx files, xlrd for older .xls files
2. **Specify data types** when you know the expected format
3. **Validate required columns** for critical data processing
4. **Use sheet information** to understand file structure before loading
5. **Handle large files** by loading specific sheets or using chunking
6. **Monitor memory usage** for large datasets

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python excel_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

## Supported File Formats

- `.xlsx`: Excel 2007+ format (recommended, uses openpyxl)
- `.xls`: Excel 97-2003 format (uses xlrd)
- `.xlsm`: Excel with macros (uses openpyxl)
- `.xlsb`: Excel binary format (limited support)

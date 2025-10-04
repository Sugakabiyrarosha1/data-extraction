# XML Data Extraction

This folder contains robust, production-ready functions for extracting data from XML files into Pandas DataFrames with support for complex nested structures, namespaces, and large files.

## Features

- **XML File Parsing**: Load data from standard XML files
- **Nested Structure Support**: Handle complex nested XML structures
- **Namespace Handling**: Support for XML namespaces
- **Streaming Parser**: Memory-efficient processing for large XML files
- **Automatic Record Detection**: Intelligent detection of record elements
- **Structure Analysis**: Analyze XML structure without loading all data
- **Export Functionality**: Export DataFrames back to XML format
- **Comprehensive Error Handling**: Detailed error messages and exception handling
- **Data Validation**: Built-in data quality checks and validation
- **Memory Management**: Efficient processing for large datasets

## Files

- `xml_extraction.py`: Main module containing all XML extraction functions

## Functions

### `load_xml(path, root_element=None, record_element=None, namespaces=None, encoding='utf-8', use_lxml=True)`

Main function for loading XML files with advanced parsing capabilities.

**Parameters:**
- `path` (str): Path to the XML file
- `root_element` (str, optional): Root element to start parsing from
- `record_element` (str, optional): Element that represents individual records
- `namespaces` (dict, optional): XML namespaces mapping
- `encoding` (str): File encoding
- `use_lxml` (bool): Whether to use lxml for better performance

**Returns:**
- `pd.DataFrame`: Parsed XML data

### `load_xml_streaming(path, record_element, chunk_size=1000, namespaces=None)`

Load large XML files using streaming parser.

**Parameters:**
- `path` (str): Path to XML file
- `record_element` (str): Element that represents individual records
- `chunk_size` (int): Number of records to process at once
- `namespaces` (dict, optional): XML namespaces

**Returns:**
- `pd.DataFrame`: Parsed XML data

### `get_xml_structure(path)`

Get the structure of an XML file without loading all data.

**Parameters:**
- `path` (str): Path to XML file

**Returns:**
- `dict`: XML structure information including:
  - `root_element`: Root element name
  - `root_attributes`: Root element attributes
  - `child_elements`: List of child elements with their properties
  - `total_elements`: Total number of elements

### `export_to_xml(data, path, root_name='data', record_name='record', **kwargs)`

Export DataFrame to XML file.

**Parameters:**
- `data` (pd.DataFrame): DataFrame to export
- `path` (str): Output XML file path
- `root_name` (str): Root element name
- `record_name` (str): Record element name
- `**kwargs`: Additional arguments for XML formatting

### `_parse_xml_element(element)`

Internal function for parsing XML elements into dictionaries.

**Parameters:**
- `element`: XML element to parse

**Returns:**
- `dict`: Parsed element data

### `_find_record_elements(root)`

Internal function for finding common record element patterns.

**Parameters:**
- `root`: Root XML element

**Returns:**
- `list`: List of record elements

### `_validate_dataframe(df, source)`

Internal function for DataFrame validation.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to validate
- `source` (str): Source file path for logging

## Usage Examples

### Basic XML Loading

```python
from xml_extraction import load_xml

# Load XML file with automatic record detection
df = load_xml('data.xml')
print(f"Loaded {len(df)} records")
```

### Loading with Specific Record Element

```python
# Load XML with specific record element
df = load_xml('data.xml', record_element='product')
print(f"Columns: {list(df.columns)}")
```

### Loading with Namespaces

```python
# Load XML with namespace handling
namespaces = {
    'ns': 'http://example.com/namespace',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

df = load_xml('data.xml', record_element='ns:product', namespaces=namespaces)
```

### Getting XML Structure

```python
from xml_extraction import get_xml_structure

# Analyze XML structure without loading data
structure = get_xml_structure('data.xml')
print(f"Root element: {structure['root_element']}")
print(f"Child elements: {[elem['tag'] for elem in structure['child_elements']]}")
```

### Streaming Large XML Files

```python
from xml_extraction import load_xml_streaming

# Load large XML file with streaming parser
df = load_xml_streaming('large_data.xml', record_element='record', chunk_size=1000)
```

### Exporting to XML

```python
from xml_extraction import export_to_xml

# Export DataFrame to XML
export_to_xml(df, 'output.xml', root_name='products', record_name='product')
```

## Dependencies

- `pandas`: For DataFrame operations
- `xml.etree.ElementTree`: For XML parsing (built-in)
- `lxml`: For better performance and streaming (optional)
- `numpy`: For numerical operations (used in examples)

## XML Structure Support

The module handles various XML structures:

### Simple XML
```xml
<products>
    <product id="1">
        <name>Laptop</name>
        <price>999.99</price>
    </product>
</products>
```

### Nested XML
```xml
<products>
    <product id="1" category="electronics">
        <name>Laptop</name>
        <specifications>
            <cpu>Intel i7</cpu>
            <ram>16GB</ram>
        </specifications>
    </product>
</products>
```

### XML with Namespaces
```xml
<ns:products xmlns:ns="http://example.com/namespace">
    <ns:product id="1">
        <ns:name>Laptop</ns:name>
    </ns:product>
</ns:products>
```

## Record Element Detection

The module automatically detects common record element patterns:

- `item`
- `record`
- `row`
- `entry`
- `product`
- `user`
- `customer`
- `order`
- `transaction`
- `event`
- `log`
- `message`

## Attribute Handling

XML attributes are automatically converted to DataFrame columns with the `@` prefix:

```xml
<product id="1" category="electronics">
    <name>Laptop</name>
</product>
```

Becomes:
```
@id  @category  name
1    electronics  Laptop
```

## Nested Element Handling

Nested elements are preserved as dictionaries in the DataFrame:

```xml
<product>
    <specifications>
        <cpu>Intel i7</cpu>
        <ram>16GB</ram>
    </specifications>
</product>
```

Becomes:
```
specifications
{'cpu': 'Intel i7', 'ram': '16GB'}
```

## Error Handling

The module includes comprehensive error handling for common scenarios:

1. **File Not Found**: Clear error message when XML file doesn't exist
2. **Invalid XML**: Handles malformed XML with detailed error messages
3. **Memory Management**: Streaming parser for large files
4. **Data Validation**: Checks for empty DataFrames and malformed data
5. **Encoding Issues**: Support for different file encodings

## Performance Considerations

1. **Large Files**: Use streaming parser for files >100MB
2. **Memory Usage**: Monitor memory usage for large datasets
3. **Nested Structures**: Complex nesting can increase memory usage
4. **Parser Choice**: lxml is faster but requires additional dependency

## Best Practices

1. **Use appropriate parser** based on your needs (lxml for performance, ElementTree for simplicity)
2. **Specify record elements** when you know the structure
3. **Handle namespaces** properly for complex XML documents
4. **Use streaming parser** for large files
5. **Validate data** before processing
6. **Monitor memory usage** for large datasets

## Testing

The module includes built-in testing functionality. Run the script directly to see example usage:

```bash
python xml_extraction.py
```

This will create sample data, test all functions, and demonstrate various loading scenarios.

## Supported File Formats

- `.xml`: Standard XML format
- `.xml.gz`: Compressed XML files (with appropriate decompression)

## Streaming Parser

For large XML files (>100MB), the module provides a streaming parser if `lxml` is available:

```bash
pip install lxml
```

The streaming parser processes XML files incrementally, reducing memory usage for large datasets.

## Common XML Patterns

### E-commerce Product Catalog
```xml
<catalog>
    <product id="1" category="electronics">
        <name>Laptop</name>
        <price>999.99</price>
        <specifications>
            <cpu>Intel i7</cpu>
            <ram>16GB</ram>
        </specifications>
    </product>
</catalog>
```

### User Data
```xml
<users>
    <user id="1">
        <name>John Doe</name>
        <email>john@example.com</email>
        <profile>
            <age>30</age>
            <location>New York</location>
        </profile>
    </user>
</users>
```

### Log Data
```xml
<logs>
    <log timestamp="2024-01-01T10:00:00Z" level="INFO">
        <message>Application started</message>
        <source>main.py</source>
    </log>
</logs>
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Use streaming parser for large files
2. **Namespace Problems**: Specify namespaces correctly
3. **Record Detection**: Manually specify record elements if auto-detection fails
4. **Encoding Issues**: Specify correct encoding for non-UTF-8 files

### Performance Tips

1. **Use lxml** for better performance
2. **Specify record elements** to avoid unnecessary parsing
3. **Use streaming parser** for large files
4. **Filter data** at the XML level when possible

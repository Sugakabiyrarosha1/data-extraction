# Data Extraction Toolkit

A comprehensive, production-ready toolkit for extracting data from various sources into Pandas DataFrames. This toolkit provides robust, well-documented functions for handling different data formats and sources with advanced features like error handling, data validation, and performance optimization.

## 🚀 Features

- **Multiple Data Sources**: Support for CSV, Excel, APIs, JSON, MongoDB, XML, Parquet, HDF5, MySQL, and Web Scraping
- **Production Ready**: Comprehensive error handling, logging, and data validation
- **Performance Optimized**: Memory-efficient processing, chunked loading, and streaming support
- **Advanced Features**: Pagination, filtering, compression, and connection pooling
- **Well Documented**: Detailed README files with usage examples for each module
- **Type Hints**: Full type annotation support for better IDE integration
- **Flexible Configuration**: Extensive customization options for different use cases

## 📁 Project Structure

```
data-extraction/
├── README.md                           # This file - main project overview
├── data_extraction.ipynb              # Original notebook with examples
├── CSV/                               # CSV data extraction
│   ├── csv_extraction.py
│   └── README.md
├── Excel/                             # Excel data extraction
│   ├── excel_extraction.py
│   └── README.md
├── API/                               # API data extraction
│   ├── api_extraction.py
│   └── README.md
├── JSON/                              # JSON data extraction
│   ├── json_extraction.py
│   └── README.md
├── MongoDB/                           # MongoDB data extraction
│   ├── mongodb_extraction.py
│   └── README.md
├── XML/                               # XML data extraction
│   ├── xml_extraction.py
│   └── README.md
├── Parquet/                           # Parquet data extraction
│   ├── parquet_extraction.py
│   └── README.md
├── HDF5/                              # HDF5 data extraction
│   ├── hdf5_extraction.py
│   └── README.md
├── MySQL/                             # MySQL data extraction
│   ├── mysql_extraction.py
│   └── README.md
└── WebScraping/                       # Web scraping data extraction
    ├── webscraping_extraction.py
    └── README.md
```

## 🛠️ Installation

### Basic Requirements

```bash
# Core dependencies
pip install pandas numpy

# For specific modules, install additional dependencies:

# CSV/Excel
pip install openpyxl xlrd

# API
pip install requests

# JSON
pip install ijson

# MongoDB
pip install pymongo

# XML
pip install lxml

# Parquet
pip install pyarrow

# HDF5
pip install h5py tables

# MySQL
pip install mysql-connector-python sqlalchemy

# Web Scraping
pip install requests beautifulsoup4 selenium
```

### Complete Installation

```bash
# Install all dependencies at once
pip install pandas numpy openpyxl xlrd requests ijson pymongo lxml pyarrow h5py tables mysql-connector-python sqlalchemy beautifulsoup4 selenium
```

## 📚 Quick Start

### Basic Usage

```python
# Import the extraction functions you need
from CSV.csv_extraction import load_csv
from Excel.excel_extraction import load_excel
from API.api_extraction import load_api
from JSON.json_extraction import load_json
from MongoDB.mongodb_extraction import load_mongodb
from XML.xml_extraction import load_xml
from Parquet.parquet_extraction import load_parquet
from HDF5.hdf5_extraction import load_hdf5
from MySQL.mysql_extraction import load_mysql
from WebScraping.webscraping_extraction import scrape_website

# Load data from different sources
df_csv = load_csv('data.csv')
df_excel = load_excel('data.xlsx', sheet_name='Sheet1')
df_api = load_api('https://api.example.com/data')
df_json = load_json('data.json')
df_mongodb = load_mongodb('mongodb://localhost:27017/', 'mydb', 'mycollection')
df_xml = load_xml('data.xml', record_element='item')
df_parquet = load_parquet('data.parquet')
df_hdf5 = load_hdf5('data.h5', key='dataset')
df_mysql = load_mysql('localhost', 'user', 'password', 'mydb', 'SELECT * FROM mytable')
df_web = scrape_website('https://example.com', {'title': 'h1', 'content': '.content'})

# All functions return Pandas DataFrames
print(f"Loaded {len(df_csv)} rows from CSV")
print(f"Loaded {len(df_excel)} rows from Excel")
print(f"Loaded {len(df_api)} rows from API")
```

### Advanced Usage

```python
# Advanced CSV loading with custom options
df_csv = load_csv(
    path='large_file.csv',
    encoding='utf-8',
    delimiter=';',
    skip_rows=1,
    chunk_size=10000,
    use_pandas_engine=True
)

# Advanced Excel loading with multiple sheets
df_excel = load_excel(
    path='workbook.xlsx',
    sheet_name=['Sheet1', 'Sheet2'],
    header_row=1,
    use_openpyxl=True,
    engine='openpyxl'
)

# Advanced API loading with pagination
df_api = load_api(
    url='https://api.example.com/data',
    pagination=True,
    page_param='page',
    per_page_param='limit',
    max_pages=10,
    data_key='results'
)

# Advanced JSON loading with flattening
df_json = load_json(
    path='nested_data.json',
    flatten_nested=True,
    handle_arrays='expand',
    max_depth=5
)

# Advanced MongoDB loading with query
df_mongodb = load_mongodb(
    uri='mongodb://localhost:27017/',
    db_name='mydb',
    collection_name='mycollection',
    query={'status': 'active'},
    projection={'name': 1, 'email': 1},
    sort=[('created_at', -1)],
    limit=1000
)

# Advanced XML loading with namespaces
df_xml = load_xml(
    path='data.xml',
    root_element='catalog',
    record_element='product',
    namespaces={'ns': 'http://example.com/ns'},
    use_lxml=True
)

# Advanced Parquet loading with filtering
df_parquet = load_parquet(
    path='data.parquet',
    columns=['id', 'name', 'value'],
    filters=[('category', '=', 'A')],
    engine='pyarrow'
)

# Advanced HDF5 loading with chunking
df_hdf5 = load_hdf5(
    path='data.h5',
    key='dataset',
    columns=['id', 'value'],
    where='value > 100',
    chunksize=5000
)

# Advanced MySQL loading with connection pooling
df_mysql = load_mysql(
    host='localhost',
    user='user',
    password='password',
    database='mydb',
    query='SELECT * FROM mytable WHERE status = %s',
    params=['active'],
    use_pooling=True,
    chunk_size=1000
)

# Advanced web scraping with Selenium
df_web = scrape_website(
    url='https://example.com',
    selectors={'title': 'h1', 'content': '.content'},
    use_selenium=True,
    max_pages=5,
    delay=1.0,
    wait_for_element='.content'
)
```

## 🔧 Module Overview

### 1. CSV Extraction (`CSV/`)
- **Features**: Multiple encodings, custom delimiters, chunked loading, data validation
- **Use Cases**: Large CSV files, different formats, data quality checks
- **Key Functions**: `load_csv()`, `get_csv_info()`, `validate_csv()`

### 2. Excel Extraction (`Excel/`)
- **Features**: Multiple sheets, different engines, header customization, data validation
- **Use Cases**: Excel workbooks, multiple sheets, different Excel versions
- **Key Functions**: `load_excel()`, `get_excel_info()`, `load_excel_sheets()`

### 3. API Extraction (`API/`)
- **Features**: RESTful APIs, pagination, authentication, rate limiting, data flattening
- **Use Cases**: REST APIs, paginated data, authenticated endpoints
- **Key Functions**: `load_api()`, `_flatten_nested_json()`, `_handle_pagination()`

### 4. JSON Extraction (`JSON/`)
- **Features**: Nested structures, arrays, streaming, JSONL support
- **Use Cases**: Complex JSON data, large files, nested structures
- **Key Functions**: `load_json()`, `load_jsonl()`, `_flatten_dict()`

### 5. MongoDB Extraction (`MongoDB/`)
- **Features**: Connection pooling, query optimization, batch processing, collection info
- **Use Cases**: MongoDB databases, large collections, complex queries
- **Key Functions**: `load_mongodb()`, `get_mongodb_collection_info()`, `MongoDBClient`

### 6. XML Extraction (`XML/`)
- **Features**: Namespaces, streaming, complex structures, multiple parsers
- **Use Cases**: XML data, large files, complex hierarchies
- **Key Functions**: `load_xml()`, `load_xml_streaming()`, `_parse_xml_element()`

### 7. Parquet Extraction (`Parquet/`)
- **Features**: Columnar format, compression, filtering, partitioned data
- **Use Cases**: Big data, columnar storage, efficient querying
- **Key Functions**: `load_parquet()`, `load_parquet_partitioned()`, `get_parquet_metadata()`

### 8. HDF5 Extraction (`HDF5/`)
- **Features**: Hierarchical storage, compression, chunked loading, querying
- **Use Cases**: Scientific data, large datasets, hierarchical storage
- **Key Functions**: `load_hdf5()`, `load_hdf5_chunked()`, `get_hdf5_info()`

### 9. MySQL Extraction (`MySQL/`)
- **Features**: Connection pooling, parameterized queries, batch processing, table info
- **Use Cases**: MySQL databases, large tables, complex queries
- **Key Functions**: `load_mysql()`, `execute_mysql_query()`, `get_mysql_table_info()`

### 10. Web Scraping (`WebScraping/`)
- **Features**: BeautifulSoup, Selenium, e-commerce scraping, table extraction
- **Use Cases**: Website data, dynamic content, product information
- **Key Functions**: `scrape_website()`, `scrape_ecommerce_products()`, `scrape_table_data()`

## 🎯 Use Cases

### Data Science and Analytics
- Load data from multiple sources for analysis
- Handle large datasets efficiently
- Validate data quality automatically
- Process different file formats seamlessly

### ETL Pipelines
- Extract data from various sources
- Transform data during loading
- Load data into target systems
- Handle errors and retries

### Web Scraping and Data Collection
- Scrape websites for data collection
- Handle dynamic content with Selenium
- Extract structured data from tables
- Respect rate limits and robots.txt

### Database Operations
- Connect to different database types
- Execute complex queries
- Handle connection pooling
- Process large result sets

### File Processing
- Handle different file formats
- Process large files efficiently
- Validate data integrity
- Support various encodings

## 🔍 Data Validation

All modules include comprehensive data validation:

```python
# Automatic validation includes:
# - Empty DataFrame checks
# - Duplicate column detection
# - Memory usage monitoring
# - Data type validation
# - Missing value detection
# - Data quality metrics

# Example validation output:
# INFO: DataFrame shape: (1000, 5)
# INFO: Memory usage: 0.05 MB
# WARNING: Found empty columns: ['unused_column']
# WARNING: Found duplicate column names: ['id', 'id']
```

## 🚨 Error Handling

Robust error handling with meaningful messages:

```python
try:
    df = load_csv('data.csv')
except FileNotFoundError:
    print("File not found. Please check the path.")
except pd.errors.EmptyDataError:
    print("File is empty or contains no data.")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## 📊 Performance Features

### Memory Efficiency
- Chunked loading for large files
- Streaming parsers for XML/JSON
- Memory usage monitoring
- Garbage collection optimization

### Speed Optimization
- Connection pooling for databases
- Parallel processing where possible
- Efficient data types
- Lazy loading for large datasets

### Scalability
- Batch processing for large collections
- Pagination support for APIs
- Configurable chunk sizes
- Progress tracking

## 🔧 Configuration

### Environment Variables
```bash
# Database connections
export MYSQL_HOST=localhost
export MYSQL_USER=user
export MYSQL_PASSWORD=password
export MONGODB_URI=mongodb://localhost:27017/

# API settings
export API_KEY=your_api_key
export API_BASE_URL=https://api.example.com

# Web scraping
export CHROME_DRIVER_PATH=/path/to/chromedriver
export USER_AGENT=Custom Bot 1.0
```

### Configuration Files
```python
# config.py
DATABASE_CONFIG = {
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'pool_size': 10
    },
    'mongodb': {
        'uri': 'mongodb://localhost:27017/',
        'max_pool_size': 100
    }
}

API_CONFIG = {
    'base_url': 'https://api.example.com',
    'timeout': 30,
    'retries': 3
}
```

## 🧪 Testing

### Unit Tests
```python
# Example test structure
import unittest
from CSV.csv_extraction import load_csv

class TestCSVExtraction(unittest.TestCase):
    def test_load_csv_basic(self):
        df = load_csv('test_data.csv')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
    
    def test_load_csv_with_options(self):
        df = load_csv('test_data.csv', delimiter=';', encoding='utf-8')
        self.assertIsInstance(df, pd.DataFrame)
```

### Integration Tests
```python
# Test with real data sources
def test_api_integration():
    df = load_api('https://jsonplaceholder.typicode.com/posts')
    assert len(df) > 0
    assert 'title' in df.columns
```

## 📈 Monitoring and Logging

### Logging Configuration
```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_extraction.log'),
        logging.StreamHandler()
    ]
)
```

### Performance Monitoring
```python
import time
from contextlib import contextmanager

@contextmanager
def timer(operation_name):
    start_time = time.time()
    try:
        yield
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"{operation_name} completed in {elapsed_time:.2f} seconds")

# Usage
with timer("CSV Loading"):
    df = load_csv('large_file.csv')
```

## 🤝 Contributing

### Development Setup
```bash
# Clone the repository
git clone <repository-url>
cd data-extraction

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest tests/

# Run linting
flake8 .
black .
```

### Code Style
- Follow PEP 8 guidelines
- Use type hints
- Write comprehensive docstrings
- Include error handling
- Add logging statements

### Pull Request Process
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Pandas**: For the excellent DataFrame functionality
- **Requests**: For HTTP library functionality
- **BeautifulSoup**: For HTML parsing
- **Selenium**: For web automation
- **PyMongo**: For MongoDB integration
- **SQLAlchemy**: For database abstraction
- **PyArrow**: For Parquet support
- **H5Py**: For HDF5 support

## 📞 Support

### Documentation
- Each module has detailed README files
- Code includes comprehensive docstrings
- Examples are provided for all functions

### Issues
- Report bugs via GitHub issues
- Request features via GitHub issues
- Ask questions via GitHub discussions

### Community
- Join our community discussions
- Share your use cases
- Contribute improvements

## 🔄 Version History

### v1.0.0 (Current)
- Initial release with all 10 extraction modules
- Comprehensive documentation
- Production-ready error handling
- Performance optimizations
- Full type hint support

### Future Releases
- Additional data source support
- Enhanced performance features
- More configuration options
- Extended testing coverage

---

**Happy Data Extracting! 🎉**

This toolkit is designed to make data extraction from various sources as simple and reliable as possible. Whether you're working with small CSV files or large distributed databases, these modules provide the tools you need to get your data into Pandas DataFrames efficiently and reliably.

For detailed information about each module, please refer to the individual README files in each folder.
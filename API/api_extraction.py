"""
API Data Extraction Module

This module provides robust, production-ready functions for extracting data from REST APIs
into Pandas DataFrames with authentication, pagination, rate limiting, and comprehensive error handling.

Features:
- REST API data extraction with authentication support
- Automatic pagination handling
- Rate limiting and retry mechanisms
- Nested JSON structure flattening
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Support for various API response formats
- Session management with connection pooling

Author: Data Extraction Toolkit
"""

import requests
import time
import json
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClient:
    """
    Advanced API client with authentication, rate limiting, and error handling.
    """
    
    def __init__(self, 
                 base_url: str = "",
                 api_key: Optional[str] = None,
                 headers: Optional[Dict[str, str]] = None,
                 rate_limit_delay: float = 0.1,
                 max_retries: int = 3):
        """
        Initialize API client with configuration.
        
        Args:
            base_url (str): Base URL for the API
            api_key (str, optional): API key for authentication
            headers (dict, optional): Additional headers
            rate_limit_delay (float): Delay between requests in seconds
            max_retries (int): Maximum number of retry attempts
        """
        self.base_url = base_url.rstrip('/')
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Setup headers
        self.headers = headers or {}
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'
        self.session.headers.update(self.headers)
    
    def _rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = time.time()
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> requests.Response:
        """
        Make GET request with rate limiting and error handling.
        
        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters
            
        Returns:
            requests.Response: API response
        """
        self._rate_limit()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def post(self, endpoint: str, data: Optional[Dict] = None, json_data: Optional[Dict] = None) -> requests.Response:
        """
        Make POST request with rate limiting and error handling.
        
        Args:
            endpoint (str): API endpoint
            data (dict, optional): Form data
            json_data (dict, optional): JSON data
            
        Returns:
            requests.Response: API response
        """
        self._rate_limit()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            if json_data:
                response = self.session.post(url, json=json_data, timeout=30)
            else:
                response = self.session.post(url, data=data, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API POST request failed: {e}")
            raise


def load_api(url: str,
             api_key: Optional[str] = None,
             headers: Optional[Dict[str, str]] = None,
             params: Optional[Dict] = None,
             pagination: bool = False,
             page_param: str = 'page',
             per_page_param: str = 'per_page',
             max_pages: int = 10,
             data_key: Optional[str] = None,
             flatten_nested: bool = True) -> pd.DataFrame:
    """
    Load data from REST API with advanced features.
    
    Args:
        url (str): API endpoint URL
        api_key (str, optional): API key for authentication
        headers (dict, optional): Additional headers
        params (dict, optional): Query parameters
        pagination (bool): Whether to handle pagination
        page_param (str): Parameter name for page number
        per_page_param (str): Parameter name for items per page
        max_pages (int): Maximum number of pages to fetch
        data_key (str, optional): Key in JSON response containing the data array
        flatten_nested (bool): Whether to flatten nested JSON structures
    
    Returns:
        pd.DataFrame: Loaded and processed data
    """
    try:
        # Initialize API client
        client = APIClient(api_key=api_key, headers=headers)
        
        all_data = []
        page = 1
        
        while True:
            # Prepare request parameters
            request_params = params.copy() if params else {}
            if pagination:
                request_params[page_param] = page
                request_params[per_page_param] = 100  # Reasonable page size
            
            logger.info(f"Fetching page {page} from {url}")
            
            # Make API request
            response = client.get(url, params=request_params)
            data = response.json()
            
            # Extract data array if data_key is specified
            if data_key and data_key in data:
                page_data = data[data_key]
            else:
                page_data = data if isinstance(data, list) else [data]
            
            # Handle empty response
            if not page_data:
                logger.info("No more data available")
                break
            
            all_data.extend(page_data)
            
            # Check if we should continue pagination
            if not pagination or page >= max_pages:
                break
            
            # Check if this is the last page (common patterns)
            if isinstance(data, dict):
                if 'has_more' in data and not data['has_more']:
                    break
                if 'total_pages' in data and page >= data['total_pages']:
                    break
                if 'next' in data and not data['next']:
                    break
            
            page += 1
        
        if not all_data:
            logger.warning("No data retrieved from API")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Flatten nested structures if requested
        if flatten_nested:
            df = _flatten_nested_json(df)
        
        # Data validation
        _validate_dataframe(df, f"API: {url}")
        
        logger.info(f"Successfully loaded {len(df)} records from API")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from API {url}: {str(e)}")
        raise


def load_api_with_auth(url: str,
                      auth_type: str = 'bearer',
                      credentials: Dict[str, str] = None,
                      **kwargs) -> pd.DataFrame:
    """
    Load data from API with various authentication methods.
    
    Args:
        url (str): API endpoint URL
        auth_type (str): Type of authentication ('bearer', 'basic', 'api_key')
        credentials (dict): Authentication credentials
        **kwargs: Additional arguments passed to load_api()
    
    Returns:
        pd.DataFrame: Loaded and processed data
    """
    headers = kwargs.get('headers', {})
    
    if auth_type == 'bearer' and credentials.get('token'):
        headers['Authorization'] = f"Bearer {credentials['token']}"
    elif auth_type == 'basic' and credentials.get('username') and credentials.get('password'):
        import base64
        auth_string = f"{credentials['username']}:{credentials['password']}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        headers['Authorization'] = f"Basic {auth_b64}"
    elif auth_type == 'api_key' and credentials.get('api_key'):
        api_key_param = credentials.get('api_key_param', 'api_key')
        if credentials.get('api_key_header'):
            headers[credentials['api_key_header']] = credentials['api_key']
        else:
            # Add to URL parameters
            if 'params' not in kwargs:
                kwargs['params'] = {}
            kwargs['params'][api_key_param] = credentials['api_key']
    
    kwargs['headers'] = headers
    return load_api(url, **kwargs)


def _flatten_nested_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten nested JSON structures in DataFrame columns.
    
    Args:
        df (pd.DataFrame): DataFrame with potentially nested JSON columns
    
    Returns:
        pd.DataFrame: DataFrame with flattened columns
    """
    flattened_data = []
    
    for _, row in df.iterrows():
        flat_row = {}
        for col, value in row.items():
            if isinstance(value, dict):
                # Flatten dictionary values
                for nested_key, nested_value in value.items():
                    flat_row[f"{col}_{nested_key}"] = nested_value
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Handle list of dictionaries (common in APIs)
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        for nested_key, nested_value in item.items():
                            flat_row[f"{col}_{i}_{nested_key}"] = nested_value
            else:
                flat_row[col] = value
        flattened_data.append(flat_row)
    
    return pd.DataFrame(flattened_data)


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
    print("Testing API data extraction with public APIs...")
    
    # Example 1: JSONPlaceholder API (no authentication required)
    try:
        print("\n=== Testing JSONPlaceholder API ===")
        api_url = 'https://jsonplaceholder.typicode.com/posts'
        
        # Load data without pagination
        df_posts = load_api(
            url=api_url,
            pagination=False,
            flatten_nested=True
        )
        
        print(f"Successfully loaded {len(df_posts)} posts")
        print(f"Columns: {list(df_posts.columns)}")
        print("\nFirst 3 rows:")
        print(df_posts.head(3))
        
    except Exception as e:
        print(f"Error with JSONPlaceholder API: {e}")
    
    # Example 2: Cat Facts API (with pagination)
    try:
        print("\n=== Testing Cat Facts API ===")
        cat_api_url = 'https://catfact.ninja/facts'
        
        # Load data with pagination
        df_cats = load_api(
            url=cat_api_url,
            pagination=True,
            page_param='page',
            per_page_param='limit',
            max_pages=2,  # Limit to 2 pages
            data_key='data'  # The actual data is in the 'data' key
        )
        
        print(f"Successfully loaded {len(df_cats)} cat facts")
        print("\nFirst 3 rows:")
        print(df_cats.head(3))
        
    except Exception as e:
        print(f"Error with Cat Facts API: {e}")
    
    # Example 3: Mock API data for demonstration
    print("\n=== Testing Nested JSON Flattening ===")
    mock_api_data = [
        {
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zipcode": "10001"
            },
            "company": {
                "name": "Tech Corp",
                "department": "Engineering"
            }
        },
        {
            "id": 2,
            "name": "Jane Smith",
            "email": "jane@example.com",
            "address": {
                "street": "456 Oak Ave",
                "city": "Los Angeles",
                "zipcode": "90210"
            },
            "company": {
                "name": "Design Inc",
                "department": "Marketing"
            }
        }
    ]
    
    # Convert to DataFrame and demonstrate flattening
    df_mock = pd.DataFrame(mock_api_data)
    print("Original nested data:")
    print(df_mock)
    
    print("\nFlattened data:")
    df_flattened = _flatten_nested_json(df_mock)
    print(df_flattened)
    
    # Example 4: API Client usage
    print("\n=== Testing API Client ===")
    try:
        client = APIClient(
            base_url="https://jsonplaceholder.typicode.com",
            rate_limit_delay=0.1
        )
        
        response = client.get("/users")
        users_data = response.json()
        df_users = pd.DataFrame(users_data)
        
        print(f"Loaded {len(df_users)} users using API client")
        print(f"Columns: {list(df_users.columns)}")
        
    except Exception as e:
        print(f"Error with API client: {e}")
    
    print("\nAPI extraction testing completed!")

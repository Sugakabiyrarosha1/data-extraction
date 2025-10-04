"""
Web Scraping Data Extraction Module

This module provides robust, production-ready functions for extracting data from websites
into Pandas DataFrames with BeautifulSoup, Selenium support, and comprehensive error handling.

Features:
- BeautifulSoup and Selenium support for different website types
- Advanced web scraping with JavaScript support
- E-commerce product scraping with common selectors
- Table data extraction from HTML tables
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large datasets
- Support for pagination and dynamic content
- Rate limiting and respectful scraping

Author: Data Extraction Toolkit
"""

import os
import pandas as pd
import time
import re
from typing import Dict, List, Any, Optional, Union
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import web scraping libraries
try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logger.warning("requests and beautifulsoup4 not available. Install with: pip install requests beautifulsoup4")

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("selenium not available. Install with: pip install selenium")

from urllib.parse import urljoin, urlparse


class WebScraper:
    """
    Advanced web scraper with BeautifulSoup and Selenium support.
    """
    
    def __init__(self, 
                 use_selenium: bool = False,
                 headless: bool = True,
                 user_agent: str = None,
                 delay: float = 1.0):
        """
        Initialize web scraper.
        
        Args:
            use_selenium (bool): Whether to use Selenium for JavaScript-heavy sites
            headless (bool): Whether to run browser in headless mode
            user_agent (str): Custom user agent string
            delay (float): Delay between requests in seconds
        """
        self.use_selenium = use_selenium
        self.headless = headless
        self.delay = delay
        self.user_agent = user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        self.driver = None
        self.session = requests.Session() if REQUESTS_AVAILABLE else None
        
        # Setup session headers
        if self.session:
            self.session.headers.update({
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
    
    def get_page(self, url: str, wait_for_element: str = None) -> BeautifulSoup:
        """
        Get page content and return BeautifulSoup object.
        
        Args:
            url (str): URL to scrape
            wait_for_element (str, optional): CSS selector to wait for (Selenium only)
        
        Returns:
            BeautifulSoup: Parsed HTML content
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests and beautifulsoup4 are required for web scraping")
        
        try:
            if self.use_selenium and SELENIUM_AVAILABLE:
                return self._get_page_selenium(url, wait_for_element)
            else:
                return self._get_page_requests(url)
                
        except Exception as e:
            logger.error(f"Error getting page {url}: {str(e)}")
            raise
    
    def _get_page_requests(self, url: str) -> BeautifulSoup:
        """Get page using requests and BeautifulSoup."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Add delay between requests
            if self.delay > 0:
                time.sleep(self.delay)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
            
        except Exception as e:
            logger.error(f"Error with requests: {str(e)}")
            raise
    
    def _get_page_selenium(self, url: str, wait_for_element: str = None) -> BeautifulSoup:
        """Get page using Selenium WebDriver."""
        if not SELENIUM_AVAILABLE:
            raise ImportError("selenium is required for Selenium-based scraping")
        
        try:
            if not self.driver:
                self._setup_selenium_driver()
            
            self.driver.get(url)
            
            # Wait for specific element if specified
            if wait_for_element:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_element))
                )
            
            # Add delay
            if self.delay > 0:
                time.sleep(self.delay)
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            return soup
            
        except Exception as e:
            logger.error(f"Error with Selenium: {str(e)}")
            raise
    
    def _setup_selenium_driver(self):
        """Setup Selenium WebDriver."""
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument(f'--user-agent={self.user_agent}')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
        except Exception as e:
            logger.error(f"Error setting up Selenium driver: {str(e)}")
            raise
    
    def close(self):
        """Close Selenium driver if open."""
        if self.driver:
            self.driver.quit()
            self.driver = None


def scrape_website(url: str,
                  selectors: Dict[str, str],
                  use_selenium: bool = False,
                  max_pages: int = 1,
                  delay: float = 1.0,
                  wait_for_element: str = None) -> pd.DataFrame:
    """
    Scrape data from a website using CSS selectors.
    
    Args:
        url (str): Base URL to scrape
        selectors (dict): Field name to CSS selector mapping
        use_selenium (bool): Whether to use Selenium for JavaScript-heavy sites
        max_pages (int): Maximum number of pages to scrape
        delay (float): Delay between requests in seconds
        wait_for_element (str, optional): CSS selector to wait for (Selenium only)
    
    Returns:
        pd.DataFrame: Scraped data
    """
    try:
        logger.info(f"Scraping website: {url}")
        
        scraper = WebScraper(use_selenium=use_selenium, delay=delay)
        all_data = []
        
        try:
            for page in range(max_pages):
                # Construct page URL (assuming pagination)
                if page == 0:
                    page_url = url
                else:
                    # Common pagination patterns
                    if '?' in url:
                        page_url = f"{url}&page={page + 1}"
                    else:
                        page_url = f"{url}?page={page + 1}"
                
                logger.info(f"Scraping page {page + 1}: {page_url}")
                
                soup = scraper.get_page(page_url, wait_for_element)
                
                # Extract data using selectors
                page_data = _extract_data_with_selectors(soup, selectors)
                
                if not page_data:
                    logger.info(f"No data found on page {page + 1}, stopping")
                    break
                
                all_data.extend(page_data)
                logger.info(f"Extracted {len(page_data)} items from page {page + 1}")
        
        finally:
            scraper.close()
        
        if not all_data:
            logger.warning("No data extracted from website")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        _validate_dataframe(df, f"Web scraping: {url}")
        
        logger.info(f"Successfully scraped {len(df)} items from {max_pages} pages")
        return df
        
    except Exception as e:
        logger.error(f"Error scraping website {url}: {str(e)}")
        raise


def _extract_data_with_selectors(soup: BeautifulSoup, selectors: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Extract data from BeautifulSoup object using CSS selectors.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content
        selectors (dict): Field name to CSS selector mapping
    
    Returns:
        list: List of extracted data dictionaries
    """
    try:
        # Find the container element (usually the first selector's parent)
        first_selector = list(selectors.values())[0]
        container_elements = soup.select(first_selector)
        
        if not container_elements:
            logger.warning(f"No elements found for selector: {first_selector}")
            return []
        
        data = []
        
        for element in container_elements:
            item_data = {}
            
            for field_name, selector in selectors.items():
                try:
                    # Find element within the container
                    target_element = element.select_one(selector)
                    
                    if target_element:
                        # Extract text or attribute
                        if target_element.get_text(strip=True):
                            item_data[field_name] = target_element.get_text(strip=True)
                        elif target_element.get('href'):
                            item_data[field_name] = target_element.get('href')
                        elif target_element.get('src'):
                            item_data[field_name] = target_element.get('src')
                        else:
                            item_data[field_name] = str(target_element)
                    else:
                        item_data[field_name] = None
                        
                except Exception as e:
                    logger.warning(f"Error extracting {field_name}: {str(e)}")
                    item_data[field_name] = None
            
            if any(item_data.values()):  # Only add if at least one field has data
                data.append(item_data)
        
        return data
        
    except Exception as e:
        logger.error(f"Error extracting data with selectors: {str(e)}")
        raise


def scrape_ecommerce_products(base_url: str,
                             product_selector: str = '.product',
                             selectors: Optional[Dict[str, str]] = None,
                             max_pages: int = 5,
                             use_selenium: bool = False) -> pd.DataFrame:
    """
    Scrape e-commerce product data with common selectors.
    
    Args:
        base_url (str): Base URL of the e-commerce site
        product_selector (str): CSS selector for product containers
        selectors (dict, optional): Custom selectors for product fields
        max_pages (int): Maximum number of pages to scrape
        use_selenium (bool): Whether to use Selenium
    
    Returns:
        pd.DataFrame: Product data
    """
    try:
        # Default selectors for common e-commerce fields
        default_selectors = {
            'name': '.product-title, .product-name, h2, h3',
            'price': '.price, .product-price, .cost',
            'description': '.product-description, .description, p',
            'image': 'img',
            'rating': '.rating, .stars, .review-score',
            'availability': '.availability, .stock, .in-stock'
        }
        
        if selectors:
            default_selectors.update(selectors)
        
        # Add product container selector
        default_selectors['_container'] = product_selector
        
        logger.info(f"Scraping e-commerce products from: {base_url}")
        
        return scrape_website(
            url=base_url,
            selectors=default_selectors,
            use_selenium=use_selenium,
            max_pages=max_pages,
            wait_for_element=product_selector
        )
        
    except Exception as e:
        logger.error(f"Error scraping e-commerce products: {str(e)}")
        raise


def scrape_table_data(url: str,
                     table_selector: str = 'table',
                     header_row: int = 0,
                     use_selenium: bool = False) -> pd.DataFrame:
    """
    Scrape tabular data from HTML tables.
    
    Args:
        url (str): URL containing the table
        table_selector (str): CSS selector for the table
        header_row (int): Row index to use as headers
        use_selenium (bool): Whether to use Selenium
    
    Returns:
        pd.DataFrame: Table data
    """
    try:
        logger.info(f"Scraping table data from: {url}")
        
        scraper = WebScraper(use_selenium=use_selenium)
        
        try:
            soup = scraper.get_page(url)
            
            # Find the table
            table = soup.select_one(table_selector)
            if not table:
                logger.warning(f"No table found with selector: {table_selector}")
                return pd.DataFrame()
            
            # Extract table data
            rows = table.find_all('tr')
            if not rows:
                logger.warning("No rows found in table")
                return pd.DataFrame()
            
            # Get headers
            header_row_data = rows[header_row]
            headers = [th.get_text(strip=True) for th in header_row_data.find_all(['th', 'td'])]
            
            # Get data rows
            data_rows = rows[header_row + 1:]
            data = []
            
            for row in data_rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) == len(headers):
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    data.append(row_data)
            
            df = pd.DataFrame(data, columns=headers)
            _validate_dataframe(df, f"Table scraping: {url}")
            
            logger.info(f"Successfully scraped table with {len(df)} rows")
            return df
            
        finally:
            scraper.close()
            
    except Exception as e:
        logger.error(f"Error scraping table data: {str(e)}")
        raise


def scrape_news_articles(url: str,
                        article_selector: str = 'article',
                        selectors: Optional[Dict[str, str]] = None,
                        max_pages: int = 5,
                        use_selenium: bool = False) -> pd.DataFrame:
    """
    Scrape news articles with common selectors.
    
    Args:
        url (str): Base URL of the news site
        article_selector (str): CSS selector for article containers
        selectors (dict, optional): Custom selectors for article fields
        max_pages (int): Maximum number of pages to scrape
        use_selenium (bool): Whether to use Selenium
    
    Returns:
        pd.DataFrame: Article data
    """
    try:
        # Default selectors for common news fields
        default_selectors = {
            'title': 'h1, h2, .title, .headline',
            'content': '.content, .article-content, .story, p',
            'author': '.author, .byline, .writer',
            'date': '.date, .published, .timestamp',
            'category': '.category, .section, .tag'
        }
        
        if selectors:
            default_selectors.update(selectors)
        
        # Add article container selector
        default_selectors['_container'] = article_selector
        
        logger.info(f"Scraping news articles from: {url}")
        
        return scrape_website(
            url=url,
            selectors=default_selectors,
            use_selenium=use_selenium,
            max_pages=max_pages,
            wait_for_element=article_selector
        )
        
    except Exception as e:
        logger.error(f"Error scraping news articles: {str(e)}")
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
    print("Testing web scraping data extraction...")
    
    # Create sample data
    import numpy as np
    
    mock_scraped_data = pd.DataFrame({
        'name': [
            'Wireless Bluetooth Headphones',
            'Smart Fitness Tracker',
            'Portable Phone Charger',
            'Ergonomic Office Chair',
            'LED Desk Lamp',
            'Mechanical Gaming Keyboard',
            'Wireless Mouse',
            'USB-C Hub',
            'Bluetooth Speaker',
            'Laptop Stand'
        ],
        'price': [
            '$79.99', '$129.99', '$24.99', '$199.99', '$45.99',
            '$89.99', '$29.99', '$39.99', '$59.99', '$34.99'
        ],
        'rating': [
            '4.5', '4.2', '4.7', '4.8', '4.3',
            '4.6', '4.4', '4.1', '4.5', '4.0'
        ],
        'availability': [
            'In Stock', 'In Stock', 'In Stock', 'Limited Stock', 'In Stock',
            'In Stock', 'In Stock', 'In Stock', 'In Stock', 'In Stock'
        ],
        'category': [
            'Electronics', 'Electronics', 'Electronics', 'Furniture', 'Electronics',
            'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Furniture'
        ],
        'description': [
            'High-quality wireless headphones with noise cancellation',
            'Advanced fitness tracker with heart rate monitoring',
            'Fast-charging portable power bank for mobile devices',
            'Comfortable ergonomic chair for long work sessions',
            'Adjustable LED desk lamp with multiple brightness levels',
            'Mechanical keyboard with RGB backlighting',
            'Wireless optical mouse with ergonomic design',
            'Multi-port USB-C hub for laptop connectivity',
            'Portable Bluetooth speaker with excellent sound quality',
            'Adjustable laptop stand for better ergonomics'
        ]
    })
    
    print("Mock scraped data created successfully!")
    print(f"Shape: {mock_scraped_data.shape}")
    print("Columns:", list(mock_scraped_data.columns))
    
    # Display the data
    print("\nScraped Data Preview:")
    print(mock_scraped_data.head())
    
    # Demonstrate different scraping scenarios
    print("\nWeb Scraping Examples:")
    
    # Example 1: Basic product scraping
    print("\n1. Basic product scraping:")
    print("Selectors used:")
    selectors_example = {
        'name': '.product-title',
        'price': '.price',
        'rating': '.rating',
        'availability': '.stock-status'
    }
    for field, selector in selectors_example.items():
        print(f"  {field}: {selector}")
    
    # Example 2: E-commerce scraping
    print("\n2. E-commerce product scraping:")
    print("Using scrape_ecommerce_products() with default selectors")
    print("Default selectors:")
    default_selectors = {
        'name': '.product-title, .product-name, h2, h3',
        'price': '.price, .product-price, .cost',
        'description': '.product-description, .description, p',
        'image': 'img',
        'rating': '.rating, .stars, .review-score',
        'availability': '.availability, .stock, .in-stock'
    }
    for field, selector in default_selectors.items():
        print(f"  {field}: {selector}")
    
    # Example 3: Table scraping
    print("\n3. Table data scraping:")
    print("Using scrape_table_data() for HTML tables")
    print("Common table selectors:")
    table_selectors = {
        'table': 'table',
        'rows': 'tr',
        'cells': 'td, th'
    }
    for element, selector in table_selectors.items():
        print(f"  {element}: {selector}")
    
    # Example 4: Data cleaning and processing
    print("\n4. Data cleaning and processing:")
    # Clean the scraped data
    cleaned_data = mock_scraped_data.copy()
    
    # Clean price data
    cleaned_data['price_numeric'] = cleaned_data['price'].str.replace('$', '').astype(float)
    
    # Clean rating data
    cleaned_data['rating_numeric'] = cleaned_data['rating'].astype(float)
    
    # Add availability status
    cleaned_data['in_stock'] = cleaned_data['availability'].apply(
        lambda x: True if 'In Stock' in x else False
    )
    
    print("Cleaned data with numeric fields:")
    print(cleaned_data[['name', 'price_numeric', 'rating_numeric', 'in_stock']].head())
    
    # Example 5: Data analysis
    print("\n5. Data analysis:")
    print("Price statistics:")
    price_stats = cleaned_data['price_numeric'].describe()
    print(price_stats)
    
    print("\nCategory distribution:")
    category_dist = cleaned_data['category'].value_counts()
    print(category_dist)
    
    print("\nAverage rating by category:")
    avg_rating = cleaned_data.groupby('category')['rating_numeric'].mean().round(2)
    print(avg_rating)
    
    print("\nReal Web Scraping Usage Example:")
    print("""
# For actual web scraping, use this pattern:

# Basic website scraping
selectors = {
    'title': 'h1',
    'content': '.content',
    'author': '.author',
    'date': '.date'
}

df = scrape_website(
    url='https://example.com/articles',
    selectors=selectors,
    max_pages=5,
    delay=1.0
)

# E-commerce product scraping
df_products = scrape_ecommerce_products(
    base_url='https://shop.example.com/products',
    product_selector='.product-item',
    max_pages=10,
    use_selenium=True  # For JavaScript-heavy sites
)

# Table data scraping
df_table = scrape_table_data(
    url='https://example.com/data-table',
    table_selector='#data-table',
    header_row=0
)

# Custom scraping with Selenium
df_js = scrape_website(
    url='https://spa.example.com',
    selectors={'data': '.dynamic-content'},
    use_selenium=True,
    wait_for_element='.dynamic-content'
)
""")
    
    print("\nWeb scraping demonstration completed successfully!")

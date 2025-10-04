# Web Scraping Data Extraction

This folder provides robust, production-ready functions for extracting data from websites into Pandas DataFrames. It supports both static and dynamic content scraping using BeautifulSoup and Selenium, with comprehensive error handling and data validation.

## Features

- **BeautifulSoup Support**: Fast scraping of static HTML content
- **Selenium Integration**: JavaScript-heavy sites and dynamic content
- **E-commerce Scraping**: Specialized functions for product data extraction
- **Table Extraction**: Direct HTML table to DataFrame conversion
- **News Article Scraping**: Common selectors for news and blog content
- **Pagination Support**: Multi-page scraping with configurable limits
- **Rate Limiting**: Respectful scraping with configurable delays
- **Data Validation**: Comprehensive quality checks and error handling
- **Memory Efficient**: Optimized for large-scale data extraction

## Classes

### `WebScraper`
Advanced web scraper with BeautifulSoup and Selenium support.

**`__init__(self, use_selenium: bool = False, headless: bool = True, user_agent: str = None, delay: float = 1.0)`**
- Initializes the scraper with configuration options
- `use_selenium`: Whether to use Selenium for JavaScript-heavy sites
- `headless`: Run browser in headless mode (Selenium only)
- `user_agent`: Custom user agent string
- `delay`: Delay between requests in seconds

**`get_page(self, url: str, wait_for_element: str = None) -> BeautifulSoup`**
- Retrieves and parses a web page
- `url`: URL to scrape
- `wait_for_element`: CSS selector to wait for (Selenium only)
- Returns: BeautifulSoup object with parsed HTML

**`close(self)`**
- Closes Selenium driver if open

## Functions

### `scrape_website(url: str, selectors: Dict[str, str], use_selenium: bool = False, max_pages: int = 1, delay: float = 1.0, wait_for_element: str = None) -> pd.DataFrame`
- **Description**: Scrapes data from a website using CSS selectors with support for pagination and dynamic content.
- **Parameters**:
  - `url` (str): Base URL to scrape
  - `selectors` (dict): Field name to CSS selector mapping (e.g., `{'title': 'h1', 'price': '.price'}`)
  - `use_selenium` (bool): Whether to use Selenium for JavaScript-heavy sites
  - `max_pages` (int): Maximum number of pages to scrape (default: 1)
  - `delay` (float): Delay between requests in seconds (default: 1.0)
  - `wait_for_element` (str, optional): CSS selector to wait for before extracting data (Selenium only)
- **Returns**: `pd.DataFrame`: Scraped data as a Pandas DataFrame
- **Raises**: `Exception`: For errors during scraping

### `scrape_ecommerce_products(base_url: str, product_selector: str = '.product', selectors: Optional[Dict[str, str]] = None, max_pages: int = 5, use_selenium: bool = False) -> pd.DataFrame`
- **Description**: Specialized function for scraping e-commerce product data with common selectors.
- **Parameters**:
  - `base_url` (str): Base URL of the e-commerce site
  - `product_selector` (str): CSS selector for product containers (default: '.product')
  - `selectors` (dict, optional): Custom selectors for product fields
  - `max_pages` (int): Maximum number of pages to scrape (default: 5)
  - `use_selenium` (bool): Whether to use Selenium for dynamic content
- **Returns**: `pd.DataFrame`: Product data with fields like name, price, rating, availability
- **Default Selectors**:
  - `name`: '.product-title, .product-name, h2, h3'
  - `price`: '.price, .product-price, .cost'
  - `description`: '.product-description, .description, p'
  - `image`: 'img'
  - `rating`: '.rating, .stars, .review-score'
  - `availability`: '.availability, .stock, .in-stock'

### `scrape_table_data(url: str, table_selector: str = 'table', header_row: int = 0, use_selenium: bool = False) -> pd.DataFrame`
- **Description**: Extracts tabular data from HTML tables on web pages.
- **Parameters**:
  - `url` (str): URL containing the table
  - `table_selector` (str): CSS selector for the table (default: 'table')
  - `header_row` (int): Row index to use as headers (default: 0)
  - `use_selenium` (bool): Whether to use Selenium for dynamic tables
- **Returns**: `pd.DataFrame`: Table data with proper column headers
- **Raises**: `Exception`: For errors during table extraction

### `scrape_news_articles(url: str, article_selector: str = 'article', selectors: Optional[Dict[str, str]] = None, max_pages: int = 5, use_selenium: bool = False) -> pd.DataFrame`
- **Description**: Scrapes news articles and blog posts with common selectors.
- **Parameters**:
  - `url` (str): Base URL of the news site
  - `article_selector` (str): CSS selector for article containers (default: 'article')
  - `selectors` (dict, optional): Custom selectors for article fields
  - `max_pages` (int): Maximum number of pages to scrape (default: 5)
  - `use_selenium` (bool): Whether to use Selenium for dynamic content
- **Returns**: `pd.DataFrame`: Article data with fields like title, content, author, date
- **Default Selectors**:
  - `title`: 'h1, h2, .title, .headline'
  - `content`: '.content, .article-content, .story, p'
  - `author`: '.author, .byline, .writer'
  - `date`: '.date, .published, .timestamp'
  - `category`: '.category, .section, .tag'

### `_extract_data_with_selectors(soup: BeautifulSoup, selectors: Dict[str, str]) -> List[Dict[str, Any]]`
- **Description**: Internal helper function to extract data from BeautifulSoup object using CSS selectors.

### `_validate_dataframe(df: pd.DataFrame, source: str) -> None`
- **Description**: Internal helper function to validate DataFrame for common data quality issues.

## Installation Requirements

```bash
# Basic web scraping
pip install requests beautifulsoup4

# For JavaScript-heavy sites
pip install selenium

# For Chrome WebDriver (Selenium)
# Download from: https://chromedriver.chromium.org/
# Or use: pip install webdriver-manager
```

## Usage Examples

### Basic Website Scraping

```python
from webscraping_extraction import scrape_website

# Define selectors for the data you want to extract
selectors = {
    'title': 'h1.article-title',
    'content': '.article-content',
    'author': '.author-name',
    'date': '.publish-date',
    'tags': '.tag-list a'
}

# Scrape the website
df = scrape_website(
    url='https://example.com/articles',
    selectors=selectors,
    max_pages=5,
    delay=1.0
)

print(f"Scraped {len(df)} articles")
print(df.head())
```

### E-commerce Product Scraping

```python
from webscraping_extraction import scrape_ecommerce_products

# Scrape products with default selectors
df_products = scrape_ecommerce_products(
    base_url='https://shop.example.com/products',
    product_selector='.product-item',
    max_pages=10
)

# Custom selectors for specific site
custom_selectors = {
    'name': '.product-title',
    'price': '.price-current',
    'rating': '.rating-stars',
    'reviews': '.review-count'
}

df_custom = scrape_ecommerce_products(
    base_url='https://shop.example.com/products',
    product_selector='.product-item',
    selectors=custom_selectors,
    max_pages=5
)

print(f"Scraped {len(df_products)} products")
print(df_products[['name', 'price', 'rating']].head())
```

### Table Data Extraction

```python
from webscraping_extraction import scrape_table_data

# Scrape data from HTML table
df_table = scrape_table_data(
    url='https://example.com/data-table',
    table_selector='#main-table',
    header_row=0
)

print(f"Scraped table with {len(df_table)} rows")
print(df_table.head())
```

### JavaScript-Heavy Sites (Selenium)

```python
from webscraping_extraction import scrape_website

# For sites that require JavaScript
df_js = scrape_website(
    url='https://spa.example.com',
    selectors={'data': '.dynamic-content'},
    use_selenium=True,
    wait_for_element='.dynamic-content',
    max_pages=3
)

print(f"Scraped {len(df_js)} items from JavaScript site")
```

### News Article Scraping

```python
from webscraping_extraction import scrape_news_articles

# Scrape news articles
df_news = scrape_news_articles(
    url='https://news.example.com',
    article_selector='.article',
    max_pages=5
)

print(f"Scraped {len(df_news)} articles")
print(df_news[['title', 'author', 'date']].head())
```

### Data Cleaning and Processing

```python
import pandas as pd
import re

# Clean scraped data
def clean_price(price_str):
    """Extract numeric price from string."""
    if pd.isna(price_str):
        return None
    # Remove currency symbols and extract number
    price_clean = re.sub(r'[^\d.,]', '', str(price_str))
    try:
        return float(price_clean.replace(',', ''))
    except:
        return None

def clean_rating(rating_str):
    """Extract numeric rating from string."""
    if pd.isna(rating_str):
        return None
    # Extract first number found
    match = re.search(r'(\d+\.?\d*)', str(rating_str))
    return float(match.group(1)) if match else None

# Apply cleaning functions
df_products['price_numeric'] = df_products['price'].apply(clean_price)
df_products['rating_numeric'] = df_products['rating'].apply(clean_rating)

# Remove rows with missing essential data
df_clean = df_products.dropna(subset=['name', 'price_numeric'])

print(f"Cleaned data: {len(df_clean)} products")
print(df_clean[['name', 'price_numeric', 'rating_numeric']].head())
```

### Advanced Usage with Custom Scraper

```python
from webscraping_extraction import WebScraper

# Create custom scraper instance
scraper = WebScraper(
    use_selenium=True,
    headless=True,
    delay=2.0,
    user_agent='Custom Bot 1.0'
)

try:
    # Get page content
    soup = scraper.get_page('https://example.com', wait_for_element='.content')
    
    # Custom extraction logic
    titles = [h2.get_text(strip=True) for h2 in soup.select('h2.title')]
    links = [a.get('href') for a in soup.select('a.more-link')]
    
    # Create DataFrame
    df_custom = pd.DataFrame({
        'title': titles,
        'link': links
    })
    
    print(f"Custom extraction: {len(df_custom)} items")
    
finally:
    scraper.close()
```

## Best Practices

### 1. Respectful Scraping
- Always add delays between requests (`delay` parameter)
- Use appropriate user agents
- Respect robots.txt files
- Don't overload servers with too many concurrent requests

### 2. Error Handling
- Always wrap scraping in try-except blocks
- Handle network timeouts and connection errors
- Validate scraped data before processing
- Log errors for debugging

### 3. Selector Strategy
- Use specific CSS selectors to avoid false matches
- Test selectors in browser developer tools first
- Have fallback selectors for different page layouts
- Use data attributes when available (e.g., `[data-product-id]`)

### 4. Performance Optimization
- Use BeautifulSoup for static content (faster)
- Use Selenium only when necessary (JavaScript-heavy sites)
- Implement pagination limits to avoid infinite loops
- Cache results when possible

### 5. Data Quality
- Always validate scraped data
- Handle missing values appropriately
- Clean and normalize data after scraping
- Remove duplicates and invalid entries

## Common Issues and Solutions

### 1. No Data Found
- Check if selectors are correct
- Verify the page loads properly
- Use browser developer tools to inspect elements
- Try different selectors or wait for elements to load

### 2. JavaScript Content Not Loading
- Use `use_selenium=True`
- Add `wait_for_element` parameter
- Increase delay between requests
- Check if the site requires authentication

### 3. Rate Limiting/Blocking
- Increase delay between requests
- Use different user agents
- Implement IP rotation if necessary
- Respect robots.txt and terms of service

### 4. Memory Issues with Large Datasets
- Process data in chunks
- Use pagination limits
- Clear browser cache (Selenium)
- Monitor memory usage

## Legal and Ethical Considerations

- **Respect robots.txt**: Check the site's robots.txt file
- **Terms of Service**: Review the website's terms of service
- **Rate Limiting**: Don't overload servers with requests
- **Data Usage**: Ensure you have permission to use scraped data
- **Copyright**: Respect copyright and intellectual property rights
- **Privacy**: Be mindful of personal data and privacy laws

## Troubleshooting

### Common Error Messages

1. **"requests and beautifulsoup4 not available"**
   - Solution: Install required packages: `pip install requests beautifulsoup4`

2. **"selenium not available"**
   - Solution: Install Selenium: `pip install selenium`

3. **"No elements found for selector"**
   - Solution: Check selector syntax and page structure

4. **"TimeoutException"**
   - Solution: Increase wait time or check network connection

5. **"ChromeDriver not found"**
   - Solution: Install ChromeDriver or use webdriver-manager

### Debugging Tips

1. **Test selectors in browser console**:
   ```javascript
   document.querySelectorAll('.your-selector')
   ```

2. **Check page source**:
   - Right-click → View Page Source
   - Look for the elements you're trying to scrape

3. **Use browser developer tools**:
   - Inspect elements to find correct selectors
   - Check for dynamic content loading

4. **Add logging**:
   - Enable debug logging to see what's happening
   - Log intermediate results

5. **Test with small samples**:
   - Start with `max_pages=1`
   - Test selectors on a single page first

This web scraping module provides a robust foundation for extracting data from websites while maintaining ethical scraping practices and handling common challenges in web data extraction.

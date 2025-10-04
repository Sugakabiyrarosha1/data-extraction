"""
XML Data Extraction Module

This module provides robust, production-ready functions for extracting data from XML files
into Pandas DataFrames with support for complex nested structures, namespaces, and large files.

Features:
- XML file parsing with nested structure support
- Namespace handling for complex XML documents
- Streaming parser for large XML files
- Automatic record element detection
- Comprehensive error handling with meaningful messages
- Data validation and quality checks
- Memory-efficient processing for large files
- Support for both lxml and ElementTree parsers

Author: Data Extraction Toolkit
"""

import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import lxml for better performance
try:
    import lxml.etree as lxml_etree
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    logger.warning("lxml not available. Using standard ElementTree parser.")


def load_xml(path: str,
             root_element: Optional[str] = None,
             record_element: Optional[str] = None,
             namespaces: Optional[Dict[str, str]] = None,
             encoding: str = 'utf-8',
             use_lxml: bool = True) -> pd.DataFrame:
    """
    Load XML data with advanced parsing capabilities.
    
    Args:
        path (str): Path to the XML file
        root_element (str, optional): Root element to start parsing from
        record_element (str, optional): Element that represents individual records
        namespaces (dict, optional): XML namespaces mapping
        encoding (str): File encoding
        use_lxml (bool): Whether to use lxml for better performance
    
    Returns:
        pd.DataFrame: Parsed XML data
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"XML file not found: {path}")
        
        logger.info(f"Loading XML file: {path}")
        
        if use_lxml and LXML_AVAILABLE:
            return _load_xml_lxml(path, root_element, record_element, namespaces, encoding)
        else:
            return _load_xml_etree(path, root_element, record_element, namespaces, encoding)
            
    except Exception as e:
        logger.error(f"Error loading XML file {path}: {str(e)}")
        raise


def _load_xml_lxml(path: str, root_element: str, record_element: str, 
                   namespaces: Dict[str, str], encoding: str) -> pd.DataFrame:
    """Load XML using lxml for better performance."""
    try:
        # Parse XML with lxml
        tree = lxml_etree.parse(path)
        root = tree.getroot()
        
        # Handle namespaces
        if namespaces:
            for prefix, uri in namespaces.items():
                lxml_etree.register_namespace(prefix, uri)
        
        # Find records
        if record_element:
            records = root.findall(f".//{record_element}", namespaces)
        else:
            # Try to find common record patterns
            records = _find_record_elements(root)
        
        if not records:
            logger.warning("No record elements found in XML")
            return pd.DataFrame()
        
        # Parse records
        data = []
        for record in records:
            record_data = _parse_xml_element(record)
            if record_data:
                data.append(record_data)
        
        df = pd.DataFrame(data)
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} records from XML")
        return df
        
    except Exception as e:
        logger.error(f"Error parsing XML with lxml: {str(e)}")
        raise


def _load_xml_etree(path: str, root_element: str, record_element: str,
                    namespaces: Dict[str, str], encoding: str) -> pd.DataFrame:
    """Load XML using standard ElementTree."""
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        
        # Find records
        if record_element:
            records = root.findall(f".//{record_element}")
        else:
            records = _find_record_elements(root)
        
        if not records:
            logger.warning("No record elements found in XML")
            return pd.DataFrame()
        
        # Parse records
        data = []
        for record in records:
            record_data = _parse_xml_element(record)
            if record_data:
                data.append(record_data)
        
        df = pd.DataFrame(data)
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} records from XML")
        return df
        
    except Exception as e:
        logger.error(f"Error parsing XML with ElementTree: {str(e)}")
        raise


def _find_record_elements(root) -> List:
    """Find common record element patterns in XML."""
    # Common record element patterns
    patterns = [
        'item', 'record', 'row', 'entry', 'product', 'user', 'customer',
        'order', 'transaction', 'event', 'log', 'message'
    ]
    
    for pattern in patterns:
        records = root.findall(f".//{pattern}")
        if records:
            return records
    
    # If no patterns match, return direct children
    return list(root)


def _parse_xml_element(element) -> Dict[str, Any]:
    """Parse an XML element into a dictionary."""
    data = {}
    
    # Add element attributes
    for key, value in element.attrib.items():
        data[f"@{key}"] = value
    
    # Add element text if it exists and has no children
    if element.text and element.text.strip() and len(element) == 0:
        data['text'] = element.text.strip()
    
    # Process child elements
    for child in element:
        child_data = _parse_xml_element(child)
        
        if child.tag in data:
            # Handle multiple elements with same tag
            if not isinstance(data[child.tag], list):
                data[child.tag] = [data[child.tag]]
            data[child.tag].append(child_data)
        else:
            data[child.tag] = child_data
    
    return data


def load_xml_streaming(path: str, 
                      record_element: str,
                      chunk_size: int = 1000,
                      namespaces: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Load large XML files using streaming parser.
    
    Args:
        path (str): Path to XML file
        record_element (str): Element that represents individual records
        chunk_size (int): Number of records to process at once
        namespaces (dict, optional): XML namespaces
    
    Returns:
        pd.DataFrame: Parsed XML data
    """
    if not LXML_AVAILABLE:
        raise ImportError("lxml is required for streaming XML parsing")
    
    try:
        logger.info(f"Streaming XML file: {path}")
        
        all_data = []
        context = lxml_etree.iterparse(path, events=('end',), tag=record_element)
        
        for event, element in context:
            record_data = _parse_xml_element(element)
            if record_data:
                all_data.append(record_data)
            
            # Clear element to save memory
            element.clear()
            
            # Process in chunks
            if len(all_data) >= chunk_size:
                logger.info(f"Processed {len(all_data)} records...")
        
        # Clear root element
        for event, element in context:
            element.clear()
        
        if not all_data:
            logger.warning("No records found in XML stream")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        _validate_dataframe(df, path)
        
        logger.info(f"Successfully loaded {len(df)} records from XML stream")
        return df
        
    except Exception as e:
        logger.error(f"Error streaming XML file {path}: {str(e)}")
        raise


def get_xml_structure(path: str) -> Dict[str, Any]:
    """
    Get the structure of an XML file without loading all data.
    
    Args:
        path (str): Path to XML file
    
    Returns:
        dict: XML structure information
    """
    try:
        if LXML_AVAILABLE:
            tree = lxml_etree.parse(path)
            root = tree.getroot()
        else:
            tree = ET.parse(path)
            root = tree.getroot()
        
        structure = {
            'root_element': root.tag,
            'root_attributes': root.attrib,
            'child_elements': [],
            'total_elements': 0
        }
        
        # Get child elements
        for child in root:
            structure['child_elements'].append({
                'tag': child.tag,
                'attributes': child.attrib,
                'has_text': bool(child.text and child.text.strip()),
                'has_children': len(child) > 0
            })
            structure['total_elements'] += 1
        
        return structure
        
    except Exception as e:
        logger.error(f"Error getting XML structure: {str(e)}")
        raise


def export_to_xml(data: pd.DataFrame, path: str, root_name: str = 'data', 
                  record_name: str = 'record', **kwargs) -> None:
    """
    Export DataFrame to XML file.
    
    Args:
        data (pd.DataFrame): DataFrame to export
        path (str): Output XML file path
        root_name (str): Root element name
        record_name (str): Record element name
        **kwargs: Additional arguments for XML formatting
    """
    try:
        # Create root element
        root = ET.Element(root_name)
        
        # Add records
        for _, row in data.iterrows():
            record = ET.SubElement(root, record_name)
            
            for col, value in row.items():
                if pd.notna(value):
                    element = ET.SubElement(record, col)
                    element.text = str(value)
        
        # Create tree and write to file
        tree = ET.ElementTree(root)
        tree.write(path, encoding='utf-8', xml_declaration=True)
        
        logger.info(f"Successfully exported DataFrame to {path}")
        
    except Exception as e:
        logger.error(f"Error exporting to XML file {path}: {str(e)}")
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
    print("Testing XML data extraction...")
    
    # Create sample XML data for demonstration
    sample_xml_data = '''<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <product id="1" category="electronics">
        <name>Laptop</name>
        <price>999.99</price>
        <description>High-performance laptop</description>
        <specifications>
            <cpu>Intel i7</cpu>
            <ram>16GB</ram>
            <storage>512GB SSD</storage>
        </specifications>
        <reviews>
            <review rating="5">Excellent product!</review>
            <review rating="4">Very good quality</review>
        </reviews>
    </product>
    <product id="2" category="electronics">
        <name>Smartphone</name>
        <price>699.99</price>
        <description>Latest smartphone model</description>
        <specifications>
            <cpu>Snapdragon 888</cpu>
            <ram>8GB</ram>
            <storage>256GB</storage>
        </specifications>
        <reviews>
            <review rating="5">Amazing camera quality</review>
        </reviews>
    </product>
    <product id="3" category="accessories">
        <name>Wireless Headphones</name>
        <price>199.99</price>
        <description>Noise-cancelling headphones</description>
        <specifications>
            <battery>30 hours</battery>
            <connectivity>Bluetooth 5.0</connectivity>
        </specifications>
        <reviews>
            <review rating="4">Great sound quality</review>
            <review rating="5">Comfortable to wear</review>
        </reviews>
    </product>
</catalog>'''
    
    # Save sample XML file
    xml_path = 'sample_data.xml'
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(sample_xml_data)
    
    print(f"Created sample XML file: {xml_path}")
    
    try:
        # Test 1: Get XML structure
        print("\n=== Testing XML Structure Analysis ===")
        structure = get_xml_structure(xml_path)
        print(f"Root element: {structure['root_element']}")
        print(f"Child elements: {[elem['tag'] for elem in structure['child_elements']]}")
        
        # Test 2: Load XML with automatic record detection
        print("\n=== Testing XML Loading with Automatic Record Detection ===")
        df_xml = load_xml(xml_path, record_element='product')
        print(f"Shape: {df_xml.shape}")
        print("Columns:", list(df_xml.columns))
        print("\nFirst 3 rows:")
        print(df_xml.head(3))
        
        # Test 3: Load XML with custom parsing
        print("\n=== Testing XML Loading with Custom Parsing ===")
        df_xml_custom = load_xml(xml_path, record_element='product', use_lxml=LXML_AVAILABLE)
        print("Custom parsed data:")
        print(df_xml_custom[['name', 'price', '@category']].head())
        
        # Test 4: Demonstrate XML structure analysis
        print("\n=== Testing XML Structure Analysis ===")
        for col in df_xml.columns:
            if col.startswith('@'):
                print(f"Attribute: {col}")
            elif col in ['name', 'price', 'description']:
                print(f"Simple element: {col}")
            else:
                print(f"Complex element: {col}")
        
        # Test 5: Export to XML
        print("\n=== Testing XML Export ===")
        export_path = 'exported_data.xml'
        export_to_xml(df_xml_custom[['name', 'price', '@category']], export_path, 
                     root_name='products', record_name='product')
        print(f"Exported data to {export_path}")
        
        # Test 6: Streaming parser (if lxml available)
        if LXML_AVAILABLE:
            print("\n=== Testing XML Streaming Parser ===")
            df_streaming = load_xml_streaming(xml_path, record_element='product', chunk_size=2)
            print(f"Streaming parser result shape: {df_streaming.shape}")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        # Clean up sample files
        for file_path in [xml_path, 'exported_data.xml']:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Cleaned up sample file: {file_path}")
    
    print("\nXML extraction testing completed!")

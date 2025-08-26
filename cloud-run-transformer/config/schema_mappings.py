"""
Schema mappings module that bridges the old schema.py and mappings.py files
with the new architecture expected by main.py
"""

import sys
import os

# Add the parent directory to the path to import the existing files
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from schema import SCHEMAS
    from mappings import MAPPINGS
except ImportError as e:
    print(f"Warning: Could not import existing schema/mappings files: {e}")
    SCHEMAS = {}
    MAPPINGS = {}

def get_collection_schema(collection_name):
    """
    Get PyArrow schema for a given collection.
    
    Args:
        collection_name (str): Name of the collection
        
    Returns:
        pyarrow.Schema or None: The schema for the collection
    """
    return SCHEMAS.get(collection_name)

def get_collection_mapping(collection_name):
    """
    Get field mappings for a given collection.
    
    Args:
        collection_name (str): Name of the collection
        
    Returns:
        dict or None: The mapping configuration for the collection
    """
    return MAPPINGS.get(collection_name)

def get_available_collections():
    """
    Get list of available collections that have both schema and mapping definitions.
    
    Returns:
        list: List of collection names
    """
    schema_collections = set(SCHEMAS.keys())
    mapping_collections = set(MAPPINGS.keys())
    
    # Return collections that have both schema and mapping
    available = list(schema_collections.intersection(mapping_collections))
    return available

def has_collection_support(collection_name):
    """
    Check if a collection has both schema and mapping support.
    
    Args:
        collection_name (str): Name of the collection
        
    Returns:
        bool: True if collection is supported
    """
    return (collection_name in SCHEMAS and 
            collection_name in MAPPINGS)
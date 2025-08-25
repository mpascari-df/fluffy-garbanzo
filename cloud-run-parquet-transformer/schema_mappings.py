import pyarrow as pa

# Import your actual schemas
from schema import SCHEMAS

def get_collection_schema(collection_name):
    """Get schema for collection, return None if not found"""
    return SCHEMAS.get(collection_name)

def get_available_collections():
    """Get list of available collections"""
    return list(SCHEMAS.keys())
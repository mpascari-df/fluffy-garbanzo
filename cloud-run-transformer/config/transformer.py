"""
Transformation module that applies field mappings and transformations
to convert raw MongoDB documents into the target schema format.
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add the parent directory to the path to import existing files
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from mappings import Literal
    from config.schema_mappings import get_collection_mapping
except ImportError as e:
    print(f"Warning: Could not import mappings: {e}")
    
    # Fallback Literal class if import fails
    class Literal:
        def __init__(self, value):
            self.value = value

def get_nested_value(obj, path):
    """
    Safely extract a value from a nested dictionary using dot notation.
    
    Args:
        obj (dict): Source object
        path (str): Dot-separated path (e.g., 'subscription.status')
        
    Returns:
        Any: The value at the path, or None if not found
    """
    if not path or not isinstance(obj, dict):
        return None
    
    keys = path.split('.')
    current = obj
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    
    return current

def apply_transformations(source_df, collection_name):
    """
    Apply field mappings and transformations to convert source DataFrame
    to target schema format.
    
    Args:
        source_df (pd.DataFrame): Source DataFrame with normalized JSON data
        collection_name (str): Name of the collection being processed
        
    Returns:
        pd.DataFrame: Transformed DataFrame matching target schema
    """
    mapping = get_collection_mapping(collection_name)
    if not mapping:
        print(f"No mapping found for collection: {collection_name}")
        return pd.DataFrame()
    
    # Initialize result dictionary to store transformed data
    result_data = {}
    
    # Process each row in the source DataFrame
    for idx, row in source_df.iterrows():
        # Convert row to dict for easier nested access
        row_dict = row.to_dict()
        
        # Apply each mapping rule
        for target_field, (source_spec, transform_func) in mapping.items():
            try:
                # Extract source value based on specification type
                if callable(source_spec):
                    # Lambda function (e.g., lambda: datetime.now().strftime("%Y%m"))
                    source_value = source_spec()
                elif isinstance(source_spec, Literal):
                    # Literal value
                    source_value = source_spec.value
                elif isinstance(source_spec, str):
                    # Field path - check both flattened and nested access
                    if source_spec in row_dict:
                        # Direct access for pandas normalized fields
                        source_value = row_dict[source_spec]
                    else:
                        # Try nested access on original document
                        # Reconstruct original document from normalized row
                        original_doc = {}
                        for col in row_dict:
                            if pd.notna(row_dict[col]):
                                # Handle nested field reconstruction
                                keys = col.split('.')
                                current = original_doc
                                for key in keys[:-1]:
                                    if key not in current:
                                        current[key] = {}
                                    current = current[key]
                                current[keys[-1]] = row_dict[col]
                        
                        source_value = get_nested_value(original_doc, source_spec)
                else:
                    source_value = None
                
                # Apply transformation function if provided
                if transform_func and source_value is not None:
                    transformed_value = transform_func(source_value)
                else:
                    transformed_value = source_value
                
                # Store in result
                if target_field not in result_data:
                    result_data[target_field] = []
                result_data[target_field].append(transformed_value)
                
            except Exception as e:
                print(f"Error processing field {target_field} for {source_spec}: {e}")
                # Add None for failed transformations
                if target_field not in result_data:
                    result_data[target_field] = []
                result_data[target_field].append(None)
    
    # Ensure all lists have the same length
    if result_data:
        max_length = max(len(values) for values in result_data.values())
        for field, values in result_data.items():
            while len(values) < max_length:
                values.append(None)
    
    # Convert to DataFrame
    transformed_df = pd.DataFrame(result_data)
    
    print(f"Transformed {len(source_df)} rows into {len(transformed_df)} rows for collection: {collection_name}")
    if not transformed_df.empty:
        print(f"Output columns: {list(transformed_df.columns)}")
    
    return transformed_df

def validate_transformation_result(df, schema):
    """
    Validate that the transformed DataFrame can be converted to the target schema.
    
    Args:
        df (pd.DataFrame): Transformed DataFrame
        schema (pyarrow.Schema): Target PyArrow schema
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if df.empty:
        return False, "DataFrame is empty"
    
    try:
        import pyarrow as pa
        
        # Check if all required schema fields are present
        schema_fields = set(field.name for field in schema)
        df_columns = set(df.columns)
        
        missing_fields = schema_fields - df_columns
        extra_fields = df_columns - schema_fields
        
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        
        if extra_fields:
            print(f"Warning: Extra fields will be ignored: {extra_fields}")
        
        # Try to create a PyArrow table to validate types
        table = pa.Table.from_pandas(df, schema=schema, safe=False)
        return True, "Validation successful"
        
    except Exception as e:
        return False, f"Schema validation failed: {e}"
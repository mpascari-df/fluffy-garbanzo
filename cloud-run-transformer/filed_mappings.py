import pandas as pd
import json
from datetime import datetime
from mappings import MAPPINGS, Literal

def get_nested_value(obj, path):
    """Get nested value using dot notation (e.g., 'trial.leadData.createdAt')"""
    if not path or obj is None:
        return None
    
    keys = path.split('.')
    current = obj
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    
    return current

def apply_transformations(df, collection_name, transformations=None):
    """Apply field transformations using your mapping configuration"""
    mapping = MAPPINGS.get(collection_name)
    if not mapping:
        print(f"No mapping found for collection: {collection_name}")
        return df
    
    # Create new DataFrame with transformed columns
    transformed_data = {}
    
    for index, row in df.iterrows():
        row_data = {}
        
        for target_field, (source_spec, transform_func) in mapping.items():
            value = None
            
            # Handle different source specifications
            if isinstance(source_spec, Literal):
                value = source_spec.value
            elif callable(source_spec):
                value = source_spec()
            elif isinstance(source_spec, str):
                if '.' in source_spec:
                    # Handle nested fields
                    value = get_nested_value(row.to_dict(), source_spec)
                else:
                    # Direct field access
                    value = row.get(source_spec)
            
            # Apply transformation function if specified
            if transform_func and value is not None:
                try:
                    value = transform_func(value)
                except Exception as e:
                    print(f"Warning: Transform failed for {target_field}: {e}")
                    value = None
            
            row_data[target_field] = value
        
        # Store row data
        for field, val in row_data.items():
            if field not in transformed_data:
                transformed_data[field] = []
            transformed_data[field].append(val)
    
    return pd.DataFrame(transformed_data)
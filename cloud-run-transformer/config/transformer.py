"""
Transformation module that applies field mappings and transformations
to convert raw MongoDB documents into the target schema format.
UPDATED: Added resilient validation that logs issues but continues processing.
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
    Validate and auto-fix DataFrame to match target schema.
    Logs issues but continues processing - prioritizing data capture over perfection.
    
    Args:
        df (pd.DataFrame): Transformed DataFrame
        schema (pyarrow.Schema): Target PyArrow schema
        
    Returns:
        tuple: (is_valid, fixed_df, warning_messages)
    """
    if df.empty:
        return False, df, ["DataFrame is empty"]
    
    warnings = []
    
    try:
        import pyarrow as pa
        
        # Create a copy to avoid modifying the original
        fixed_df = df.copy()
        
        schema_fields = {field.name: field for field in schema}
        df_columns = set(fixed_df.columns)
        
        # Handle missing required fields - add with None values
        missing_fields = set(schema_fields.keys()) - df_columns
        if missing_fields:
            for field_name in missing_fields:
                fixed_df[field_name] = None
                field_type = schema_fields[field_name].type
                warnings.append(f"SCHEMA_DRIFT: Added missing field '{field_name}' ({field_type}) with None")
        
        # Log extra fields that will be dropped
        extra_fields = df_columns - set(schema_fields.keys())
        if extra_fields:
            warnings.append(f"SCHEMA_DRIFT: Dropping unexpected fields: {extra_fields}")
        
        # Reorder columns to match schema exactly and drop extra fields
        fixed_df = fixed_df[[field.name for field in schema]]
        
        # Try to create PyArrow table with safe casting first
        validation_success = False
        try:
            table = pa.Table.from_pandas(fixed_df, schema=schema, safe=True)
            validation_success = True
            
            # Log any data quality issues
            for col_idx, field in enumerate(schema):
                col = table.column(col_idx)
                if col.null_count > 0:
                    null_pct = (col.null_count / len(col)) * 100
                    if null_pct > 50:
                        warnings.append(f"DATA_QUALITY: Field '{field.name}' has {null_pct:.1f}% null values")
                        
        except pa.lib.ArrowTypeError as e:
            # Type mismatch - try to fix with lossy casting
            warnings.append(f"TYPE_MISMATCH: {str(e)[:200]}")  # Truncate long error messages
            
            try:
                # Try with safe=False to allow lossy casts
                table = pa.Table.from_pandas(fixed_df, schema=schema, safe=False)
                warnings.append("Applied lossy type casting to conform to schema")
                validation_success = True
            except Exception as cast_error:
                # Even lossy casting failed - try to fix column by column
                warnings.append(f"LOSSY_CAST_FAILED: {str(cast_error)[:200]}")
                
                # Identify and fix problematic columns
                for field in schema:
                    if field.name in fixed_df.columns:
                        try:
                            # Try to cast this specific column
                            test_series = fixed_df[field.name]
                            if field.type == pa.string():
                                fixed_df[field.name] = test_series.astype(str, errors='ignore')
                            elif field.type == pa.int64():
                                fixed_df[field.name] = pd.to_numeric(test_series, errors='coerce')
                            elif field.type == pa.float64():
                                fixed_df[field.name] = pd.to_numeric(test_series, errors='coerce')
                            elif field.type == pa.bool_():
                                fixed_df[field.name] = test_series.astype(bool, errors='ignore')
                            # Add more type handling as needed
                        except Exception as col_error:
                            # If casting fails, set to None
                            warnings.append(f"COLUMN_CAST_FAILED: '{field.name}' - setting to None")
                            fixed_df[field.name] = None
                
                # Try one more time after individual column fixes
                try:
                    table = pa.Table.from_pandas(fixed_df, schema=schema, safe=False)
                    warnings.append("Fixed problematic columns individually")
                    validation_success = True
                except Exception as final_error:
                    warnings.append(f"CRITICAL: Final validation failed: {str(final_error)[:200]}")
                    validation_success = False
        
        if warnings:
            print(f"[SCHEMA_VALIDATION] Warnings detected: {len(warnings)}")
            for warning in warnings[:10]:  # Limit console output to first 10 warnings
                print(f"  - {warning}")
            if len(warnings) > 10:
                print(f"  ... and {len(warnings) - 10} more warnings")
        else:
            print("[SCHEMA_VALIDATION] Clean validation - no issues detected")
        
        return validation_success, fixed_df, warnings
        
    except Exception as e:
        error_msg = f"Critical validation error: {str(e)[:500]}"
        print(f"[SCHEMA_VALIDATION] {error_msg}")
        return False, df, [error_msg]
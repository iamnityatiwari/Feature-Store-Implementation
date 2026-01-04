"""Feature computation logic."""
import pandas as pd
from sqlalchemy.orm import Session
from typing import Dict, Any, List
import json
import hashlib


def compute_feature(
    db: Session,
    feature_id: int,
    version: str,
    raw_data: pd.DataFrame,
    computation_logic: str
) -> pd.Series:
    """
    Compute feature values from raw data using computation logic.
    
    Args:
        db: Database session
        feature_id: ID of the feature
        version: Version string
        raw_data: DataFrame containing raw data
        computation_logic: Python code or SQL query to compute feature
        
    Returns:
        Series with computed feature values indexed by entity_id
    """
    # Create a safe execution environment
    safe_dict = {
        'pd': pd,
        'df': raw_data,
        'raw_data': raw_data,
    }
    
    try:
        # Execute computation logic
        exec(computation_logic, {"__builtins__": {}}, safe_dict)
        
        # Expect result to be in 'result' variable
        if 'result' not in safe_dict:
            raise ValueError("Computation logic must assign result to 'result' variable")
        
        result = safe_dict['result']
        
        # Ensure result is a Series with entity_id as index
        if isinstance(result, pd.Series):
            return result
        elif isinstance(result, pd.DataFrame):
            if len(result.columns) == 1:
                return result.iloc[:, 0]
            else:
                raise ValueError("Computation must return a single column")
        else:
            raise ValueError("Computation must return a pandas Series or DataFrame")
            
    except Exception as e:
        raise ValueError(f"Error computing feature: {str(e)}")


def store_feature_values(
    db: Session,
    feature_version_id: int,
    feature_values: pd.Series
):
    """
    Store computed feature values in the database.
    
    Args:
        db: Database session
        feature_version_id: ID of the feature version
        feature_values: Series with entity_id as index and feature values
    """
    for entity_id, value in feature_values.items():
        # Convert value to string (can be JSON for complex types)
        if isinstance(value, (dict, list)):
            value_str = json.dumps(value)
        else:
            value_str = str(value)
        
        feature_value = FeatureValue(
            feature_version_id=feature_version_id,
            entity_id=str(entity_id),
            value=value_str
        )
        db.add(feature_value)
    
    db.commit()


def validate_raw_data_schema(raw_data: pd.DataFrame, schema_definition: Dict[str, Any]) -> bool:
    """
    Validate that raw data matches the schema definition.
    
    Args:
        raw_data: DataFrame to validate
        schema_definition: Expected schema with column names and types
        
    Returns:
        True if valid, raises ValueError if invalid
    """
    required_columns = schema_definition.get('required_columns', [])
    column_types = schema_definition.get('column_types', {})
    
    # Check required columns exist
    missing_columns = set(required_columns) - set(raw_data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check column types (basic validation)
    for col, expected_type in column_types.items():
        if col in raw_data.columns:
            actual_type = str(raw_data[col].dtype)
            # Basic type checking (can be enhanced)
            if expected_type == 'numeric' and not pd.api.types.is_numeric_dtype(raw_data[col]):
                raise ValueError(f"Column {col} expected numeric type, got {actual_type}")
            elif expected_type == 'string' and not pd.api.types.is_string_dtype(raw_data[col]):
                # Check if it's object type (which can contain strings)
                if not pd.api.types.is_object_dtype(raw_data[col]):
                    raise ValueError(f"Column {col} expected string type, got {actual_type}")
    
    return True


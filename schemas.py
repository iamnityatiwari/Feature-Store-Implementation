"""Pydantic schemas for API request/response validation."""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class RawTableCreate(BaseModel):
    """Schema for creating a raw table."""
    name: str = Field(..., description="Name of the raw table")
    description: Optional[str] = Field(None, description="Description of the raw table")
    schema_definition: Dict[str, Any] = Field(..., description="JSON schema defining columns and types")


class RawTableResponse(BaseModel):
    """Schema for raw table response."""
    id: int
    name: str
    description: Optional[str]
    schema_definition: Dict[str, Any]
    created_at: datetime
    updated_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class FeatureCreate(BaseModel):
    """Schema for creating a feature."""
    name: str = Field(..., description="Name of the feature")
    description: Optional[str] = Field(None, description="Description of the feature")
    raw_table_id: int = Field(..., description="ID of the raw table")
    computation_logic: str = Field(..., description="Python code or SQL query to compute the feature")
    feature_type: str = Field(..., description="Type of feature: numeric, categorical, text, etc.")


class FeatureResponse(BaseModel):
    """Schema for feature response."""
    id: int
    name: str
    description: Optional[str]
    raw_table_id: int
    computation_logic: str
    feature_type: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class FeatureVersionCreate(BaseModel):
    """Schema for creating a feature version."""
    feature_id: int = Field(..., description="ID of the feature")
    version: str = Field(..., description="Version string (e.g., 'v1.0')")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class FeatureVersionCompute(BaseModel):
    """Schema for computing a feature version with raw data."""
    version: str = Field(..., description="Version string (e.g., 'v1.0')")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    data: List[Dict[str, Any]] = Field(..., description="Array of raw data records")
    entity_id_column: str = Field(default="id", description="Column name containing entity IDs")


class FeatureVersionResponse(BaseModel):
    """Schema for feature version response."""
    id: int
    feature_id: int
    version: str
    status: str
    computed_at: datetime
    metadata: Optional[Dict[str, Any]] = Field(None, alias="version_metadata")
    
    class Config:
        from_attributes = True
        populate_by_name = True


class FeatureVectorRequest(BaseModel):
    """Schema for requesting feature vectors."""
    entity_id: str = Field(..., description="ID of the entity")
    feature_names: Optional[List[str]] = Field(None, description="Specific features to retrieve (all if None)")
    version: Optional[str] = Field(None, description="Specific version to retrieve (latest if None)")


class FeatureVectorResponse(BaseModel):
    """Schema for feature vector response."""
    entity_id: str
    features: Dict[str, Any] = Field(..., description="Dictionary of feature name to value")
    retrieved_at: datetime


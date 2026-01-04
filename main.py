"""Main FastAPI application for the feature store."""
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import pandas as pd
import json
from datetime import datetime

from .database import get_db, init_db
from .models import RawTable, Feature, FeatureVersion, FeatureValue
from .schemas import (
    RawTableCreate, RawTableResponse,
    FeatureCreate, FeatureResponse,
    FeatureVersionCreate, FeatureVersionResponse,
    FeatureVersionCompute,
    FeatureVectorRequest, FeatureVectorResponse
)
from .compute import compute_feature, store_feature_values, validate_raw_data_schema
from .cache import feature_cache

app = FastAPI(
    title="Feature Store API",
    description="A simple feature store service for registering raw tables, computing features, and serving feature vectors",
    version="1.0.0"
)


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()


# ==================== Raw Table Endpoints ====================

@app.post("/api/v1/raw-tables", response_model=RawTableResponse, status_code=status.HTTP_201_CREATED)
def register_raw_table(raw_table: RawTableCreate, db: Session = Depends(get_db)):
    """
    Register a new raw table.
    
    - **name**: Unique name for the raw table
    - **description**: Optional description
    - **schema_definition**: JSON schema with 'required_columns' and 'column_types'
    """
    # Check if table already exists
    existing = db.query(RawTable).filter(RawTable.name == raw_table.name).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Raw table with name '{raw_table.name}' already exists"
        )
    
    db_raw_table = RawTable(
        name=raw_table.name,
        description=raw_table.description,
        schema_definition=raw_table.schema_definition
    )
    db.add(db_raw_table)
    db.commit()
    db.refresh(db_raw_table)
    
    return db_raw_table


@app.get("/api/v1/raw-tables", response_model=List[RawTableResponse])
def list_raw_tables(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """List all registered raw tables."""
    raw_tables = db.query(RawTable).offset(skip).limit(limit).all()
    return raw_tables


@app.get("/api/v1/raw-tables/{table_id}", response_model=RawTableResponse)
def get_raw_table(table_id: int, db: Session = Depends(get_db)):
    """Get a specific raw table by ID."""
    raw_table = db.query(RawTable).filter(RawTable.id == table_id).first()
    if not raw_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Raw table with ID {table_id} not found"
        )
    return raw_table


# ==================== Feature Endpoints ====================

@app.post("/api/v1/features", response_model=FeatureResponse, status_code=status.HTTP_201_CREATED)
def create_feature(feature: FeatureCreate, db: Session = Depends(get_db)):
    """
    Create a new feature definition.
    
    - **name**: Name of the feature
    - **raw_table_id**: ID of the raw table to compute from
    - **computation_logic**: Python code that computes the feature (must assign to 'result')
    - **feature_type**: Type of feature (numeric, categorical, text, etc.)
    """
    # Validate raw table exists
    raw_table = db.query(RawTable).filter(RawTable.id == feature.raw_table_id).first()
    if not raw_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Raw table with ID {feature.raw_table_id} not found"
        )
    
    db_feature = Feature(
        name=feature.name,
        description=feature.description,
        raw_table_id=feature.raw_table_id,
        computation_logic=feature.computation_logic,
        feature_type=feature.feature_type
    )
    db.add(db_feature)
    db.commit()
    db.refresh(db_feature)
    
    return db_feature


@app.get("/api/v1/features", response_model=List[FeatureResponse])
def list_features(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """List all feature definitions."""
    features = db.query(Feature).offset(skip).limit(limit).all()
    return features


@app.get("/api/v1/features/{feature_id}", response_model=FeatureResponse)
def get_feature(feature_id: int, db: Session = Depends(get_db)):
    """Get a specific feature by ID."""
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    return feature


# ==================== Feature Version Endpoints ====================

@app.post("/api/v1/features/{feature_id}/versions", response_model=FeatureVersionResponse, status_code=status.HTTP_201_CREATED)
def compute_feature_version(
    feature_id: int,
    request: FeatureVersionCompute,
    db: Session = Depends(get_db)
):
    """
    Compute and store a new version of a feature.
    
    - **feature_id**: ID of the feature (from path)
    - **version**: Version string (e.g., 'v1.0')
    - **data**: Array of raw data records
    - **entity_id_column**: Column name containing entity IDs (default: 'id')
    - **metadata**: Optional metadata about this version
    """
    # Validate feature exists
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    
    # Check if version already exists
    existing_version = db.query(FeatureVersion).filter(
        FeatureVersion.feature_id == feature_id,
        FeatureVersion.version == request.version
    ).first()
    if existing_version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Version '{request.version}' already exists for feature {feature_id}"
        )
    
    # Convert raw data to DataFrame
    try:
        df = pd.DataFrame(request.data)
        
        if request.entity_id_column not in df.columns:
            raise ValueError(f"Entity ID column '{request.entity_id_column}' not found in data")
        
        # Set entity_id as index
        df = df.set_index(request.entity_id_column)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid raw data format: {str(e)}"
        )
    
    # Validate schema
    try:
        validate_raw_data_schema(df, feature.raw_table.schema_definition)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Schema validation failed: {str(e)}"
        )
    
    # Compute feature
    try:
        feature_values = compute_feature(db, feature_id, request.version, df, feature.computation_logic)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Feature computation failed: {str(e)}"
        )
    
    # Create feature version
    db_version = FeatureVersion(
        feature_id=feature_id,
        version=request.version,
        status="active",
        version_metadata=request.metadata
    )
    db.add(db_version)
    db.commit()
    db.refresh(db_version)
    
    # Store feature values
    try:
        store_feature_values(db, db_version.id, feature_values)
    except Exception as e:
        # Rollback version creation if storing values fails
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store feature values: {str(e)}"
        )
    
    return db_version


@app.get("/api/v1/features/{feature_id}/versions", response_model=List[FeatureVersionResponse])
def list_feature_versions(feature_id: int, db: Session = Depends(get_db)):
    """List all versions of a feature."""
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    
    versions = db.query(FeatureVersion).filter(FeatureVersion.feature_id == feature_id).all()
    return versions


# ==================== Feature Vector Serving ====================

@app.post("/api/v1/feature-vectors", response_model=FeatureVectorResponse)
def get_feature_vector(request: FeatureVectorRequest, db: Session = Depends(get_db)):
    """
    Retrieve feature vector for a given entity.
    
    - **entity_id**: ID of the entity
    - **feature_names**: Optional list of specific features to retrieve (all if None)
    - **version**: Optional version string (latest if None)
    """
    # Check cache first
    cached = feature_cache.get(request.entity_id, request.feature_names, request.version)
    if cached:
        return FeatureVectorResponse(
            entity_id=request.entity_id,
            features=cached,
            retrieved_at=datetime.now()
        )
    
    # Build query
    if request.version:
        # Specific version requested
        query = db.query(FeatureValue, FeatureVersion, Feature).join(
            FeatureVersion, FeatureValue.feature_version_id == FeatureVersion.id
        ).join(
            Feature, FeatureVersion.feature_id == Feature.id
        ).filter(
            FeatureValue.entity_id == request.entity_id,
            FeatureVersion.version == request.version
        )
        
        if request.feature_names:
            query = query.filter(Feature.name.in_(request.feature_names))
        
        results = query.all()
    else:
        # Get latest version for each feature
        # First, get all features (filtered by name if specified)
        if request.feature_names:
            features_query = db.query(Feature).filter(Feature.name.in_(request.feature_names))
        else:
            features_query = db.query(Feature)
        
        features = features_query.all()
        
        # For each feature, get the latest version and its value for this entity
        results = []
        for feature in features:
            # Get latest version (by computed_at timestamp)
            latest_version = db.query(FeatureVersion).filter(
                FeatureVersion.feature_id == feature.id,
                FeatureVersion.status == "active"
            ).order_by(FeatureVersion.computed_at.desc()).first()
            
            if latest_version:
                # Get feature value for this entity and version
                feature_value = db.query(FeatureValue).filter(
                    FeatureValue.feature_version_id == latest_version.id,
                    FeatureValue.entity_id == request.entity_id
                ).first()
                
                if feature_value:
                    results.append((feature_value, latest_version, feature))
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No feature vectors found for entity '{request.entity_id}'"
        )
    
    # Build feature vector
    feature_vector = {}
    for feature_value, feature_version, feature in results:
        # Parse value (could be JSON)
        try:
            value = json.loads(feature_value.value)
        except (json.JSONDecodeError, TypeError):
            value = feature_value.value
        
        feature_vector[feature.name] = value
    
    # Cache the result
    feature_cache.set(request.entity_id, feature_vector, request.feature_names, request.version)
    
    return FeatureVectorResponse(
        entity_id=request.entity_id,
        features=feature_vector,
        retrieved_at=datetime.now()
    )


@app.get("/api/v1/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "feature-store"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


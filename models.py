"""Database models for the feature store."""
from sqlalchemy import Column, Integer, String, DateTime, Float, Text, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base
import json


class RawTable(Base):
    """Model for registered raw tables."""
    __tablename__ = "raw_tables"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    schema_definition = Column(JSON, nullable=False)  # JSON schema of columns
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    features = relationship("Feature", back_populates="raw_table")


class Feature(Base):
    """Model for feature definitions."""
    __tablename__ = "features"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    raw_table_id = Column(Integer, ForeignKey("raw_tables.id"), nullable=False)
    computation_logic = Column(Text, nullable=False)  # Python code or SQL query
    feature_type = Column(String(50), nullable=False)  # 'numeric', 'categorical', 'text', etc.
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    raw_table = relationship("RawTable", back_populates="features")
    versions = relationship("FeatureVersion", back_populates="feature", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_feature_name', 'name'),
    )


class FeatureVersion(Base):
    """Model for feature versions."""
    __tablename__ = "feature_versions"
    
    id = Column(Integer, primary_key=True, index=True)
    feature_id = Column(Integer, ForeignKey("features.id"), nullable=False)
    version = Column(String(50), nullable=False)  # e.g., "v1.0", "v1.1"
    status = Column(String(50), default="active")  # 'active', 'deprecated', 'archived'
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
    version_metadata = Column(JSON, nullable=True)  # Additional metadata about the version
    
    # Relationships
    feature = relationship("Feature", back_populates="versions")
    feature_values = relationship("FeatureValue", back_populates="feature_version", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_feature_version', 'feature_id', 'version'),
    )


class FeatureValue(Base):
    """Model for storing computed feature values."""
    __tablename__ = "feature_values"
    
    id = Column(Integer, primary_key=True, index=True)
    feature_version_id = Column(Integer, ForeignKey("feature_versions.id"), nullable=False)
    entity_id = Column(String(255), nullable=False, index=True)  # ID of the entity (user, product, etc.)
    value = Column(Text, nullable=False)  # Stored as text, can be JSON for complex types
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    feature_version = relationship("FeatureVersion", back_populates="feature_values")
    
    __table_args__ = (
        Index('idx_entity_feature', 'entity_id', 'feature_version_id'),
    )


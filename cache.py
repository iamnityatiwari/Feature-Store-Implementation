"""Caching mechanism for feature vectors."""
from cachetools import TTLCache
from typing import Dict, Any, Optional
import hashlib
import json
from datetime import datetime


class FeatureCache:
    """Simple TTL-based cache for feature vectors."""
    
    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        """
        Initialize cache.
        
        Args:
            maxsize: Maximum number of cached items
            ttl: Time to live in seconds (default 1 hour)
        """
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
    
    def _make_key(self, entity_id: str, feature_names: Optional[list], version: Optional[str]) -> str:
        """Generate cache key from request parameters."""
        key_parts = [entity_id]
        if feature_names:
            key_parts.append(','.join(sorted(feature_names)))
        if version:
            key_parts.append(version)
        key_str = '|'.join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, entity_id: str, feature_names: Optional[list] = None, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get cached feature vector."""
        key = self._make_key(entity_id, feature_names, version)
        return self.cache.get(key)
    
    def set(self, entity_id: str, features: Dict[str, Any], feature_names: Optional[list] = None, version: Optional[str] = None):
        """Cache feature vector."""
        key = self._make_key(entity_id, feature_names, version)
        self.cache[key] = features
    
    def clear(self):
        """Clear all cached items."""
        self.cache.clear()


# Global cache instance
feature_cache = FeatureCache(maxsize=1000, ttl=3600)


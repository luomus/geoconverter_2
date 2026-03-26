import os
from typing import Optional, Dict
import settings
from dataclasses import dataclass
from threading import Lock
from time import time
from threading import Lock


CRS_MAPPING = {"euref": "EPSG:3067", "wgs84": "EPSG:4326"}
GEOMETRY_TYPE_MAPPING = {"point": "points", "bbox": "bbox", "footprint": "original"}
GEOMETRY_LANG_MAPPING = {"fi": "WGS84 geometria", "en": "Footprint WKT", "tech": "footprintWKT"}
LANGUAGE_MAPPING = {"tech": 0, "fi": 1, "en": 2}
LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = "100MB"
app_settings = settings.Settings()
write_lock = Lock()

class ConversionStatusManager:
    """Thread-safe manager for conversion status tracking."""
    def __init__(self):
        self._statuses: Dict = {}
        self._lock = Lock()
    
    def update(self, conversion_id: str, status: str, **kwargs) -> None:
        with self._lock:
            existing = self._statuses.get(conversion_id, {})
            self._statuses[conversion_id] = {
                **existing,
                "status": status,
                "timestamp": time(),
                "progress_percent": kwargs.pop("progress_percent", existing.get("progress_percent", 0)),
                **kwargs
            }
    
    def get(self, conversion_id: str) -> dict:
        with self._lock:
            return self._statuses.get(conversion_id, {})
    
    def has(self, conversion_id: str) -> bool:
        with self._lock:
            return conversion_id in self._statuses
    
    def get_status_value(self, conversion_id: str) -> Optional[str]:
        with self._lock:
            return self._statuses.get(conversion_id, {}).get("status")
    
    def get_all(self) -> Dict:
        with self._lock:
            return self._statuses.copy()
    
    def remove(self, conversion_id: str) -> None:
        with self._lock:
            if conversion_id in self._statuses:
                del self._statuses[conversion_id]

@dataclass
class ConversionJob:
    """Bundle of parameters for a conversion job."""
    conversion_id: str
    input_path: str
    language: str
    geo_type: str
    crs: str
    is_user_upload: bool
    original_filename: Optional[str] = None
    
    @property
    def wkt_column(self) -> str:
        """Get the WKT column name based on language."""
        return GEOMETRY_LANG_MAPPING[self.language]
    
    @property
    def output_gpkg(self) -> str:
        """Get the output GeoPackage path."""
        conversion_id_clean = self.conversion_id.replace('.', '_')
        return os.path.join(app_settings.OUTPUT_PATH, f'{conversion_id_clean}.gpkg')
    
    @property
    def mapped_crs(self) -> str:
        """Get the mapped CRS value."""
        return CRS_MAPPING[self.crs]
    
    @property
    def mapped_geo_type(self) -> str:
        """Get the mapped geometry type."""
        return GEOMETRY_TYPE_MAPPING[self.geo_type]

# Global singleton instance for thread-safe status tracking
_status_manager = ConversionStatusManager()
    
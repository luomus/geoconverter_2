import pandas as pd
from shapely.errors import ShapelyError
from shapely.geometry import (
    GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon
)
from shapely.wkt import loads
import logging
from collections import defaultdict
from typing import Optional, Any, Dict
import numpy as np
import geopandas as gpd
import settings
import dask.dataframe as dd
import os
from threading import Lock
from time import time

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
                "progress_percent": kwargs.pop("progress_percent", 0),
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

status_manager = ConversionStatusManager()
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

logging.basicConfig(
    level=log_level, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

GEOMETRY_BUFFER_DISTANCE = 0.00001  # ~0.5 meters

def safely_parse_wkt(wkt_string: str) -> Optional[Any]:
    """Safely convert WKT string to Shapely geometry."""
    if pd.isna(wkt_string) or not isinstance(wkt_string, str) or not wkt_string:
        return None
    
    try:
        geom = loads(wkt_string)
        if geom is None or geom.is_empty or not geom.is_valid:
            logging.warning(f"Invalid or empty geometry for WKT: {wkt_string[:50]}...")
            return None
        
        # Validate bounds
        try:
            bounds = geom.bounds
            if not bounds or len(bounds) != 4:
                return None
            if any(not np.isfinite(coord) for coord in bounds):
                logging.warning(f"Geometry with infinite coordinates found: {wkt_string[:50]}...")
                return None
        except Exception:
            return None
        return geom
            
    except (ShapelyError, Exception) as e:
        #logging.warning(f"Failed to convert WKT '{wkt_string}': {e}")
        return None
    

def process_wkt_geometry(ddf: dd.DataFrame, wkt_column: str) -> dd.DataFrame:
    ddf = ddf[ddf[wkt_column].notnull()]  # Remove NA values #TODO: Maybe better to keep them in the future
    ddf = ddf[ddf[wkt_column].str.strip() != ""]  # Remove empty strings and whitespace

    logging.debug(f"Removed rows with null or empty WKT in column {wkt_column}")

    ddf["geometry"] = ddf[wkt_column].map(safely_parse_wkt)
    ddf = ddf.set_geometry("geometry")
    ddf["geometry"] = ddf["geometry"].apply(normalize_geometry_collection)
    return ddf

def convert_boolean_value(value: str) -> Optional[bool]:
    """Convert string boolean values to Python boolean."""
    if not isinstance(value, str):
        return None
        
    lower_value = value.lower()
    if lower_value in ['true', '1']:
        return True
    elif lower_value in ['false', '0']:
        return False
    else:
        return None


def convert_numeric_with_na(value: str) -> Optional[float]:
    """Convert string numeric values to float, handling NA values."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def normalize_geometry_collection(geometry: Any) -> Any:
    """Process a Shapely GeometryCollection to convert it into a more specific geometry type."""
    if not isinstance(geometry, GeometryCollection):
        return geometry
    
    geometries = list(geometry.geoms)
    
    # Handle single geometry collections
    if len(geometries) == 1:
        return geometries[0]
    
    # Determine geometry types in the collection
    geometry_types = {type(geom) for geom in geometries}
    
    # Convert homogeneous collections to appropriate Multi* types
    if geometry_types == {LineString}:
        return MultiLineString(geometries)
    elif geometry_types == {Point}:
        return MultiPoint(geometries)
    elif geometry_types == {Polygon}:
        return MultiPolygon(geometries)
    elif geometry_types == {MultiLineString}:
        return MultiLineString([g for geom in geometries for g in geom.geoms])
    elif geometry_types == {MultiPoint}:
        return MultiPoint([g for geom in geometries for g in geom.geoms])
    elif geometry_types == {MultiPolygon}:
        return MultiPolygon([g for geom in geometries for g in geom.geoms])
    
    # Handle mixed geometry types by buffering and dissolving
    return buffer_and_dissolve_mixed_geometries(geometries)


def buffer_and_dissolve_mixed_geometries(geometries: list) -> Any:
    """Buffer points and lines, then dissolve into a MultiPolygon."""
    polygons = []
    
    for geom in geometries:
        if isinstance(geom, (Point, LineString, MultiPoint, MultiLineString)):
            polygons.append(geom.buffer(GEOMETRY_BUFFER_DISTANCE))
        else:
            # Keep existing polygons as-is
            polygons.append(geom)
    
    # Dissolve all polygons into a unified geometry
    dissolved_geometry = gpd.GeoSeries(polygons).union_all()
    
    if isinstance(dissolved_geometry, Polygon):
        return MultiPolygon([dissolved_geometry])
    
    return dissolved_geometry

def apply_geometry_transformation(gdf: gpd.GeoDataFrame, geom_type: str) -> gpd.GeoDataFrame:
    """Apply geometry transformation based on the specified type (points, bbox, original)."""
    # Create a copy to avoid modifying the original GeoDataFrame
    result_gdf = gdf.copy()
    
    if geom_type == "points":
        result_gdf.geometry = gpd.GeoSeries(result_gdf.geometry, crs=result_gdf.crs).centroid
        return result_gdf
    elif geom_type == "bbox":
        result_gdf.geometry = gpd.GeoSeries(result_gdf.geometry, crs=result_gdf.crs).envelope
        return result_gdf
    else:  # original/footprint
        return result_gdf

def get_default_column_types(language, dtypes_path) -> defaultdict:
    """Get default column type mappings for TSV data. Unknown columns default to 'object'."""
    df = pd.read_csv(dtypes_path, delimiter='\t', dtype=str)
    dtypes = defaultdict(lambda: 'object', zip(df[language], df["dtype"]))
    return dtypes

def get_converters(column_types):
    return {
        **{col: convert_boolean_value for col, dtype in column_types.items() if dtype == 'bool'},
        **{col: convert_numeric_with_na for col, dtype in column_types.items() if dtype == 'int'}
    }

def check_existing_conversion(conversion_id: str) -> Optional[str]:
    """Check if conversion is already processing or complete. Returns conversion_id if should skip, None otherwise."""
    if not status_manager.has(conversion_id):
        return None
    
    current_status = status_manager.get_status_value(conversion_id)
    if current_status == "processing":
        logging.warning(f"Conversion ID {conversion_id} is already in use. Cancelling new request...")
        return conversion_id
    elif current_status == "complete":
        logging.info(f"Conversion ID {conversion_id} has already been completed. Returning existing output...")
        return conversion_id
    
    return None

def cleanup_files(*file_paths: str) -> None:
    """Safely remove multiple files."""
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.debug(f"Cleaned up file: {file_path}")
            except OSError as e:
                logging.warning(f"Failed to clean up {file_path}: {e}")

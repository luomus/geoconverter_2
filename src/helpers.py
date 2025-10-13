import pandas as pd
from shapely.errors import ShapelyError
from shapely.geometry import (
    GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon
)
from shapely.wkt import loads
import logging
from typing import Optional, Any, Dict
import numpy as np
import geopandas as gpd
import settings
import dask.dataframe as dd

app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

logging.basicConfig(
    level=log_level, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CRS_MAPPING = {
    "euref": "EPSG:3067",
    "wgs84": "EPSG:4326"
}

GEOMETRY_TYPE_MAPPING = {
    "point": "points",
    "bbox": "bbox",
    "footprint": "original"
}

GEOMETRY_BUFFER_DISTANCE = 0.00001  # ~0.5 meters

def safely_parse_wkt(wkt_string: str) -> Optional[Any]:
    """Safely convert WKT string to Shapely geometry."""
    try:
        geom = loads(wkt_string)
        if geom is None or geom.is_empty or not geom.is_valid:
            logging.warning(f"Invalid or empty geometry for WKT: {wkt_string}")
            return None
        bounds = geom.bounds
        if any(not np.isfinite(coord) for coord in bounds):
            logging.warning(f"Geometry with infinite coordinates found: {wkt_string[:100]}...")
            return None
        return geom
            
    except (ShapelyError, Exception) as e:
        logging.warning(f"Failed to convert WKT '{wkt_string}': {e}")
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
        result_gdf.geometry = result_gdf.geometry.centroid
        return result_gdf
    elif geom_type == "bbox":
        result_gdf.geometry = result_gdf.geometry.envelope
        return result_gdf
    else:  # original/footprint
        return result_gdf

def get_default_column_types(language, dtypes_path) -> Dict[str, str]:
    """Get default column type mappings for TSV data."""
    df = pd.read_csv(dtypes_path, delimiter='\t', dtype=str)
    dtypes = dict(zip(df[language], df["dtype"]))
    return dtypes

def get_converters(column_types):
    return {
        **{col: convert_boolean_value for col, dtype in column_types.items() if dtype == 'bool'},
        **{col: convert_numeric_with_na for col, dtype in column_types.items() if dtype == 'int'}
    }
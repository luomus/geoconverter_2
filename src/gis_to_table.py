import os
import logging
import warnings
import geopandas as gpd
import pandas as pd
from fiona import listlayers
import settings

os.environ["OGR_FORCE_ASCII"] = "YES"

# Suppress pyogrio warnings
warnings.filterwarnings("ignore", message=".*does not support open option.*")
warnings.filterwarnings("ignore", message="More than one layer found.*")

CRS_MAPPING = {
    "epsg:3067": "ETRS-TM35FIN",
    "epsg:4326": "wgs84",
    "epsg:2393": "ykj"
}

# Setup logging
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

def read_file_with_encoding(file_path, layer=None):
    """Read GIS file with fallback encoding options."""
    encodings = [None, 'UTF-8', 'LATIN1', 'CP1252']

    for encoding in encodings:
        try:
            gdf = gpd.read_file(file_path, layer=layer, engine='pyogrio',
                encoding=encoding, use_arrow=False)
            logging.debug(f"Read file with {encoding or 'default'} encoding")
            return gdf
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            if encoding is None:
                logging.warning(f"Failed with default encoding: {e}. Trying other encodings.")
            raise

    raise RuntimeError("Failed to read file with any encoding")


def read_multilayer_file(file_path):
    """Read and combine all layers from a multi-layer file."""
    layers = listlayers(file_path)
    layer_gdfs = []

    for layer in layers:
        try:
            gdf = read_file_with_encoding(file_path, layer=layer)
            if not gdf.empty:
                layer_gdfs.append(gdf)
        except Exception as e:
            logging.warning(f"Failed to read layer '{layer}': {e}")

    if not layer_gdfs:
        raise RuntimeError("No readable layers found")

    combined = pd.concat(layer_gdfs, ignore_index=True, join='outer', sort=False)

    if combined.empty:
        raise RuntimeError("Combined layers resulted in empty dataset")

    return combined


def gis_to_table(gis_file):
    """Converts a GIS file to CSV with geometry as WKT."""
    logging.debug(f"Converting {gis_file}")

    # Handle multi-layer files
    if os.path.splitext(gis_file)[1].lower() in ['.gpkg', '.gpx']:
        gdf = read_multilayer_file(gis_file)
    else:
        gdf = read_file_with_encoding(gis_file)

    if gdf is None or gdf.empty:
        raise RuntimeError("No features found in the GIS file")

    # Convert single-point MultiPoints to Points
    def simplify_multipoint(geom):
        if geom.geom_type in ['MultiPoint', 'MultiLineString', 'MultiPolygon'] and len(geom.geoms) == 1:
            return geom.geoms[0]
        return geom

    gdf.geometry = gdf.geometry.apply(simplify_multipoint)

    # Convert to table format
    crs_info = CRS_MAPPING.get(str(gdf.crs).lower(), "Not supported")
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    gdf["crs"] = crs_info
    df = gdf.drop(columns=gdf.geometry.name)

    # Save as CSV with proper Excel-compatible settings
    csv_path = os.path.splitext(gis_file)[0] + ".tsv"
    df.to_csv(csv_path, sep='\t', index=False, escapechar='\\', encoding='utf-8-sig')

    logging.debug(f"Converted {len(df)} records to {csv_path}")
    return csv_path


if __name__ == "__main__":
    logging.debug("Starting process locally...")

    gis_to_table("test_data/test_point_dataset.gpkg")
import os
import logging
import tempfile
from zipfile import ZipFile
import geopandas as gpd
import pandas as pd
from fiona import listlayers
import settings

app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

logging.basicConfig(
    level=log_level, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def gis_to_table(gis_file):
    """
    Converts a GIS file (Shapefile, GeoPackage, GeoJSON) to a CSV file
    with geometry stored as WKT in 'geometry_wkt' column.
    """
    SUPPORTED_EXTENSIONS = {'.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml'}

    ext = os.path.splitext(gis_file)[1].lower()
    gdf = None

    if ext == ".zip":
        logging.debug(f"ZIP archive detected: {gis_file}")
        with tempfile.TemporaryDirectory() as tmpdir:
            with ZipFile(gis_file, 'r') as archive:
                archive.extractall(tmpdir)

            found_files = []
            for root, _, files in os.walk(tmpdir):
                for fname in files:
                    fext = os.path.splitext(fname)[1].lower()
                    if fext in SUPPORTED_EXTENSIONS:
                        found_files.append(os.path.join(root, fname))

            if not found_files:
                raise RuntimeError("No supported GIS files found in ZIP archive.")

            if len(found_files) > 1:
                logging.warning(f"Multiple GIS files found in ZIP. Using the first one: {found_files[0]}")
            else:
                logging.debug(f"Found supported GIS file: {found_files[0]}")

            gdf = read_gis_file(found_files[0])
    else:
        gdf = read_gis_file(gis_file)

    if gdf is None or gdf.empty:
        raise RuntimeError("No features found in the GIS file.")
    
    # Store CRS information
    crs_info = str(gdf.crs) if gdf.crs is not None else "Unknown"
    
    # Handle geometry as WKT
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    gdf["crs"] = crs_info 
    df = gdf.drop(columns=gdf.geometry.name)
    logging.info(f"Converted {len(df)} records in {gdf.crs} to a table successfully.")

    # Save DataFrame as CSV
    csv_path = app_settings.OUTPUT_PATH + os.path.splitext(gis_file)[0] + ".csv"
    df.to_csv(csv_path, index=False)
    logging.info(f"CONVERSION COMPLETED and: {csv_path} is ready. Size: {os.path.getsize(csv_path)} bytes")

    return csv_path

def read_gis_file(path):
    """
    Helper function to load GIS file using GeoPandas, handling multipage GPKG.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".gpkg":
        layers = listlayers(path)
        logging.debug(f"GeoPackage layers found: {layers}")
        gdfs = [gpd.read_file(path, layer=layer, engine='pyogrio') for layer in layers]
        return pd.concat(gdfs, ignore_index=True)
    else:
        return gpd.read_file(path)
    
if __name__ == "__main__":
    logging.info("Starting process locally...")
    
    #gis_to_table('test.gpkg)
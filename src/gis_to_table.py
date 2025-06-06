import os
import logging
import tempfile
from zipfile import ZipFile
import geopandas as gpd
import pandas as pd
from fiona import listlayers

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def gis_to_table(gis_file):
    """
    Converts a GIS file (Shapefile, GeoPackage, GeoJSON) to a tabular pandas DataFrame
    with geometry stored as WKT in 'geometry_wkt' column.

    Parameters:
    gis_file (str): Path to the GIS file.

    Returns:
    pd.DataFrame: A DataFrame containing all attributes and geometry as WKT.
    """
    SUPPORTED_EXTENSIONS = {'.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml'}

    ext = os.path.splitext(gis_file)[1].lower()
    gdf = None

    if ext == ".zip":
        logging.info(f"ZIP archive detected: {gis_file}")
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
                logging.info(f"Found supported GIS file: {found_files[0]}")

            gdf = read_gis_file(found_files[0])
    else:
        gdf = read_gis_file(gis_file)

    if gdf is None or gdf.empty:
        raise RuntimeError("No features found in the GIS file.")
    
    # Handle geometry as WKT
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    df = gdf.drop(columns=gdf.geometry.name)
    logging.info(f"Converted {len(df)} records to a table successfully.")

    # Save DataFrame as CSV
    csv_path = os.path.splitext(gis_file)[0] + ".csv"
    df.to_csv(csv_path, index=False)
    logging.info(f"Saved table as CSV: {csv_path}")

    return csv_path

def read_gis_file(path):
    """
    Helper function to load GIS file using GeoPandas, handling multipage GPKG.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".gpkg":
        layers = listlayers(path)
        logging.info(f"GeoPackage layers found: {layers}")
        gdfs = [gpd.read_file(path, layer=layer, engine='pyogrio') for layer in layers]
        return pd.concat(gdfs, ignore_index=True)
    else:
        return gpd.read_file(path)
    
if __name__ == "__main__":
    logging.info("Starting process locally...")
    
    #gis_to_table('test.gpkg)
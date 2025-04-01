from zipfile import ZipFile
import timeit
import geopandas as gpd
import dask.dataframe as dd
import dask
import warnings
import tempfile
from shapely.wkt import loads
from shapely.errors import ShapelyError
from pyogrio import write_dataframe
import pandas as pd
import os
import dask_geopandas
from shapely.geometry import GeometryCollection, Point, LineString, MultiPoint, MultiLineString, Polygon, MultiPolygon
from threading import Lock
import logging

warnings.filterwarnings("ignore")

# Configure Dask to use threads for parallelism and set the number of workers to the number of CPU cores
dask.config.set(scheduler="threads", num_workers=os.cpu_count())

# Lock to ensure thread-safe writes to the output file
write_lock = Lock()

# Configure logging for better debugging and monitoring
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def safe_loads(wkt):
    """ Safely convert WKT string to Shapely geometry. """
    try:
        return loads(wkt)
    except ShapelyError:
        logging.warning(f"Failed to convert WKT: {wkt}")
        return None

def get_column_mapping(lookup_df):
    """ Get a dictionary of column mappings for use in Dask CSV reader. """
    column_mapping = lookup_df.set_index('finbif_api_var')['translated_var'].to_dict()
    return column_mapping

def get_dtypes(lookup_df):
    """ Get a dictionary of dtypes for use in Dask CSV reader. """
    dtype_dict = lookup_df.set_index('finbif_api_var')['dtype'].to_dict()
    return dtype_dict

def get_date_columns(lookup_df):
    """ Get a list of date columns for use in Dask CSV reader. """
    date_columns = lookup_df[lookup_df['dtype'] == 'datetime64[ns]']['finbif_api_var'].tolist()
    return date_columns

def convert_bool(value):
    """ Convert boolean columns to True/False. """
    if value.lower() in ['true', '1']:
        return True
    elif value.lower() in ['false', '0']:
        return False
    else:
        return None

def convert_int_with_na(value):
    """ Convert integer columns with NA values to float. """
    try:
        return float(value)
    except ValueError:
        return None

def process_geometry(geometry):
    """
    Process a Shapely GeometryCollection to convert it into a MultiPolygon or other geometry types.
    Buffers points and lines if necessary and dissolves the result into a single geometry.
    """
    buffer_distance = 0.00001  # Approximately 0.5 meters

    if isinstance(geometry, GeometryCollection):
        geom_types = {type(geom) for geom in geometry.geoms}
        geometries = list(geometry.geoms)

        # Handle cases where the GeometryCollection contains only one geometry
        if len(geometries) == 1:
            return geometries[0]

        # Convert homogeneous collections to MultiX types
        if geom_types == {LineString}:
            return MultiLineString(list(geometry.geoms))
        elif geom_types == {Point}:
            return MultiPoint(list(geometry.geoms))
        elif geom_types == {Polygon}:
            return MultiPolygon(list(geometry.geoms))
        elif geom_types == {MultiLineString}:
            return MultiLineString([g for geom in geometry.geoms for g in geom.geoms])
        elif geom_types == {MultiPoint}:
            return MultiPoint([g for geom in geometry.geoms for g in geom.geoms])
        elif geom_types == {MultiPolygon}:
            return MultiPolygon([g for geom in geometry.geoms for g in geom.geoms])

        # Buffer points and lines, then dissolve into a MultiPolygon
        polygons = [geom.buffer(buffer_distance) if isinstance(geom, (Point, LineString, MultiPoint, MultiLineString)) 
                    else geom for geom in geometry.geoms]

        dissolved_geometry = gpd.GeoSeries(polygons).unary_union 
            
        if isinstance(dissolved_geometry, Polygon):
            return MultiPolygon([dissolved_geometry])
        
        return dissolved_geometry        
    return geometry

def save_partition(partition, crs, output_gpkg, geom_type):
    """
    Process a single partition and write it to a GeoPackage file.
    Handles geometry transformations like centroid or envelope based on the geom_type.
    """
    gdf = gpd.GeoDataFrame(partition.compute(), geometry="geometry", crs="EPSG:4326").to_crs(crs)

    # Apply geometry transformations if specified
    if geom_type == "points":
        gdf.geometry = gdf.geometry.centroid
    elif geom_type == "bbox":
        gdf.geometry = gdf.geometry.envelope

    # Write the partition to the GeoPackage file
    with write_lock:
        write_dataframe(gdf, output_gpkg, driver="GPKG", encoding='utf8', promote_to_multi=True, append=True)

def process_file_in_zip(filename, z, output_gpkg, geom_type, crs, dtype_dict, column_mapping, date_columns, converters):
    """
    Process a rows file inside the ZIP archive.
    Reads the file as a Dask DataFrame, processes geometries, and writes the output to a GeoPackage.
    """
    with z.open(filename) as file:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(file.read())
            tmp_file_path = tmp_file.name

        try:
            file_size = os.path.getsize(tmp_file_path)
            logging.info(f"File size: {file_size}")
            
            # Read the TSV file as a Dask DataFrame
            ddf = dd.read_csv(
                tmp_file_path,
                sep="\t",
                dtype=dtype_dict,
                assume_missing=True,
                quoting=3,
                on_bad_lines="skip",
                encoding="utf-8",
                blocksize="100MB",
                converters=converters,
                parse_dates=date_columns,
            )

            # Convert WKT strings to Shapely geometries
            ddf["geometry"] = ddf["Gathering.Conversions.WGS84_WKT"].map(safe_loads)

            # Rename and reorder columns based on the lookup table
            ddf = ddf.rename(columns=column_mapping)
            existing_columns = [col for col in column_mapping.values() if col in ddf.columns]
            ddf = ddf[existing_columns]

            # Remove the output file if it already exists
            if os.path.exists(output_gpkg):
                os.remove(output_gpkg)

            # Set the geometry column and process geometries
            ddf = ddf.set_geometry("geometry")
            ddf['geometry'] = ddf['geometry'].apply(process_geometry)

            logging.info(f"Writing to GeoPackage {output_gpkg}...")

            # Convert to delayed objects to avoid recomputation
            delayed_partitions = ddf.to_delayed()
            total_partitions = len(delayed_partitions)

            # Write each partition to the GeoPackage
            for idx, partition in enumerate(delayed_partitions):
                logging.info(f"Writing partition {idx + 1} / {total_partitions}...")
                save_partition(partition, crs, output_gpkg, geom_type)

        finally:
            # Clean up the temporary file
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
                logging.info(f"Deleted temp file: {tmp_file_path}")

def zip_to_gpkg(input_file, output_gpkg, geom_type="original", crs="EPSG:3067"):
    """
    Main function to process a ZIP file containing rows TSV files.
    Reads the TSV file, processes geometries, and writes the output to a GeoPackage.
    """
    logging.info(f"Processing {input_file} -> {output_gpkg}")

    # Read the lookup table for column mappings and data types
    lookup_df = pd.read_csv("lookup_table.csv", sep=',', header=0)
    dtype_dict = get_dtypes(lookup_df)
    column_mapping = get_column_mapping(lookup_df)
    date_columns = get_date_columns(lookup_df)

    # Define converters for specific data types
    converters = {col: convert_bool for col, dtype in dtype_dict.items() if dtype == 'bool'}
    converters.update({col: convert_int_with_na for col, dtype in dtype_dict.items() if dtype == 'int'})

    num_cores = os.cpu_count()
    logging.info(f"Number of cores: {num_cores}")
    
    # Process each file in the ZIP archive
    with ZipFile(input_file, "r") as z:
        for filename in z.namelist():
            if filename.startswith("rows_") and filename.endswith(".tsv"):
                process_file_in_zip(filename, z, output_gpkg, geom_type, crs, dtype_dict, column_mapping, date_columns, converters)

if __name__ == "__main__":
    logging.info("Starting process locally...")

    zip_to_gpkg("test_data/HBF.58844.zip", f"output.gpkg", geom_type="original", crs="EPSG:3067")


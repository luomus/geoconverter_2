from zipfile import ZipFile
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

def get_column_mapping(lookup_df, column_name="translated_var"):
    """ Get a dictionary of column mappings from the lookup table for use in Dask CSV reader. """
    column_mapping = lookup_df.set_index(column_name)['translated_var'].to_dict()
    return column_mapping

def get_dtypes(lookup_df, column_name="finbif_api_var"):
    """ Get a dictionary of dtypes from the lookup table for use in Dask CSV reader. """
    return lookup_df.set_index(column_name)['dtype'].to_dict()

def get_date_columns(lookup_df):
    """ Get a list of date columns from the lookup table for use in Dask CSV reader. """
    return lookup_df[lookup_df['dtype'] == 'datetime64[ns]']['finbif_api_var'].tolist()

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

def read_tsv_to_ddf(file_path, dtype_dict, date_columns, converters, wkt_column):
    """
    Helper to read a TSV file into a Dask DataFrame and process WKT geometries.
    """
    ddf = dd.read_csv(
        file_path,
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
    ddf["geometry"] = ddf[wkt_column].map(safe_loads)
    return ddf

def compress_gpkg_to_zip(output_gpkg):
    """
    Zip the output GeoPackage file and delete the original one to save space.
    """
    zip_file_path = output_gpkg.replace(".gpkg", ".zip")
    with ZipFile(zip_file_path, 'w') as zipf:
        zipf.write(output_gpkg, arcname=os.path.basename(output_gpkg))
    if os.path.exists(output_gpkg):
        os.remove(output_gpkg)
    logging.info(f"Zipped and output GeoPackage to: {zip_file_path} and deleted the original {output_gpkg}")

def process_tsv_source(file_path, output_gpkg, geom_type, crs, dtype_dict, column_mapping, date_columns, converters, wkt_column, cleanup_temp=False, facts=None):
    """
    Process a TSV source (from temp file or disk).
    """
    try:
        file_size = os.path.getsize(file_path)
        logging.info(f"File size: {file_size}")

        ddf = read_tsv_to_ddf(file_path, dtype_dict, date_columns, converters, wkt_column)

        # Rename and reorder columns based on the lookup table
        ddf = ddf.rename(columns=column_mapping)
        existing_columns = [col for col in column_mapping.values() if col in ddf.columns]
        ddf = ddf[existing_columns]

        # Remove the output file if it already exists
        if os.path.exists(output_gpkg):
            os.remove(output_gpkg)

        # Set the geometry column and process geometries
        ddf = ddf.set_geometry("geometry")
        ddf["geometry"] = ddf["geometry"].apply(process_geometry)

        # join facts to the dask dataframe using columns "Parent" and "Havainnon tunniste"
        logging.info("Joining facts to the Dask DataFrame...")
        if facts is not None:
            ddf = ddf.merge(facts, how="left", left_on="record_id", right_on="Parent").drop(columns=["Parent"])

        logging.info(f"Writing to GeoPackage {output_gpkg}...")

        delayed_partitions = ddf.to_delayed()
        total_partitions = len(delayed_partitions)

        for idx, partition in enumerate(delayed_partitions):
            logging.info(f"Writing partition {idx + 1} / {total_partitions}...")
            save_partition(partition, crs, output_gpkg, geom_type)

    finally:
        if cleanup_temp and os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Deleted temp file: {file_path}")

        compress_gpkg_to_zip(output_gpkg)

def extract_and_process_facts(fact_file, z, columns):
    with z.open(fact_file) as file:
        facts = pd.read_csv(file, sep="\t", header=0)

    facts = facts[facts["Fact"].isin(columns)]
    facts = facts.drop(columns=["IntValue", "DecimalValue"])

    # Convert Fact values to column names and drop the original Fact column. Keep Value column as values.
    facts = facts.pivot_table(index="Parent", columns="Fact", values="Value", aggfunc="first").reset_index()
    return facts

def get_facts(input_path):
    """ Get unit and gathering facts from zipped unit_facts_HBF.id.tsv and gathering_facts_HBF.id.tsv files. """
    with ZipFile(input_path, "r") as z:
        file_name = os.path.split(input_path)[1].strip(".zip")

        input_unit_fact_file = os.path.join(f"unit_facts_{file_name}.tsv")
        input_gathering_fact_file = os.path.join(f"gathering_facts_{file_name}.tsv")
        input_document_fact_file = os.path.join(f"document_facts_{file_name}.tsv")

        unit_facts = extract_and_process_facts(input_unit_fact_file, z, ["Museo, johon lajista kerätty näyte on talletettu", "Havainnon laatu", "Havainnon määrän yksikkö"])
        gathering_facts = extract_and_process_facts(input_gathering_fact_file, z, ["Vesistöalue", "Sijainnin tarkkuusluokka", "Pesintätulos"])
        document_facts = extract_and_process_facts(input_document_fact_file, z, ["Seurattava laji"])

    # merge all facts into one dataframe
    facts = pd.merge(unit_facts, gathering_facts, how="left", on="Parent").merge(document_facts, how="left", on="Parent")

    facts = facts.rename(columns={
        "Vesistöalue": "event_fact_Vesistöalue",
        "Sijainnin tarkkuusluokka": "event_fact_Sijainnin_tarkkuusluokka",
        "Pesintätulos": "event_fact_Pesintätulos",
        "Museo, johon lajista kerätty näyte on talletettu": "record_fact_Museo_johon_lajista_kerätty_näyte_on_talletettu",
        "Havainnon laatu": "record_fact_Havainnon_laatu",
        "Havainnon määrän yksikkö": "record_fact_Havainnon_määrän_yksikkö",
        "Seurattava laji": "document_fact_Seurattava_laji"
    })

    return facts

def tsv_to_gpkg(input_file, output_gpkg, geom_type="original", crs="EPSG:3067"):
    """
    Main function to process a ZIP file or standalone TSV file.
    Reads the file(s), processes geometries, and writes the output to a GeoPackage.
    """
    logging.info(f"Processing {input_file} -> {output_gpkg}")

    # Read the lookup table for column mappings and data types
    lookup_df = pd.read_csv("lookup_table.csv", sep=',', header=0)
    date_columns = get_date_columns(lookup_df)

    is_zip = input_file.endswith(".zip")

    if is_zip:
        dtype_dict = get_dtypes(lookup_df, column_name="finbif_api_var")
        column_mapping = get_column_mapping(lookup_df, column_name="finbif_api_var")
        facts = get_facts(input_file)
    else:
        dtype_dict = get_dtypes(lookup_df, column_name="lite_download_var")
        column_mapping = get_column_mapping(lookup_df, column_name="lite_download_var")

    # Define converters for specific data types
    converters = {col: convert_bool for col, dtype in dtype_dict.items() if dtype == 'bool'}
    converters.update({col: convert_int_with_na for col, dtype in dtype_dict.items() if dtype == 'int'})

    num_cores = os.cpu_count()
    logging.info(f"Number of cores: {num_cores}")

    if is_zip:
        with ZipFile(input_file, "r") as z:
            for filename in z.namelist():
                if filename.startswith("rows_") and filename.endswith(".tsv"):
                    with z.open(filename) as file:
                        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                            tmp_file.write(file.read())
                            tmp_file_path = tmp_file.name
                    process_tsv_source(tmp_file_path, output_gpkg, geom_type, crs, dtype_dict, column_mapping, date_columns, converters, wkt_column="Gathering.Conversions.WGS84_WKT", cleanup_temp=True, facts=facts)

    else:
        process_tsv_source(input_file, output_gpkg, geom_type, crs, dtype_dict, column_mapping, date_columns, converters, wkt_column="WGS84 WKT", cleanup_temp=False, facts=None)


if __name__ == "__main__":
    logging.info("Starting process locally...")

    tsv_to_gpkg("test_data/HBF.98771.zip", f"output_from_zip.gpkg", geom_type="original", crs="EPSG:3067")
    tsv_to_gpkg("test_data/laji-data.tsv", f"output_from_tsv.gpkg", geom_type="original", crs="EPSG:3067")


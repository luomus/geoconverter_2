import logging
import os
import tempfile
import zipfile
import shutil
from threading import Lock
from time import time
from typing import Dict, Union
from zipfile import ZipFile
import dask
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd
from dask import config as dask_config
from fastapi.responses import FileResponse
from pyogrio import write_dataframe
from helpers import *
import settings


import warnings
warnings.filterwarnings("ignore")
pd.options.mode.use_inf_as_na = True

dask_config.set(scheduler="threads", num_workers=os.cpu_count())

app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)

logging.basicConfig(
    level=log_level, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Thread-safe locks
write_lock = Lock()
status_lock = Lock()

CRS_MAPPING = {
    "euref": "EPSG:3067",
    "wgs84": "EPSG:4326"
}

GEOMETRY_TYPE_MAPPING = {
    "point": "points",
    "bbox": "bbox",
    "footprint": "original"
}

LANGUAGE_MAPPING = {
    "tech": 0,
    "fi": 1,
    "en": 2
}

GEOMETRY_LANG_MAPPING = {
    "fi": "WGS84 geometria",
    "en": "Footprint WKT",
    "tech": "footprintWKT"
}

conversion_status: Dict = {}

LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = "100MB"

def handle_conversion_request(conversion_id: str, zip_path: str, language: str, geo_type: str, crs: str, background_tasks, uploaded_file: bool) -> Union[FileResponse, dict]:
    """
    Handle API conversion request - manages status tracking and task scheduling.
    
    Args:
        conversion_id: Unique identifier (file name) for this conversion. E.g. HBF.12345
        zip_path: Path to the input ZIP file
        language: Language for the column names ('fi', 'en', 'tech')
        geo_type: Type of geometry processing ('point', 'bbox', 'footprint')
        crs: Coordinate reference system ('euref', 'wgs84')
        background_tasks: FastAPI background tasks manager
        uploaded_file: Whether this is an uploaded file
        
    Returns:
        FileResponse for small files, status dict for large files
    """
    mapped_geo_type = GEOMETRY_TYPE_MAPPING[geo_type]
    mapped_crs = CRS_MAPPING[crs]
    
    file_size = os.path.getsize(zip_path)

    logging.info(f"Starting conversion for ID: {conversion_id}, zip_path: {zip_path}, file size: {file_size}")

    update_conversion_status(
        conversion_id,
        "processing",
        uploaded_file=uploaded_file
    )
    
    if file_size < LARGE_FILE_THRESHOLD:
        # Small files: process immediately and clean up after
        convert_file(zip_path, language, mapped_geo_type, mapped_crs, conversion_id)
                
        return {
            "id": conversion_id,
            "status": "completed",
            "message": "Small file processed immediately. Ready for download.",
            "status_url": f"/status/{conversion_id}",
            "download_url": f"/output/{conversion_id}",
            "file_size_mb": round(file_size / (1024*1024), 1)
        }
        
    else:
        # Large files: process in background (background task handles cleanup)
        background_tasks.add_task(convert_file, zip_path, language, mapped_geo_type, mapped_crs, conversion_id)

        return {
            "id": conversion_id,
            "status": "processing",
            "message": f"Large file detected ({file_size / (1024*1024):.1f}MB). Processing in background...",
            "status_url": f"/status/{conversion_id}",
            "download_url": f"/output/{conversion_id}",
            "file_size_mb": round(file_size / (1024*1024), 1)
        }

def create_output_zip(zip_path: str, output_gpkg: str, conversion_id: str, cleanup_source: bool = False) -> str:
    """
    Create a ZIP file containing the GPKG and all other files from input ZIP.
    """
    logging.debug(f"Creating output ZIP: {zip_path} with GPKG: {output_gpkg}")
    zip_path_out = conversion_id + '.zip'

    with ZipFile(zip_path, "r") as zin, ZipFile(zip_path_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for zi in zin.infolist():
            # Skip the occurrences.txt at the root of the input zip
            if zi.filename == "occurrences.txt":
                logging.debug("Skipping occurrences.txt in output zip")
                continue

            if zi.is_dir():
                # Preserve explicit directory entries if present
                zout.writestr(zi, b"")
                continue

            # Stream-copy file contents to avoid loading into memory or disk
            with zin.open(zi, "r") as src, zout.open(zi, "w") as dst:
                shutil.copyfileobj(src, dst, length=1_048_576)
                logging.debug(f"Added {zi.filename} to zip {zip_path_out}")

        # Finally, add the generated GPKG to the root of the output zip
        if os.path.exists(output_gpkg):
            zout.write(output_gpkg, arcname=os.path.basename(output_gpkg))
            logging.debug(f"Added {output_gpkg} to zip {zip_path_out}")
        else:
            logging.warning(f"GPKG file not found: {output_gpkg}")

    if cleanup_source:
        cleanup_files(output_gpkg)

    return zip_path_out

def update_conversion_status(conversion_id: str, status: str, **kwargs) -> None:
    """Thread-safe update of conversion status."""
    with status_lock:
        conversion_status[conversion_id] = {
            "status": status,
            "timestamp": time(),
            **kwargs
        }

def cleanup_files(*file_paths: str) -> None:
    """Safely remove multiple files."""
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.debug(f"Cleaned up file: {file_path}")
            except OSError as e:
                logging.warning(f"Failed to clean up {file_path}: {e}")

def convert_file(zip_path: str, language: str, geo_type: str, crs: str, conversion_id: str) -> None:
    """Process file conversion from ZIP/TSV to GeoPackage."""
    
    output_gpkg = f'{conversion_id}.gpkg'
    wkt_column = GEOMETRY_LANG_MAPPING[language]


    try:
        logging.debug(f"Processing {zip_path} -> {output_gpkg}")

        if zip_path.endswith(".zip"):
            occurrence_file = f"occurrences.txt"
            logging.debug(f"Reading occurrence file: {occurrence_file}...")

            with ZipFile(zip_path, "r") as zip_file:
                with zip_file.open(occurrence_file) as source_file:
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        temp_file.write(source_file.read())
                        temp_file_path = temp_file.name

            process_tsv_data(
                conversion_id,
                zip_path,
                temp_file_path, 
                output_gpkg, 
                language,
                geo_type, 
                crs, 
                wkt_column=wkt_column,
                cleanup_temp=True, 
                compress_output=True
            )
        else:
            # Process standalone TSV file
            pass
        
        if zip_path.endswith(".zip"):
            final_output = f"{conversion_id}.zip"
        else:
            final_output = output_gpkg
            
        if not os.path.exists(final_output):
            raise FileNotFoundError(f"Output file was not created: {final_output}")
        
        logging.info(f"CONVERSION COMPLETED for ID: {conversion_id}. Size: {os.path.getsize(final_output)} bytes")
        
        with status_lock:
            current_status = conversion_status.get(conversion_id, {})
            uploaded_file = current_status.get("uploaded_file", False)
        
        update_conversion_status(
            conversion_id, 
            "completed", 
            output=final_output,
            file_size=os.path.getsize(final_output),
            uploaded_file=uploaded_file
        )
        
    except Exception as e:
        logging.error(f"Error during conversion: {e}")
        update_conversion_status(conversion_id, "failed", error=str(e))
        cleanup_files(output_gpkg)

def write_partition_to_geopackage(partition, crs: str, output_gpkg: str, geom_type: str, append: bool = True) -> bool:
    """Process a single partition and write it to a GeoPackage file.

    Returns True if data was written (non-empty after filtering), else False.
    """
    gdf = gpd.GeoDataFrame(
        partition.compute(), 
        geometry="geometry", 
        crs="EPSG:4326"
    ).to_crs(crs)

    valid_mask = (
        gdf['geometry'].notna() & 
        gdf['geometry'].is_valid & 
        ~gdf['geometry'].is_empty
    )
    gdf = gdf[valid_mask].copy()

    if len(gdf) == 0:
        logging.warning("No valid geometries found in partition, skipping...")
        return False

    if not isinstance(gdf, gpd.GeoDataFrame):
        logging.warning("Partition was not a GeoDataFrame. Converting it to GeoDataFrame...")
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=crs)

    gdf = apply_geometry_transformation(gdf, geom_type)
    
    # Write to GeoPackage with thread safety
    with write_lock:
        write_dataframe(
            gdf, 
            output_gpkg,
            driver="GPKG", 
            encoding='utf8', 
            promote_to_multi=True, 
        append=append
        )
    return True

def read_tsv_as_dask_dataframe(file_path: str, language: str, wkt_column: str) -> dd.DataFrame:
    """Read a TSV file into a Dask DataFrame and process WKT geometries."""
    column_types = get_default_column_types(language, dtypes_path='lookup_table.tsv')
    mapped_language = LANGUAGE_MAPPING.get(language, 0)
    skipped_rows = [0, 1, 2]
    skipped_rows.remove(mapped_language)

    # Define converters for specific data types
    converters = get_converters(column_types)

    ddf = dd.read_csv(
        file_path,
        sep="\t",
        assume_missing=True,
        quoting=3,  # csv.QUOTE_NONNUMERIC
        on_bad_lines="skip",
        encoding="utf-8",
        blocksize=CHUNK_SIZE,
        header=0, # is always 0 as we skip rows
        dtype=column_types,
        skiprows=skipped_rows,  # Skip other header rows
        converters=converters
    )

    logging.debug(f"Read occurrences from {file_path}")

    ddf = ddf[ddf[wkt_column].notnull()]  # Remove NA values #TODO: Maybe better to keep them in the future
    ddf = ddf[ddf[wkt_column].str.strip() != ""]  # Remove empty strings and whitespace

    logging.debug(f"Removed rows with null or empty WKT in column {wkt_column}")

    ddf["geometry"] = ddf[wkt_column].map(safely_parse_wkt)
    ddf = ddf.set_geometry("geometry")
    ddf["geometry"] = ddf["geometry"].apply(normalize_geometry_collection)

    return ddf

def process_tsv_data(
    conversion_id: str,
    zip_path: str,
    temp_file_path: str, 
    output_gpkg: str, 
    language: str, 
    geom_type: str, 
    crs: str, 
    wkt_column: str,
    cleanup_temp: bool = False, 
    compress_output: bool = True
) -> None:
    """Process TSV data from a file and convert it to GeoPackage format."""
    try:
        ddf = read_tsv_as_dask_dataframe(temp_file_path, language, wkt_column)

        logging.debug(f"Removing output GeoPackage: {output_gpkg} if exists already")
        if os.path.exists(output_gpkg):
            os.remove(output_gpkg)

        logging.debug(f"Writing to GeoPackage {output_gpkg}...")

        delayed_partitions = ddf.to_delayed()
        total_partitions = len(delayed_partitions)

        created = False
        for idx, partition in enumerate(delayed_partitions):
            logging.debug(f"Writing partition {idx + 1} / {total_partitions}...")
            wrote = write_partition_to_geopackage(
                partition,
                crs,
                output_gpkg,
                geom_type,
                append=created
            )
            if wrote and not created:
                created = True

    finally:
        logging.debug(f"Finished processing occurrences.txt -> {output_gpkg}")
        if compress_output:
            create_output_zip(zip_path, output_gpkg, conversion_id, cleanup_source=True)

        if cleanup_temp:
            cleanup_files(zip_path)


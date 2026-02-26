import logging
import os
import tempfile
import zipfile
import shutil
from dataclasses import dataclass
from threading import Lock
from time import time
from typing import Dict, Union, Optional
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
from email_notifications import notify_failure
import warnings

warnings.filterwarnings("ignore")
pd.options.mode.use_inf_as_na = True
dask_config.set(scheduler="threads", num_workers=os.cpu_count())
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
write_lock = Lock()

CRS_MAPPING = {"euref": "EPSG:3067", "wgs84": "EPSG:4326"}
GEOMETRY_TYPE_MAPPING = {"point": "points", "bbox": "bbox", "footprint": "original"}
LANGUAGE_MAPPING = {"tech": 0, "fi": 1, "en": 2}
GEOMETRY_LANG_MAPPING = {"fi": "WGS84 geometria", "en": "Footprint WKT", "tech": "footprintWKT"}
LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = "100MB"

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
        return app_settings.OUTPUT_PATH + f'{conversion_id_clean}.gpkg'
    
    @property
    def mapped_crs(self) -> str:
        """Get the mapped CRS value."""
        return CRS_MAPPING[self.crs]
    
    @property
    def mapped_geo_type(self) -> str:
        """Get the mapped geometry type."""
        return GEOMETRY_TYPE_MAPPING[self.geo_type]

# ============================================================================
# MAIN CONVERSION WORKFLOW (STEP 1)
# ============================================================================

def handle_zip_conversion_request(conversion_id: str, zip_path: str, language: str, geo_type: str, crs: str, background_tasks, is_user_upload: bool, original_filename: Optional[str] = None) -> Union[FileResponse, dict]:
    """
    Handle API conversion request - manages status tracking and task scheduling.
    
    Args:
        conversion_id: Unique identifier (file name) for this conversion. E.g. HBF.12345
        zip_path: Path to the input file (ZIP or TSV)
        language: Language for the column names ('fi', 'en', 'tech')
        geo_type: Type of geometry processing ('point', 'bbox', 'footprint')
        crs: Coordinate reference system ('euref', 'wgs84')
        background_tasks: FastAPI background tasks manager
        is_user_upload: True if file was uploaded by user (skips auth check on download)
        
    Returns:
        FileResponse for small files, status dict for large files
    """
    job = ConversionJob(
        conversion_id=conversion_id,
        input_path=zip_path,
        language=language,
        geo_type=geo_type,
        crs=crs,
        is_user_upload=is_user_upload,
        original_filename=original_filename
    )
    
    # Check for existing conversion
    existing = check_existing_conversion(job.conversion_id)
    if existing:
        return existing

    file_size = os.path.getsize(job.input_path)
    logging.info(f"Starting ZIP conversion for ID: {job.conversion_id}, input_path: {job.input_path}, file size: {file_size}")

    status_manager.update(job.conversion_id, "processing", is_user_upload=job.is_user_upload, original_filename=job.original_filename)
    
    if file_size < LARGE_FILE_THRESHOLD:
        # Small files: process immediately and clean up after
        try:
            convert_file(job)
        except Exception as e:
            # Status already updated by convert_file, just re-raise
            raise
                
        return job.conversion_id
        
    else:
        # Large files: process in background (background task handles cleanup)
        background_tasks.add_task(convert_file, job)

        return job.conversion_id

def handle_tsv_conversion_request(conversion_id: str, tsv_path: str, language: str, geo_type: str, crs: str, background_tasks) -> Union[FileResponse, dict]:
    """Handle conversion request for direct TSV file."""
    job = ConversionJob(
        conversion_id=conversion_id,
        input_path=tsv_path,
        language=language,
        geo_type=geo_type,
        crs=crs,
        is_user_upload=True,
        original_filename="laji-data"
    )
    
    # Check for existing conversion
    existing = check_existing_conversion(job.conversion_id)
    if existing:
        return existing

    file_size = os.path.getsize(job.input_path)
    logging.info(f"Starting TSV conversion for ID: {job.conversion_id}, input_path: {job.input_path}, file size: {file_size}")

    status_manager.update(job.conversion_id, "processing", is_user_upload=job.is_user_upload, original_filename=job.original_filename)
    
    try:
        convert_file(job)
    except Exception as e:
        raise
    return job.conversion_id


# ============================================================================
# FILE PROCESSING
# ============================================================================

def create_output_zip(job: ConversionJob, cleanup_source: bool = False) -> str:
    """Create output ZIP containing the GPKG and all original files except occurrences.txt."""
    logging.debug(f"Creating output ZIP: {job.input_path} with GPKG: {job.output_gpkg}")
    zip_path_out = app_settings.OUTPUT_PATH + job.conversion_id + '.zip'

    with ZipFile(job.input_path, "r") as zin, ZipFile(zip_path_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
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
        if os.path.exists(job.output_gpkg):
            zout.write(job.output_gpkg, arcname=f"{job.original_filename}.gpkg")
            logging.debug(f"Added {job.output_gpkg} to zip {zip_path_out}")
        else:
            logging.warning(f"GPKG file not found: {job.output_gpkg}")

    if cleanup_source:
        cleanup_files(job.output_gpkg)

    return zip_path_out

def create_output_zip_simple(job: ConversionJob, cleanup_source: bool = False) -> str:
    """Create output ZIP containing only the GPKG file."""
    logging.debug(f"Creating simple output ZIP with GPKG: {job.output_gpkg}")
    zip_path_out = app_settings.OUTPUT_PATH + job.conversion_id + '.zip'

    with ZipFile(zip_path_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        if os.path.exists(job.output_gpkg):
            zout.write(job.output_gpkg, arcname=f"{job.original_filename}.gpkg")
            logging.debug(f"Added {job.output_gpkg} to zip {zip_path_out}")
        else:
            logging.warning(f"GPKG file not found: {job.output_gpkg}")

    if cleanup_source:
        cleanup_files(job.output_gpkg)

    return zip_path_out

def convert_file(job: ConversionJob) -> None:
    """Process file conversion from ZIP/TSV to GeoPackage."""
    try:
        logging.debug(f"Processing {job.input_path} -> {job.conversion_id}")

        if job.input_path.endswith(".zip"):
            final_output = _extract_and_process_zip(job)
        elif job.input_path.endswith(".tsv"):
            process_tsv_data_simple(job, job.input_path, cleanup_temp=False, compress_output=True)
            final_output = app_settings.OUTPUT_PATH + f"{job.conversion_id}.zip"
        else:
            logging.warning(f"Only ZIP and TSV files are supported. Received {job.input_path}. Skipping conversion.")
            return
            
        if not os.path.exists(final_output):
            raise FileNotFoundError(f"Output file was not created: {final_output}")
        
        logging.info(f"CONVERSION COMPLETED for ID: {job.conversion_id}. Size: {os.path.getsize(final_output)} bytes")
        
        status_manager.update(
            job.conversion_id, "complete", 
            output=final_output,
            file_size=os.path.getsize(final_output),
            is_user_upload=job.is_user_upload,
            original_filename=job.original_filename,
            progress_percent=100
        )
        
    except Exception as e:
        logging.error(f"Error during conversion: {e}")
        
        # Send email notification for conversion failure
        notify_failure(str(e), job.conversion_id)
        
        status_manager.update(job.conversion_id, "failed", error=str(e))
        cleanup_files(job.output_gpkg)

def _extract_and_process_zip(job: ConversionJob) -> str:
    """Extract occurrences.txt from ZIP and process it to GeoPackage."""
    occurrence_file = "occurrences.txt"
    
    logging.debug(f"Reading occurrence file: {occurrence_file}...")
    
    with ZipFile(job.input_path, "r") as zip_file:
        with zip_file.open(occurrence_file) as source_file:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(source_file.read())
                temp_file_path = temp_file.name

    process_tsv_data(job, temp_file_path, cleanup_temp=True, compress_output=True)
    
    return app_settings.OUTPUT_PATH + f"{job.conversion_id}.zip"

# ============================================================================
# DATA PROCESSING
# ============================================================================

def write_partition_to_geopackage(partition, crs: str, output_gpkg: str, geom_type: str, append: bool = True) -> bool:
    """Process a partition and write valid geometries to GeoPackage. Returns True if data was written."""
    gdf = gpd.GeoDataFrame(
        partition.compute(), 
        geometry="geometry", 
        crs="EPSG:4326"
    ).to_crs(crs)

    if gdf.empty:
        logging.warning("The file is empty after crs conversion or computing, skipping...")
        return False

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
    try:
        with write_lock:
            write_dataframe(
                gdf, 
                output_gpkg,
                driver="GPKG", 
                encoding='utf8', 
                promote_to_multi=True, 
            append=append
            )
    except Exception as e:
        logging.error(f"Failed to write partition to GeoPackage: {e}")
        return False
    return True

def read_tsv_as_dask_dataframe(file_path: str, job: ConversionJob) -> dd.DataFrame:
    """Read TSV file into Dask DataFrame and process WKT geometries."""
    column_types = get_default_column_types(job.language, dtypes_path='lookup_table.tsv')
    mapped_language = LANGUAGE_MAPPING.get(job.language, 0)
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

    ddf = process_wkt_geometry(ddf, job.wkt_column)

    return ddf

def read_tsv_simple(file_path: str, wkt_column: str = 'WGS84 WKT') -> dd.DataFrame:
    """Read TSV file with standard headers (no multi-language rows)."""
    ddf = dd.read_csv(
        file_path,
        sep="\t",
        assume_missing=True,
        quoting=3,
        on_bad_lines="skip",
        encoding="utf-8",
        blocksize=CHUNK_SIZE,
        dtype=str
    )

    ddf = process_wkt_geometry(ddf, wkt_column)

    return ddf

def process_tsv_data(
    job: ConversionJob,
    temp_file_path: str,
    cleanup_temp: bool = False, 
    compress_output: bool = True
) -> None:
    """Process TSV data from a file and convert it to GeoPackage format."""
    created = False
    try:
        ddf = read_tsv_as_dask_dataframe(temp_file_path, job)

        logging.debug(f"Removing output GeoPackage: {job.output_gpkg} if exists already")
        if os.path.exists(job.output_gpkg):
            os.remove(job.output_gpkg)

        logging.debug(f"Writing to GeoPackage {job.output_gpkg}...")

        delayed_partitions = ddf.to_delayed()
        total_partitions = len(delayed_partitions)

        for idx, partition in enumerate(delayed_partitions):
            progress_percent = min(int(((idx + 1) / total_partitions) * 100), 99)
            status_manager.update(job.conversion_id, "processing", progress_percent=progress_percent)
            logging.debug(f"Writing partition {idx + 1} / {total_partitions}... ({progress_percent}%)")
            wrote = write_partition_to_geopackage(
                partition,
                job.mapped_crs,
                job.output_gpkg,
                job.mapped_geo_type,
                append=created
            )
            if wrote and not created:
                created = True
        
        # Set to 99% when done processing partitions (100% only when status is "complete")
        status_manager.update(job.conversion_id, "processing", progress_percent=99)

        # Check if GPKG was actually created
        if not created or not os.path.exists(job.output_gpkg):
            raise ValueError(f"No valid geometries found in the data. GPKG file could not be created.")

    finally:
        if compress_output and created:
            create_output_zip(job, cleanup_source=True)

        if cleanup_temp:
            cleanup_files(temp_file_path)

def process_tsv_data_simple(
    job: ConversionJob,
    tsv_file_path: str,
    cleanup_temp: bool = False, 
    compress_output: bool = True
) -> None:
    """Process TSV data with standard headers (no multi-language rows)."""
    created = False
    try:
        ddf = read_tsv_simple(tsv_file_path, wkt_column='WGS84 WKT')

        logging.debug(f"Writing to GeoPackage {job.output_gpkg}...")

        delayed_partitions = ddf.to_delayed()
        total_partitions = len(delayed_partitions)

        for idx, partition in enumerate(delayed_partitions):
            progress_percent = min(int(((idx + 1) / total_partitions) * 100), 99)
            status_manager.update(job.conversion_id, "processing", progress_percent=progress_percent)
            logging.debug(f"Writing partition {idx + 1} / {total_partitions}... ({progress_percent}%)")
            wrote = write_partition_to_geopackage(
                partition,
                job.mapped_crs,
                job.output_gpkg,
                job.mapped_geo_type,
                append=created
            )
            if wrote and not created:
                created = True
        
        # Set to 99% when done processing partitions (100% only when status is "complete")
        status_manager.update(job.conversion_id, "processing", progress_percent=99)

        # Check if GPKG was actually created
        if not created or not os.path.exists(job.output_gpkg):
            raise ValueError(f"No valid geometries found in the data. GPKG file could not be created.")

    finally:
        if compress_output and created:
            create_output_zip_simple(job, cleanup_source=True)

        if cleanup_temp:
            cleanup_files(tsv_file_path)


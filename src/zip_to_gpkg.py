import logging
import os
import tempfile
import zipfile
import shutil
from typing import Optional
from zipfile import ZipFile
import dask
import dask.dataframe as dd
import dask_geopandas
import pandas as pd
from dask import config as dask_config
import warnings

from models import (
    ConversionJob,
    app_settings,
    CHUNK_SIZE,
    _status_manager,
)
from helpers import (
    process_wkt_geometry,
    check_existing_conversion,
    cleanup_files,
    process_partition_for_geopackage,
    write_gdf_to_geopackage,
    get_default_column_types,
    get_converters,
    apply_geometry_transformation,
)
from email_notifications import notify_failure

warnings.filterwarnings("ignore")

pd.options.mode.use_inf_as_na = True
dask_config.set(scheduler="threads", num_workers=os.cpu_count())

LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
LANGUAGE_MAPPING = {"tech": 0, "fi": 1, "en": 2}


def handle_zip_conversion_request(conversion_id: str, zip_path: str, language: str, geo_type: str, crs: str, background_tasks, is_user_upload: bool, original_filename: Optional[str] = None) -> str:
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

    _status_manager.update(job.conversion_id, "processing", is_user_upload=job.is_user_upload, original_filename=job.original_filename)
    
    if file_size >= LARGE_FILE_THRESHOLD:
        background_tasks.add_task(convert_file, job)
    else:
        convert_file(job)
    
    return job.conversion_id
    
def convert_file(job: ConversionJob) -> None:
    """Process file conversion from ZIP/TSV to GeoPackage."""
    try:
        logging.debug(f"Processing {job.input_path} -> {job.conversion_id}")

        temp_file_path = _extract_occurrences_file(job.input_path)
        process_tsv_data(job, temp_file_path, cleanup_temp=True)
        
        final_output = os.path.join(app_settings.OUTPUT_PATH, f"{job.conversion_id}.zip")
            
        if not os.path.exists(final_output):
            raise FileNotFoundError(f"Output file was not created: {final_output}")
        
        logging.info(f"CONVERSION COMPLETED for ID: {job.conversion_id}. Size: {os.path.getsize(final_output)} bytes")
        
        _status_manager.update(
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
        notify_failure(str(e), job.conversion_id, details={
            "Input": job.input_path,
        })
        
        _status_manager.update(job.conversion_id, "failed", error=str(e))
        cleanup_files(job.output_gpkg)
        
def _extract_occurrences_file(zip_path: str) -> str:
    """Extract occurrences.txt from ZIP to a temporary file.
    
    Args:
        zip_path: Path to the ZIP file
        
    Returns:
        Path to the temporary file containing occurrences.txt
    """
    occurrence_file = "occurrences.txt"
    
    logging.debug(f"Extracting {occurrence_file} from ZIP...")
    
    with ZipFile(zip_path, "r") as zip_file:
        with zip_file.open(occurrence_file) as source_file:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(source_file.read())
                return temp_file.name

def create_output_zip(job: ConversionJob):
    """Create output ZIP containing the GPKG and all original files except occurrences.txt."""
    logging.debug(f"Creating output ZIP: {job.input_path} with GPKG: {job.output_gpkg}")
    zip_path_out = os.path.join(app_settings.OUTPUT_PATH, job.conversion_id + '.zip')

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

def read_tsv_as_dask_dataframe(file_path: str, job: ConversionJob) -> dd.DataFrame:
    """Read TSV file into Dask DataFrame and process WKT geometries."""
    column_types = get_default_column_types(job.language, dtypes_path='lookup_table.tsv')
    mapped_language = LANGUAGE_MAPPING.get(job.language, 0)
    skipped_rows = [0, 1, 2]
    skipped_rows.remove(mapped_language)

    # Define converters for specific data types
    converters = get_converters(column_types)

    # Read the header to build a complete dtype dict so unknown columns get
    # 'object' instead of being inferred by dask (defaultdict's default factory
    # is not triggered by pandas' `col in dtype` check).
    header = pd.read_csv(file_path, sep='\t', nrows=0, skiprows=skipped_rows, encoding='utf-8')
    full_dtype = {col: column_types[col] for col in header.columns}

    ddf = dd.read_csv(
        file_path,
        sep="\t",
        assume_missing=True,
        quoting=3,  # csv.QUOTE_NONNUMERIC
        on_bad_lines="skip",
        encoding="utf-8",
        blocksize=CHUNK_SIZE,
        header=0, # is always 0 as we skip rows
        dtype=full_dtype,
        skiprows=skipped_rows,  # Skip other header rows
        converters=converters
    )

    return ddf

def process_tsv_data(
    job: ConversionJob,
    temp_file_path: str,
    cleanup_temp: bool = False
    ) -> None:
    """Process TSV data from a file and convert it to GeoPackage format."""
    created = False
    try:
        ddf = read_tsv_as_dask_dataframe(temp_file_path, job)
        ddf = process_wkt_geometry(ddf, job.wkt_column)

        logging.debug(f"Removing output GeoPackage: {job.output_gpkg} if exists already")
        if os.path.exists(job.output_gpkg):
            os.remove(job.output_gpkg)

        logging.debug(f"Writing to GeoPackage {job.output_gpkg}...")

        delayed_partitions = ddf.to_delayed()
        total_partitions = len(delayed_partitions)

        for idx, partition in enumerate(delayed_partitions):
            progress_percent = min(int(((idx + 1) / total_partitions) * 100), 99)
            _status_manager.update(job.conversion_id, "processing", progress_percent=progress_percent)
            logging.debug(f"Writing partition {idx + 1} / {total_partitions}... ({progress_percent}%)")

            gdf = process_partition_for_geopackage(partition, job.mapped_crs)
            if gdf is not None:
                gdf = apply_geometry_transformation(gdf, job.mapped_geo_type)
                wrote = write_gdf_to_geopackage(gdf, job.output_gpkg, append=created)
                
                if wrote and not created:
                    created = True
        
        # Set to 99% when done processing partitions (100% only when status is "complete")
        _status_manager.update(job.conversion_id, "processing", progress_percent=99)

        # Check if GPKG was actually created
        if not created or not os.path.exists(job.output_gpkg):
            raise ValueError(f"No valid geometries found in the data. GPKG file could not be created.")

    finally:
        if created:
            create_output_zip(job)
            cleanup_files(job.output_gpkg)

        if cleanup_temp:
            cleanup_files(temp_file_path)
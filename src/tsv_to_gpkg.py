import logging
import os
import zipfile
from typing import Optional
from zipfile import ZipFile
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from dask import config as dask_config

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
    apply_geometry_transformation,
)
from email_notifications import notify_failure

import warnings

warnings.filterwarnings("ignore")
pd.options.mode.use_inf_as_na = True
dask_config.set(scheduler="threads", num_workers=os.cpu_count())


def handle_tsv_conversion_request(conversion_id: str, tsv_path: str, language: str, geo_type: str, crs: str, original_filename: Optional[str] = "laji-data") -> str:
    """Handle conversion request for direct TSV file."""
    job = ConversionJob(
        conversion_id=conversion_id,
        input_path=tsv_path,
        language=language,
        geo_type=geo_type,
        crs=crs,
        is_user_upload=True,
        original_filename=original_filename
    )
    
    # Check for existing conversion
    existing = check_existing_conversion(job.conversion_id)
    if existing:
        return existing

    file_size = os.path.getsize(job.input_path)
    logging.info(f"Starting TSV conversion for ID: {job.conversion_id}, input_path: {job.input_path}, file size: {file_size}")

    _status_manager.update(job.conversion_id, "processing", is_user_upload=job.is_user_upload, original_filename=job.original_filename)
    
    try:
        convert_file(job)
    except Exception as e:
        raise
    return job.conversion_id

def convert_file(job: ConversionJob) -> None:
    """Process file conversion from TSV to GeoPackage."""
    try:
        logging.debug(f"Processing {job.input_path} -> {job.conversion_id}")
 
        process_tsv_data(job, job.input_path)
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

def process_tsv_data(job: ConversionJob, tsv_file_path: str,) -> None:
    """Process TSV data with standard headers (no multi-language rows)."""
    created = False

    try:
        ddf = dd.read_csv(
            tsv_file_path,
            sep="\t",
            assume_missing=True,
            quoting=3,
            on_bad_lines="skip",
            encoding="utf-8",
            blocksize=CHUNK_SIZE,
            dtype=str
        )

        ddf = process_wkt_geometry(ddf, 'WGS84 WKT')

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

def create_output_zip(job: ConversionJob):
    """Create output ZIP containing only the GPKG file."""
    logging.debug(f"Creating simple output ZIP with GPKG: {job.output_gpkg}")
    zip_path_out = os.path.join(app_settings.OUTPUT_PATH, job.conversion_id + '.zip')

    with ZipFile(zip_path_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        if os.path.exists(job.output_gpkg):
            zout.write(job.output_gpkg, arcname=f"{job.original_filename}.gpkg")
            logging.debug(f"Added {job.output_gpkg} to zip {zip_path_out}")
        else:
            logging.warning(f"GPKG file not found: {job.output_gpkg}")

        
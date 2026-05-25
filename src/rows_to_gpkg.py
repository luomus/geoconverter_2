import logging
import os
import pandas as pd
import dask.dataframe as dd

from models import ConversionJob, CHUNK_SIZE, _status_manager
from helpers import (
    process_wkt_geometry,
    cleanup_files,
    process_partition_for_geopackage,
    write_gdf_to_geopackage,
    apply_geometry_transformation,
)

ROWS_WKT_COLUMN = "Gathering.Conversions.WGS84_WKT"

# Coordinate columns with no analytical value beyond the WKT geometry
GEOMETRY_COLUMNS_TO_DROP = {
    "Gathering.Conversions.WGS84.LatMin(N)",
    "Gathering.Conversions.WGS84.LatMax(N)",
    "Gathering.Conversions.WGS84.LonMin(E)",
    "Gathering.Conversions.WGS84.LonMax(E)",
    "Gathering.Conversions.WGS84CenterPoint.Lat(N)",
    "Gathering.Conversions.WGS84CenterPoint.Lon(E)",
    "Gathering.Conversions.ETRS-TM35FINCenterPoint.Lat(N)",
    "Gathering.Conversions.ETRS-TM35FINCenterPoint.Lon(E)",
    "Gathering.Conversions.ETRS-TM35FIN.LatMin(N)",
    "Gathering.Conversions.ETRS-TM35FIN.LatMax(N)",
    "Gathering.Conversions.ETRS-TM35FIN.LonMin(E)",
    "Gathering.Conversions.ETRS-TM35FIN.LonMax(E)",
    "Gathering.Conversions.YKJ.LatMin(N)",
    "Gathering.Conversions.YKJ.LatMax(N)",
    "Gathering.Conversions.YKJ.LonMin(E)",
    "Gathering.Conversions.YKJ.LonMax(E)",
    "Gathering.Conversions.YKJ_10KM.Lat(N)",
    "Gathering.Conversions.YKJ_10KM.Lon(E)",
    "Gathering.Conversions.YKJ_1KM.Lat(N)",
    "Gathering.Conversions.YKJ_1KM.Lon(E)"
}


def read_rows_as_dask_dataframe(file_path: str) -> dd.DataFrame:
    """Read a rows_* TSV file into a Dask DataFrame.

    Drops all Gathering.Conversions.* columns except the WKT column.
    """
    header = pd.read_csv(file_path, sep="\t", nrows=0, encoding="utf-8")

    usecols = [c for c in header.columns if c not in GEOMETRY_COLUMNS_TO_DROP]
    dtype = {c: "object" for c in usecols}

    ddf = dd.read_csv(
        file_path,
        sep="\t",
        assume_missing=True,
        quoting=3,
        on_bad_lines="skip",
        encoding="utf-8",
        blocksize=CHUNK_SIZE,
        usecols=usecols,
        dtype=dtype,
        na_values=[""],
    )

    return ddf


def process_rows_data(
    job: ConversionJob,
    temp_file_path: str,
    source_filename: str,
    cleanup_temp: bool = False,
) -> None:
    """Process a rows_* TSV file and convert it to GeoPackage format."""
    from zip_to_gpkg import create_output_zip

    created = False
    try:
        ddf = read_rows_as_dask_dataframe(temp_file_path)
        ddf = process_wkt_geometry(ddf, ROWS_WKT_COLUMN)

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

        _status_manager.update(job.conversion_id, "processing", progress_percent=99)

        if not created or not os.path.exists(job.output_gpkg):
            raise ValueError("No valid geometries found in the data. GPKG file could not be created.")

    finally:
        if created:
            create_output_zip(job, skip_file=source_filename)
            cleanup_files(job.output_gpkg)

        if cleanup_temp:
            cleanup_files(temp_file_path)

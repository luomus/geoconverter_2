"""
Test file for table_to_gpkg module.

Run with: python -m pytest tests/test_table_to_gpkg.py -v
"""

import pytest
import sys
import os
import tempfile
import pandas as pd
import geopandas as gpd
from unittest.mock import MagicMock, patch
from shapely.geometry import Point
from zipfile import ZipFile
import dask.dataframe as dd


class MockSettings:
    LOGGING = "INFO"
    OUTPUT_PATH = "./"

mock_settings = MagicMock()
mock_settings.Settings.return_value = MockSettings()
sys.modules['settings'] = mock_settings

# Mock FastAPI and its dependencies
sys.modules['fastapi'] = MagicMock()
sys.modules['fastapi.responses'] = MagicMock()

# Add src directory to path to import the module
sys.path.append('src/')

from table_to_gpkg import (
    update_conversion_status,
    cleanup_files,
    handle_conversion_request,
    convert_file,
    write_partition_to_geopackage,
    read_tsv_as_dask_dataframe,
    process_wkt_geometry,
    process_tsv_data,
    create_output_zip,
    LANGUAGE_MAPPING,
    GEOMETRY_LANG_MAPPING,
    CRS_MAPPING,
    GEOMETRY_TYPE_MAPPING,
    LARGE_FILE_THRESHOLD,
    CHUNK_SIZE,
    conversion_status,
    status_lock
)

@pytest.mark.parametrize(
    "file_size,expected_status,expect_background",
    [
        (1024, "completed", False),            # small file (1 KB)
        (12 * 1024 * 1024, "processing", True)  # large file (12 MB)
    ]
)
@patch("table_to_gpkg.convert_file")
def test_handle_conversion_request(
    mock_convert, file_size, expected_status, expect_background, tmp_path
):
    # Create temp file with desired size
    test_file = tmp_path / "test.zip"
    test_file.write_bytes(b"0" * file_size)

    background_tasks = MagicMock()

    result = handle_conversion_request(
        conversion_id="test123",
        zip_path=str(test_file),
        language="en",
        geo_type="point",
        crs="wgs84",
        background_tasks=background_tasks,
        uploaded_file=True,
    )

    assert result["status"] == expected_status
    assert result["id"] == "test123"
    assert result['status_url'] == f"/status/test123"

    if expect_background:
        background_tasks.add_task.assert_called_once()
        mock_convert.assert_not_called()
    else:
        mock_convert.assert_called_once()
        background_tasks.add_task.assert_not_called()

def test_create_output_zip(tmp_path):
    """Test create_output_zip function with real file operations."""
    
    # Create test input ZIP with occurrences.txt and other files
    input_zip = tmp_path / "input.zip"
    with ZipFile(input_zip, "w") as zf:
        zf.writestr("occurrences.txt", "id\tname\n1\ttest\n")
        zf.writestr("meta.xml", "<metadata>test</metadata>")
        zf.writestr("doc/readme.txt", "Test documentation")
    
    # Create test GPKG file in working directory 
    test_gpkg_name = "test123.gpkg"
    gdf = gpd.GeoDataFrame(
        {"id": [1], "name": ["test"]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326"
    )
    
    conversion_id = "test123"
    
    # Change to tmp directory to ensure output is created there
    original_dir = os.getcwd()
    try:
        os.chdir(tmp_path)
        
        # Create GPKG file in the working directory
        gdf.to_file(test_gpkg_name, driver="GPKG")
        
        # Call the function
        result_zip = create_output_zip(
            zip_path=str(input_zip),
            output_gpkg=test_gpkg_name,  # Use just filename
            conversion_id=conversion_id,
            cleanup_source=False
        )
        
        # Verify output ZIP was created
        assert os.path.exists(result_zip)
        assert result_zip == f"./{conversion_id}.zip"
        
        # Verify contents of output ZIP
        with ZipFile(result_zip, "r") as zf:
            file_list = zf.namelist()
            
            # Should contain GPKG but not occurrences.txt
            assert "test123.gpkg" in file_list
            assert "occurrences.txt" not in file_list
            
            # Should contain other original files
            assert "meta.xml" in file_list
            assert "doc/readme.txt" in file_list
            
            # Verify GPKG content is correct
            with tempfile.TemporaryDirectory() as extract_dir:
                zf.extractall(extract_dir)
                extracted_gpkg = os.path.join(extract_dir, "test123.gpkg")
                result_gdf = gpd.read_file(extracted_gpkg)
                assert len(result_gdf) == 1
                assert result_gdf.iloc[0]["name"] == "test"
    
    finally:
        os.chdir(original_dir)

def test_update_conversion_status():
    """Test update_conversion_status function."""
    conversion_status.clear()
    
    conversion_id = "test_123"
    update_conversion_status(conversion_id, "processing")
    
    assert conversion_id in conversion_status
    assert conversion_status[conversion_id]["status"] == "processing"
    assert "timestamp" in conversion_status[conversion_id]


def test_cleanup_files():
    """Test cleanup_files function."""
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"test content")
        temp_file = f.name
    
    assert os.path.exists(temp_file)
    cleanup_files(temp_file)
    assert not os.path.exists(temp_file)


def _create_fake_zip(tmp_path):
    """Helper to create a minimal valid zip with occurrences.txt."""
    zip_path = tmp_path / "test.zip"
    occurrence_content = b"id\tname\n1\tsample\n"
    with ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrences.txt", occurrence_content)
    return zip_path


@patch("table_to_gpkg.os.path.exists")
@patch("table_to_gpkg.os.path.getsize")
@patch("table_to_gpkg.process_tsv_data")
@patch("table_to_gpkg.update_conversion_status")
def test_convert_file_success(mock_update_status, mock_process_tsv, mock_getsize, mock_exists, tmp_path):
    """Test convert_file with success scenario."""
    conversion_id = "test123"
    zip_path = _create_fake_zip(tmp_path)
    
    mock_exists.return_value = True
    mock_getsize.return_value = 1024

    convert_file(str(zip_path), "en", "point", "wgs84", conversion_id)

    mock_process_tsv.assert_called_once()
    
    final_call = mock_update_status.call_args_list[-1]
    assert final_call[0][0] == conversion_id
    assert final_call[0][1] == "completed"


@patch("table_to_gpkg.process_tsv_data")
@patch("table_to_gpkg.update_conversion_status")
@patch("table_to_gpkg.cleanup_files")
def test_convert_file_failure(mock_cleanup, mock_update_status, mock_process_tsv, tmp_path):
    """Test convert_file handles failures correctly."""
    conversion_id = "test456"
    zip_path = _create_fake_zip(tmp_path)
    
    # Make process_tsv_data raise an exception
    mock_process_tsv.side_effect = Exception("Processing failed")

    convert_file(str(zip_path), "en", "point", "wgs84", conversion_id)

    # Verify update_conversion_status was called with "failed"
    final_call = mock_update_status.call_args_list[-1]
    assert final_call[0][0] == conversion_id
    assert final_call[0][1] == "failed"
    
    # Verify cleanup was called
    mock_cleanup.assert_called_once()


class FakePartition:
    """Simulate Dask partition with .compute()."""
    def __init__(self, df):
        self._df = df

    def compute(self):
        return self._df


@pytest.mark.parametrize(
    "geometries,expect_write",
    [
        ([Point(0, 0), Point(1, 1)], True),  # valid geometries
        ([None, None], False),               # all missing
        ([], False),                         # empty dataframe
    ]
)
@patch("table_to_gpkg.apply_geometry_transformation", side_effect=lambda gdf, gt: gdf)
@patch("table_to_gpkg.write_dataframe")
def test_write_partition_to_geopackage(
    mock_write, mock_transform, tmp_path, geometries, expect_write
):
    # Arrange
    df = gpd.GeoDataFrame({"geometry": geometries}, crs="EPSG:4326")
    partition = FakePartition(df)
    output_gpkg = tmp_path / "out.gpkg"

    # Act
    write_partition_to_geopackage(
        partition=partition,
        crs="EPSG:3857",
        output_gpkg=str(output_gpkg),
        geom_type="point",
    )

    # Assert
    if expect_write:
        mock_write.assert_called_once()
        mock_transform.assert_called_once()
    else:
        mock_write.assert_not_called()
        mock_transform.assert_not_called()

@pytest.mark.parametrize(
    "language,wkt_column",
    [
        ("en", "Footprint WKT"),
        ("fi", "WGS84 geometria"),
        ("tech", "footprintWKT"),
    ]
)
@patch("table_to_gpkg.get_default_column_types")
@patch("table_to_gpkg.dd.read_csv")
def test_read_tsv_as_dask_dataframe(
    mock_read_csv, mock_column_types, tmp_path, language, wkt_column
):
    """Test read_tsv_as_dask_dataframe with different languages."""
    
    # Create test TSV file
    test_data = f"id\t{wkt_column}\n1\tPOINT(0 0)\n2\tPOINT(1 1)"
    test_file = tmp_path / "test.tsv"
    test_file.write_text(test_data)
    
    # Mock the helper function
    mock_column_types.return_value = {"id": "int64", wkt_column: "object"}
    
    # Create a simple mock DataFrame
    mock_df = MagicMock()
    mock_df.__len__.return_value = 2
    
    # All filtering operations return the same DataFrame
    mock_df.__getitem__.return_value = mock_df
    mock_df.str = MagicMock()
    mock_df.str.strip.return_value = mock_df
    mock_df.map.return_value = mock_df
    mock_df.set_geometry.return_value = mock_df
    
    mock_read_csv.return_value = mock_df
    
    # Call the function
    result = read_tsv_as_dask_dataframe(str(test_file), language, wkt_column)
    
    # Verify read_csv was called with correct parameters
    mock_read_csv.assert_called_once()
    call_kwargs = mock_read_csv.call_args[1]
    assert call_kwargs["sep"] == "\t"
    assert call_kwargs["encoding"] == "utf-8"
    assert call_kwargs["on_bad_lines"] == "skip"
    assert call_kwargs["blocksize"] == "100MB"
    
    # Verify helper functions were called
    mock_column_types.assert_called_once_with(language, dtypes_path='lookup_table.tsv')
    
    # Verify the result is the processed DataFrame
    assert result == mock_df


@pytest.mark.parametrize(
    "cleanup_temp,compress_output",
    [
        (True, True),   # Full processing
        (False, False), # Minimal processing
    ]
)
@patch("table_to_gpkg.cleanup_files")
@patch("table_to_gpkg.create_output_zip")
@patch("table_to_gpkg.write_partition_to_geopackage")
@patch("table_to_gpkg.read_tsv_as_dask_dataframe")
@patch("table_to_gpkg.os.remove")
@patch("table_to_gpkg.os.path.exists")
def test_process_tsv_data(
    mock_exists, mock_remove, mock_read_tsv, mock_write_partition, mock_create_zip, mock_cleanup, 
    tmp_path, cleanup_temp, compress_output
):
    """Test process_tsv_data function with different configurations."""
    
    # Setup test data
    conversion_id = "test123"
    temp_file_path = str(tmp_path / "temp.tsv")
    
    # Create temp file
    with open(temp_file_path, "w") as f:
        f.write("id\tFootprint WKT\n1\tPOINT(0 0)")
    
    # Mock dask dataframe with single partition
    mock_partition = MagicMock()
    mock_ddf = MagicMock()
    mock_ddf.to_delayed.return_value = [mock_partition]
    mock_read_tsv.return_value = mock_ddf
    
    # Configure mocks to simulate successful processing
    mock_write_partition.return_value = True  # Simulate successful write
    
    # Configure os.path.exists to simulate the workflow:
    # 1. First call: check if GPKG exists for deletion (False)
    # 2. Second call: check if GPKG was created successfully (True)
    call_count = [0]  # Use list to allow modification in nested function
    
    def exists_side_effect(path):
        if path == "test123.gpkg":
            call_count[0] += 1
            return call_count[0] > 1
        return False
    
    mock_exists.side_effect = exists_side_effect
    
    # Call the function
    process_tsv_data(
        conversion_id=conversion_id,
        zip_path="test.zip",
        temp_file_path=temp_file_path,
        output_gpkg="test123.gpkg",
        language="en",
        geom_type="point",
        crs="EPSG:4326",
        wkt_column="Footprint WKT",
        cleanup_temp=cleanup_temp,
        compress_output=compress_output
    )
    
    # Verify core functionality always happens
    mock_read_tsv.assert_called_once()
    mock_write_partition.assert_called_once()
    
    # Verify conditional behavior
    if cleanup_temp:
        mock_cleanup.assert_called_once()
    else:
        mock_cleanup.assert_not_called()
        
    if compress_output:
        mock_create_zip.assert_called_once()
    else:
        mock_create_zip.assert_not_called()

